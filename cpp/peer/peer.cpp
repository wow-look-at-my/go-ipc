// The C++ peer of spec/peer.md.
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <exception>
#include <map>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <goipc/goipc.hpp>

namespace {

constexpr std::chrono::seconds role_timeout{60};

std::uint64_t parse_u64(std::string_view what, std::string_view s)
{
	std::uint64_t v = 0;
	auto [end, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
	if (ec != std::errc() || end != s.data() + s.size())
		throw std::runtime_error(std::string(what) + ": not a number: " + std::string(s));
	return v;
}

void ready()
{
	if (std::fputs("ready\n", stdout) < 0 || std::fflush(stdout) != 0)
		throw std::runtime_error("write ready line");
}

std::span<const std::byte> bytes_of(std::string_view s) { return std::as_bytes(std::span(s.data(), s.size())); }

void role_recv(std::string_view name, std::uint64_t total, std::uint64_t capacity, const goipc::detail::deadline &d)
{
	auto q = goipc::Queue::create(name, capacity);
	ready();
	std::map<std::string, std::uint64_t> next;
	std::vector<std::byte> buf(q.max_message_size());
	for (std::uint64_t i = 0; i < total; i++) {
		goipc::received r = q.recv(buf, d.left());
		std::string_view text(reinterpret_cast<const char *>(buf.data()), r.size);
		auto colon = text.find(':');
		if (colon == std::string_view::npos)
			throw std::runtime_error("message " + std::to_string(i) + ": payload \"" + std::string(text) + "\" has no ':'");
		std::string sender(text.substr(0, colon));
		std::uint64_t seq = parse_u64("message " + std::to_string(i) + " seq", text.substr(colon + 1));
		if (seq != r.type)
			throw std::runtime_error("message " + std::to_string(i) + ": type " + std::to_string(r.type) +
						 ", payload \"" + std::string(text) + "\"");
		std::uint64_t &want = next[sender];
		if (seq != want)
			throw std::runtime_error("message " + std::to_string(i) + ": sender \"" + sender + "\" sent seq " +
						 std::to_string(seq) + ", want " + std::to_string(want));
		want = seq + 1;
	}
	std::printf("ok %llu\n", static_cast<unsigned long long>(total));
	std::fflush(stdout);
	q.close();
	q.unlink();
}

void role_send(std::string_view name, std::string_view sender, std::uint64_t count, const goipc::detail::deadline &d)
{
	auto q = goipc::Queue::open(name);
	for (std::uint64_t i = 0; i < count; i++) {
		std::string payload = std::string(sender) + ":" + std::to_string(i);
		q.send(static_cast<std::uint32_t>(i), bytes_of(payload), d.left());
	}
	q.close();
}

void role_listen_echo(std::string_view name, std::uint64_t capacity, const goipc::detail::deadline &d)
{
	auto c = goipc::Conn::listen(name, capacity);
	ready();
	std::vector<std::byte> buf(c.channel().max_message_size());
	for (;;) {
		std::size_t n = c.read(buf, d.left());
		if (n == 0)
			break;
		c.write(std::span(buf).first(n), d.left());
	}
	c.close();
	c.unlink();
}

std::byte pattern_at(std::uint64_t i) { return static_cast<std::byte>((i * 31 + 7) % 256); }

void role_dial_check(std::string_view name, std::uint64_t total, const goipc::detail::deadline &d)
{
	auto c = goipc::Conn::dial(name);
	std::vector<std::byte> want(total);
	for (std::uint64_t i = 0; i < total; i++)
		want[i] = pattern_at(i);

	// The ring holds less than the stream, so the write runs beside the read.
	std::exception_ptr write_err;
	std::thread writer([&] {
		try {
			c.write(want, d.left());
			c.close_write(d.left());
		} catch (...) {
			write_err = std::current_exception();
		}
	});

	std::vector<std::byte> got;
	got.reserve(total);
	std::vector<std::byte> buf(c.channel().max_message_size());
	std::exception_ptr read_err;
	try {
		for (;;) {
			std::size_t n = c.read(buf, d.left());
			if (n == 0)
				break;
			got.insert(got.end(), buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(n));
			if (got.size() > total)
				throw std::runtime_error("echo returned more than " + std::to_string(total) + " bytes");
		}
	} catch (...) {
		read_err = std::current_exception();
	}
	if (read_err)
		c.close();
	writer.join();
	if (write_err)
		std::rethrow_exception(write_err);
	if (read_err)
		std::rethrow_exception(read_err);
	if (got.size() != total)
		throw std::runtime_error("echo returned " + std::to_string(got.size()) + " bytes, want " + std::to_string(total));
	for (std::uint64_t i = 0; i < total; i++) {
		if (got[i] != want[i])
			throw std::runtime_error("echo differs at byte " + std::to_string(i) + ": got " +
						 std::to_string(static_cast<int>(got[i])) + ", want " +
						 std::to_string(static_cast<int>(want[i])));
	}
	c.close();
}

void run(int argc, char **argv)
{
	if (argc < 2)
		throw std::runtime_error("usage: goipc-peer <role> <args...>");
	std::string_view role = argv[1];
	auto need = [&](int n, const char *usage) {
		if (argc != n + 2)
			throw std::runtime_error(std::string("usage: ") + usage);
	};
	goipc::detail::deadline d(role_timeout);
	if (role == "recv") {
		need(3, "recv <name> <total> <capacity>");
		role_recv(argv[2], parse_u64("total", argv[3]), parse_u64("capacity", argv[4]), d);
	} else if (role == "send") {
		need(3, "send <name> <sender> <count>");
		role_send(argv[2], argv[3], parse_u64("count", argv[4]), d);
	} else if (role == "listen-echo") {
		need(2, "listen-echo <name> <capacity>");
		role_listen_echo(argv[2], parse_u64("capacity", argv[3]), d);
	} else if (role == "dial-check") {
		need(2, "dial-check <name> <bytes>");
		role_dial_check(argv[2], parse_u64("bytes", argv[3]), d);
	} else {
		throw std::runtime_error("unknown role \"" + std::string(role) + "\"");
	}
}

} // namespace

int main(int argc, char **argv)
{
	try {
		run(argc, argv);
	} catch (const std::exception &e) {
		std::fprintf(stderr, "goipc-peer: %s\n", e.what());
		return 1;
	}
	return 0;
}
