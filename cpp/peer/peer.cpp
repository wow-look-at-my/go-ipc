// The C++ peer of spec/peer.md.
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <fstream>
#include <iterator>
#include <map>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <unistd.h>

#include <goipc/goipc.hpp>

#include "demo.hpp"

namespace {

// fixture.h builds each values.json entry as fx_value_<i>(), and uses bytes_of for bytes fields.
std::vector<std::byte> bytes_of(const char *p, std::size_t n)
{
	std::vector<std::byte> out(n);
	for (std::size_t i = 0; i < n; i++)
		out[i] = static_cast<std::byte>(p[i]);
	return out;
}

#include "fixture.h"

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

// recv_checked receives total messages as the recv role checks them.
void recv_checked(goipc::Queue &q, std::uint64_t total, const goipc::detail::deadline &d)
{
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
}

void print_line(const std::string &line)
{
	if (std::fputs((line + "\n").c_str(), stdout) < 0 || std::fflush(stdout) != 0)
		throw std::runtime_error("write \"" + line + "\"");
}

void role_recv(std::string_view name, std::uint64_t total, std::uint64_t capacity, const goipc::detail::deadline &d)
{
	auto q = goipc::Queue::create(name, capacity);
	ready();
	recv_checked(q, total, d);
	print_line("ok " + std::to_string(total));
	q.close();
	q.unlink();
}

// die_now stands in for a process that dies: no destructor, close or exit
// handler runs.
[[noreturn]] void die_now()
{
	std::fflush(stdout);
	std::fflush(stderr);
	::_exit(0);
}

void role_claim_and_die(std::string_view name, std::uint64_t length, const goipc::detail::deadline &d)
{
	auto q = goipc::Queue::open(name);
	goipc::Claim c = q.claim(1, length, d.left());
	std::memset(c.bytes().data(), 0xAB, c.bytes().size());
	die_now();
}

bool is_peer_gone(const goipc::error &e) { return e.value() == goipc::errc::peer_gone; }

void check_mode(std::string_view mode)
{
	if (mode != "close" && mode != "exit" && mode != "release")
		throw std::runtime_error("mode must be close, exit or release, not \"" + std::string(mode) + "\"");
}

// stop_now ends the process with no close. Mode release removes the life socket first.
[[noreturn]] void stop_now(std::string_view mode)
{
	if (mode == "release")
		goipc::release();
	die_now();
}

void role_send_until_gone(std::string_view name, std::string_view sender, const goipc::detail::deadline &d)
{
	auto q = goipc::Queue::open(name);
	std::uint64_t n = 0;
	for (;; n++) {
		std::string payload = std::string(sender) + ":" + std::to_string(n);
		try {
			q.send(static_cast<std::uint32_t>(n), bytes_of(payload), d.left());
		} catch (const goipc::error &e) {
			if (!is_peer_gone(e))
				throw;
			break;
		}
	}
	print_line("gone " + std::to_string(n));
	q.close();
}

void role_recv_then_stop(std::string_view name, std::uint64_t count, std::uint64_t capacity, std::string_view mode,
			 const goipc::detail::deadline &d)
{
	check_mode(mode);
	auto q = goipc::Queue::create(name, capacity);
	ready();
	recv_checked(q, count, d);
	print_line("ok " + std::to_string(count));
	if (mode != "close") {
		q.unlink();
		stop_now(mode);
	}
	q.close();
	q.unlink();
}

std::string chan_payload(std::uint64_t i) { return "0:" + std::to_string(i); }

void role_chan_recv_until_gone(std::string_view name, std::uint64_t capacity, std::uint64_t count,
			       const goipc::detail::deadline &d)
{
	auto c = goipc::Channel::create(name, capacity);
	ready();
	std::vector<std::byte> buf(c.max_message_size());
	std::uint64_t n = 0;
	for (;; n++) {
		goipc::received r{};
		try {
			r = c.recv(buf, d.left());
		} catch (const goipc::error &e) {
			if (!is_peer_gone(e))
				throw;
			break;
		}
		std::string_view text(reinterpret_cast<const char *>(buf.data()), r.size);
		if (r.type != n || text != chan_payload(n))
			throw std::runtime_error("message " + std::to_string(n) + ": type " + std::to_string(r.type) +
						 ", payload \"" + std::string(text) + "\"");
	}
	if (n != count)
		throw std::runtime_error("received " + std::to_string(n) + " messages, want " + std::to_string(count));
	try {
		c.send(0, {}, d.left());
		throw std::runtime_error("a send after the peer left went through");
	} catch (const goipc::error &e) {
		if (!is_peer_gone(e))
			throw;
	}
	print_line("ok " + std::to_string(count));
	c.close();
	c.unlink();
}

void role_chan_send_then_stop(std::string_view name, std::uint64_t count, std::string_view mode,
			      const goipc::detail::deadline &d)
{
	check_mode(mode);
	auto c = goipc::Channel::open(name);
	for (std::uint64_t i = 0; i < count; i++)
		c.send(static_cast<std::uint32_t>(i), bytes_of(chan_payload(i)), d.left());
	if (mode != "close")
		stop_now(mode);
	c.close();
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
	// close sends end-of-stream only when the ring has room. close_write waits for room.
	c.close_write(d.left());
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

template <class T>
void send_value(goipc::Queue &q, int idx, const T &v, const goipc::detail::deadline &d)
{
	std::vector<std::byte> buf(v.size());
	if (v.encode(buf) != buf.size())
		throw std::runtime_error("entry " + std::to_string(idx) + ": encode does not write " +
					 std::to_string(buf.size()) + " bytes");
	q.send(T::type_id, buf, d.left());
}

void role_typed_send(std::string_view name, const goipc::detail::deadline &d)
{
	auto q = goipc::Queue::open(name);
#define FX_CASE(idx, T) send_value<T>(q, idx, fx_value_##idx(), d);
	FX_CASES
#undef FX_CASE
	q.close();
}

template <class T>
std::vector<std::byte> reencode_as(std::span<const std::byte> in)
{
	std::optional<T> m = T::decode(in);
	if (!m)
		throw std::runtime_error("decode rejects the record");
	std::vector<std::byte> out(m->size());
	if (m->encode(out) != out.size())
		throw std::runtime_error("encode does not write " + std::to_string(out.size()) + " bytes");
	return out;
}

// reencode decodes in with the decoder for type, then encodes the result.
std::vector<std::byte> reencode(std::uint32_t type, std::span<const std::byte> in)
{
#define FX_CASE(idx, T) \
	if (type == T::type_id) \
		return reencode_as<T>(in);
	FX_CASES
#undef FX_CASE
	throw std::runtime_error("no message has type ID " + std::to_string(type));
}

std::vector<std::byte> read_file(const std::string &path)
{
	std::ifstream f(path, std::ios::binary);
	std::vector<char> raw((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
	if (!f.is_open() || f.bad())
		throw std::runtime_error("cannot read " + path);
	return bytes_of(raw.data(), raw.size());
}

void recv_entry(goipc::Queue &q, std::span<std::byte> buf, const std::string &spec, int idx, std::uint32_t want_type,
		const goipc::detail::deadline &d)
{
	std::string where = "entry " + std::to_string(idx) + ": ";
	goipc::received r = q.recv(buf, d.left());
	if (r.type != want_type)
		throw std::runtime_error(where + "type " + std::to_string(r.type) + ", want " + std::to_string(want_type));
	std::string path = spec + "/vectors/schema/" + std::to_string(idx) + ".bin";
	std::vector<std::byte> got;
	try {
		got = reencode(r.type, buf.first(r.size));
	} catch (const std::exception &e) {
		throw std::runtime_error(where + e.what());
	}
	if (got != read_file(path))
		throw std::runtime_error(where + "re-encoding differs from " + path);
}

void role_typed_recv(std::string_view name, std::uint64_t capacity, const goipc::detail::deadline &d)
{
	const char *spec = std::getenv("GOIPC_SPEC_DIR");
	if (spec == nullptr || *spec == '\0')
		throw std::runtime_error("GOIPC_SPEC_DIR is not set; it must name the spec directory");
	auto q = goipc::Queue::create(name, capacity);
	try {
		ready();
		std::vector<std::byte> buf(q.max_message_size());
		int count = 0;
#define FX_CASE(idx, T) \
	recv_entry(q, buf, spec, idx, T::type_id, d); \
	count++;
		FX_CASES
#undef FX_CASE
		std::printf("ok %d\n", count);
		std::fflush(stdout);
	} catch (...) {
		q.close();
		q.unlink();
		throw;
	}
	q.close();
	q.unlink();
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
	} else if (role == "typed-send") {
		need(1, "typed-send <name>");
		role_typed_send(argv[2], d);
	} else if (role == "typed-recv") {
		need(2, "typed-recv <name> <capacity>");
		role_typed_recv(argv[2], parse_u64("capacity", argv[3]), d);
	} else if (role == "claim-and-die") {
		need(2, "claim-and-die <name> <length>");
		role_claim_and_die(argv[2], parse_u64("length", argv[3]), d);
	} else if (role == "send-until-gone") {
		need(2, "send-until-gone <name> <sender>");
		role_send_until_gone(argv[2], argv[3], d);
	} else if (role == "recv-then-stop") {
		need(4, "recv-then-stop <name> <count> <capacity> <mode>");
		role_recv_then_stop(argv[2], parse_u64("count", argv[3]), parse_u64("capacity", argv[4]), argv[5], d);
	} else if (role == "chan-recv-until-gone") {
		need(3, "chan-recv-until-gone <name> <capacity> <count>");
		role_chan_recv_until_gone(argv[2], parse_u64("capacity", argv[3]), parse_u64("count", argv[4]), d);
	} else if (role == "chan-send-then-stop") {
		need(3, "chan-send-then-stop <name> <count> <mode>");
		role_chan_send_then_stop(argv[2], parse_u64("count", argv[3]), argv[4], d);
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
