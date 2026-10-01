#include <chrono>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <goipc/goipc.hpp>

#include "helpers.hpp"

namespace {

using namespace std::chrono_literals;
using goipc::Channel;
using goipc::Conn;
using goipc::errc;
using testutil::bytes_of;
using testutil::errc_of;
using testutil::text_of;

std::vector<std::byte> pattern(std::size_t n)
{
	std::vector<std::byte> b(n);
	for (std::size_t i = 0; i < n; i++)
		b[i] = static_cast<std::byte>((i * 31 + 7) % 256);
	return b;
}

std::vector<std::byte> read_n(Conn &c, std::size_t n)
{
	std::vector<std::byte> out(n);
	std::size_t got = 0;
	while (got < n) {
		std::size_t k = c.read(std::span(out).subspan(got), 10s);
		if (k == 0)
			throw std::runtime_error("unexpected end of stream after " + std::to_string(got) + " bytes");
		got += k;
	}
	return out;
}

TEST(Channel, BothDirections)
{
	auto name = testutil::unique_name("ch");
	testutil::channel_files f{name};
	auto creator = Channel::create(name, 4096);
	auto opener = Channel::open(name);
	EXPECT_EQ(creator.tx().name(), name + ".c2o");
	EXPECT_EQ(creator.rx().name(), name + ".o2c");
	EXPECT_EQ(opener.tx().name(), name + ".o2c");
	EXPECT_EQ(opener.rx().name(), name + ".c2o");
	EXPECT_EQ(creator.max_message_size(), 2040u);

	creator.send(1, bytes_of("to opener"), 1s);
	auto a = opener.recv(1s);
	EXPECT_EQ(a.type, 1u);
	EXPECT_EQ(text_of(a.payload), "to opener");

	ASSERT_TRUE(opener.try_send(2, bytes_of("to creator")));
	std::vector<std::byte> buf(64);
	auto b = creator.recv(buf, 1s);
	EXPECT_EQ(b.type, 2u);
	EXPECT_EQ(text_of(std::span(buf).first(b.size)), "to creator");

	creator.unlink();
	for (const char *dir : {".c2o", ".o2c"})
		EXPECT_FALSE(testutil::file_exists("/dev/shm/go-shm-" + name + dir)) << dir;
}

TEST(Channel, OpenMissingFails)
{
	EXPECT_EQ(errc_of([&] { Channel::open(testutil::unique_name("missing")); }), errc::sys);
	EXPECT_EQ(errc_of([&] { Channel::create("a/b"); }), errc::invalid_name);
}

TEST(Conn, LargeWriteSplitsAndReassembles)
{
	auto name = testutil::unique_name("conn");
	testutil::channel_files f{name};
	auto l = Conn::listen(name, 16384);
	auto d = Conn::dial(name);
	ASSERT_EQ(d.channel().max_message_size(), 8184u);

	auto want = pattern(10000);
	d.write(want, 1s);
	// Messages: a full one and the 1816-byte rest.
	EXPECT_EQ(l.channel().rx().ring().buffered(), (8 + 8184u) + (8 + 1816u));
	EXPECT_EQ(read_n(l, want.size()), want);
}

TEST(Conn, SmallReadsDrainOneMessage)
{
	auto name = testutil::unique_name("conn");
	testutil::channel_files f{name};
	auto l = Conn::listen(name, 4096);
	auto d = Conn::dial(name);
	d.write(bytes_of("abcdef"), 1s);
	std::vector<std::byte> two(2);
	std::string got;
	for (int i = 0; i < 3; i++) {
		ASSERT_EQ(l.read(two, 1s), 2u);
		got += text_of(two);
	}
	EXPECT_EQ(got, "abcdef");
	EXPECT_EQ(l.read(std::span<std::byte>(), 1s), 0u);
	EXPECT_EQ(errc_of([&] { l.read(two, 10ms); }), errc::timed_out);
}

TEST(Conn, PeerCloseGivesEndOfStream)
{
	auto name = testutil::unique_name("conn");
	testutil::channel_files f{name};
	auto l = Conn::listen(name, 4096);
	auto d = Conn::dial(name);
	l.write(bytes_of("last words"), 1s);
	l.close();

	EXPECT_EQ(text_of(read_n(d, 10)), "last words");
	EXPECT_FALSE(d.eof());
	std::vector<std::byte> buf(8);
	EXPECT_EQ(d.read(buf, 1s), 0u);
	EXPECT_TRUE(d.eof());
	EXPECT_EQ(d.read(buf, 1s), 0u);
}

TEST(Conn, CloseWrite)
{
	auto name = testutil::unique_name("conn");
	testutil::channel_files f{name};
	auto l = Conn::listen(name, 4096);
	auto d = Conn::dial(name);

	d.write(bytes_of("question"), 1s);
	d.close_write(1s);
	EXPECT_EQ(errc_of([&] { d.write(bytes_of("more"), 1s); }), errc::closed);
	EXPECT_NO_THROW(d.close_write(1s));

	EXPECT_EQ(text_of(read_n(l, 8)), "question");
	std::vector<std::byte> buf(16);
	EXPECT_EQ(l.read(buf, 1s), 0u);
	EXPECT_TRUE(l.eof());

	l.write(bytes_of("answer"), 1s);
	EXPECT_EQ(text_of(read_n(d, 6)), "answer");

	// Close after close_write sends no second end-of-stream.
	d.close();
	EXPECT_FALSE(l.channel().rx().try_recv().has_value());
}

TEST(Conn, CrossProcessEcho)
{
	auto name = testutil::unique_name("conn");
	testutil::channel_files f{name};
	auto l = Conn::listen(name, 4096);
	pid_t pid = testutil::fork_child([&] {
		auto d = Conn::dial(name);
		std::vector<std::byte> buf(1000);
		for (;;) {
			std::size_t n = d.read(buf, 30s);
			if (n == 0)
				break;
			d.write(std::span(buf).first(n), 30s);
		}
		d.close();
		return 0;
	});

	// Less than the ring holds, so a write-then-read cannot deadlock.
	auto want = pattern(1500);
	l.write(want, 10s);
	EXPECT_EQ(read_n(l, want.size()), want);
	l.close_write(10s);
	std::vector<std::byte> buf(8);
	EXPECT_EQ(l.read(buf, 10s), 0u);
	EXPECT_EQ(testutil::exit_code(testutil::wait_child(pid).status), 0);
}

} // namespace
