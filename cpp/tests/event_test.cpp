#include <chrono>
#include <fstream>

#include <sys/stat.h>

#include <gtest/gtest.h>

#include <goipc/goipc.hpp>

#include "helpers.hpp"

namespace {

using namespace std::chrono_literals;
using goipc::errc;
using goipc::Event;
using testutil::errc_of;

struct event_files {
	std::string name;
	~event_files() { Event::remove(name); }
};

TEST(Event, CreateMakesFifo)
{
	auto name = testutil::unique_name("ev");
	event_files f{name};
	auto e = Event::create(name);
	EXPECT_EQ(e.path(), "/dev/shm/go-ipc-" + name + ".event");
	struct stat st;
	ASSERT_EQ(::stat(e.path().c_str(), &st), 0);
	EXPECT_TRUE(S_ISFIFO(st.st_mode));
	EXPECT_EQ(st.st_mode & 0777, 0600u);
}

TEST(Event, SignalBeforeWaitIsKept)
{
	auto name = testutil::unique_name("ev");
	event_files f{name};
	auto e = Event::create(name);
	e.signal();
	EXPECT_TRUE(e.wait(0ms));
	EXPECT_FALSE(e.wait(0ms));
}

TEST(Event, TimeoutConsumesNothing)
{
	auto name = testutil::unique_name("ev");
	event_files f{name};
	auto e = Event::create(name);
	auto start = std::chrono::steady_clock::now();
	EXPECT_FALSE(e.wait(30ms));
	EXPECT_GE(std::chrono::steady_clock::now() - start, 30ms);
	e.signal(1);
	EXPECT_TRUE(e.wait(0ms));
	EXPECT_FALSE(e.wait(0ms));
}

TEST(Event, SignalCountsAndCap)
{
	auto name = testutil::unique_name("ev");
	event_files f{name};
	auto e = Event::create(name);
	e.signal(3);
	for (int i = 0; i < 3; i++)
		EXPECT_TRUE(e.wait(0ms));
	EXPECT_FALSE(e.wait(0ms));

	e.signal(5000);
	int n = 0;
	while (e.wait(0ms))
		n++;
	EXPECT_EQ(n, goipc::wire::signal_max_tokens);
	e.signal(0);
	EXPECT_FALSE(e.wait(0ms));
}

TEST(Event, OpenSharesTokens)
{
	auto name = testutil::unique_name("ev");
	event_files f{name};
	auto a = Event::create(name);
	auto b = Event::open(name);
	a.signal();
	EXPECT_TRUE(b.wait(1s));
}

TEST(Event, UseAfterClose)
{
	auto name = testutil::unique_name("ev");
	event_files f{name};
	auto e = Event::create(name);
	e.close();
	e.close();
	EXPECT_EQ(errc_of([&] { e.signal(); }), errc::closed);
	EXPECT_EQ(errc_of([&] { e.wait(0ms); }), errc::closed);
}

TEST(Event, UnlinkAndRemove)
{
	auto name = testutil::unique_name("ev");
	auto e = Event::create(name);
	ASSERT_TRUE(testutil::file_exists(e.path()));
	e.unlink();
	EXPECT_FALSE(testutil::file_exists(e.path()));
	EXPECT_NO_THROW(e.unlink());
	EXPECT_NO_THROW(Event::remove(name));
}

TEST(Event, CreateReplacesStaleFile)
{
	auto name = testutil::unique_name("ev");
	event_files f{name};
	std::string path = goipc::detail::event_path(name);
	std::ofstream(path) << "stale";
	EXPECT_EQ(errc_of([&] { Event::open(name); }), errc::invalid);
	auto e = Event::create(name);
	e.signal();
	EXPECT_TRUE(e.wait(0ms));
}

TEST(Event, OpenMissingFails)
{
	auto name = testutil::unique_name("ev");
	try {
		Event::open(name);
		FAIL() << "no error";
	} catch (const goipc::error &e) {
		EXPECT_EQ(e.value(), errc::sys);
		EXPECT_EQ(e.code().value(), ENOENT);
	}
}

TEST(Event, InvalidNames)
{
	for (const char *bad : {"", ".", "..", "a/b", "a\\b"}) {
		EXPECT_EQ(errc_of([&] { Event::create(bad); }), errc::invalid_name) << bad;
		EXPECT_EQ(errc_of([&] { Event::open(bad); }), errc::invalid_name) << bad;
	}
}

TEST(Event, CrossProcessWake)
{
	auto name = testutil::unique_name("ev");
	event_files f{name};
	auto e = Event::create(name);
	auto ack_name = testutil::unique_name("ack");
	event_files af{ack_name};
	auto ack = Event::create(ack_name);

	pid_t pid = testutil::fork_child([&] {
		auto mine = Event::open(name);
		auto back = Event::open(ack_name);
		bool woke = mine.wait(10s);
		back.signal();
		return woke ? 0 : 1;
	});
	// The child is parked or about to park; either way the token reaches it.
	e.signal();
	EXPECT_TRUE(ack.wait(10s));
	EXPECT_EQ(testutil::exit_code(testutil::wait_child(pid).status), 0);
	EXPECT_FALSE(e.wait(0ms));
}

} // namespace
