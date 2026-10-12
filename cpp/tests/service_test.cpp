#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <spawn.h>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <goipc/goipc.hpp>

#include "demo.hpp"
#include "helpers.hpp"

extern char **environ;

namespace {

using namespace std::chrono_literals;
using goipc::Client;
using goipc::errc;
using goipc::Service;

constexpr std::uint32_t t_echo = 1;
constexpr std::uint32_t t_echoed = 2;
constexpr std::uint32_t t_fail = 3;
constexpr std::uint32_t t_who = 4;
constexpr std::uint32_t t_whoami = 5;
// A park request is answered once the test releases the handler.
constexpr std::uint32_t t_park = 6;
constexpr std::uint32_t t_parked = 7;

// fixture counts the clients that went and holds the park requests.
struct fixture {
	std::mutex mu;
	std::condition_variable cv;
	int gone = 0;
	bool released = false;

	goipc::reply handle(goipc::Session &s, std::uint32_t type, std::span<const std::byte> payload)
	{
		switch (type) {
		case t_echo:
			return {t_echoed, std::vector<std::byte>(payload.begin(), payload.end())};
		case t_fail:
			throw std::runtime_error(testutil::text_of(payload));
		case t_who: {
			std::uint64_t ordinal = s.ordinal();
			goipc::reply r{t_whoami, std::vector<std::byte>(sizeof ordinal)};
			std::memcpy(r.payload.data(), &ordinal, sizeof ordinal);
			return r;
		}
		case t_park: {
			std::unique_lock l(mu);
			cv.wait(l, [&] { return released; });
			return {t_parked, {}};
		}
		}
		throw std::runtime_error("not a request");
	}

	void on_gone(goipc::Session &)
	{
		std::lock_guard l(mu);
		gone++;
		cv.notify_all();
	}

	void release()
	{
		std::lock_guard l(mu);
		released = true;
		cv.notify_all();
	}

	int wait_gone(int want)
	{
		std::unique_lock l(mu);
		cv.wait(l, [&] { return gone >= want; });
		return gone;
	}

	Service serve(const std::string &name)
	{
		return Service::serve(
			name, [this](goipc::Session &s, std::uint32_t t, std::span<const std::byte> p) { return handle(s, t, p); },
			[this](goipc::Session &s) { on_gone(s); }, 4096);
	}
};

struct service_files {
	std::string name;
	~service_files() { goipc::Queue::remove(name + ".svc"); }
};

std::string echo(Client &c, const std::string &text)
{
	goipc::message m = c.call(t_echo, testutil::bytes_of(text), 5s);
	EXPECT_EQ(m.type, t_echoed);
	return testutil::text_of(m.payload);
}

TEST(Service, AnswersCalls)
{
	auto name = testutil::unique_name("svc");
	service_files f{name};
	fixture fx;
	auto svc = fx.serve(name);
	auto c = Client::connect(name, 5s, 4096);
	EXPECT_EQ(c.ordinal(), 0u);
	EXPECT_EQ(c.max_payload_size(), 2040 - goipc::wire::service_sequence_size);
	EXPECT_EQ(echo(c, "hello"), "hello");
	EXPECT_EQ(echo(c, ""), "");

	try {
		c.call(t_fail, testutil::bytes_of("boom"), 5s);
		FAIL() << "fail returned a reply";
	} catch (const goipc::call_error &e) {
		EXPECT_EQ(e.message(), "boom");
		EXPECT_EQ(e.value(), errc::call);
		EXPECT_EQ(std::string(e.what()).find("boom"), std::string("goipc: call failed: ").size());
	}
	try {
		c.call(99, {}, 5s);
		FAIL() << "an unknown type returned a reply";
	} catch (const goipc::call_error &e) {
		EXPECT_EQ(e.message(), "not a request");
	}
	EXPECT_EQ(echo(c, "after"), "after");

	c.close();
	EXPECT_EQ(fx.wait_gone(1), 1);
	svc.close();
	EXPECT_FALSE(testutil::file_exists("/dev/shm/go-ipc-" + name + ".svc.name"));
}

TEST(Service, GivesEachClientAnOrdinal)
{
	auto name = testutil::unique_name("svc");
	service_files f{name};
	fixture fx;
	auto svc = fx.serve(name);
	std::vector<Client> clients;
	std::vector<bool> seen(3, false);
	for (int i = 0; i < 3; i++) {
		clients.push_back(Client::connect(name, 5s, 4096));
		goipc::message m = clients.back().call(t_who, {}, 5s);
		ASSERT_EQ(m.type, t_whoami);
		ASSERT_EQ(m.payload.size(), 8u);
		std::uint64_t ordinal = 0;
		std::memcpy(&ordinal, m.payload.data(), sizeof ordinal);
		EXPECT_EQ(ordinal, clients.back().ordinal());
		ASSERT_LT(ordinal, 3u);
		EXPECT_FALSE(seen[ordinal]) << "ordinal " << ordinal << " twice";
		seen[ordinal] = true;
	}
	for (auto &c : clients)
		c.close();
	EXPECT_EQ(fx.wait_gone(3), 3);
}

// A client that connects before the service exists is adopted when it starts.
TEST(Service, ClientWaitsForTheService)
{
	auto name = testutil::unique_name("svc");
	service_files f{name};
	Client early;
	std::thread t([&] { early = Client::connect(name, 5s, 4096); });
	fixture fx;
	auto svc = fx.serve(name);
	t.join();
	EXPECT_EQ(echo(early, "early"), "early");
	early.close();
	EXPECT_EQ(fx.wait_gone(1), 1);
}

TEST(Service, CloseFailsEveryCall)
{
	auto name = testutil::unique_name("svc");
	service_files f{name};
	fixture fx;
	auto svc = fx.serve(name);
	auto c = Client::connect(name, 5s, 4096);
	EXPECT_EQ(echo(c, "before"), "before");
	svc.close();
	EXPECT_EQ(testutil::errc_of([&] { c.call(t_echo, testutil::bytes_of("x"), 5s); }), errc::peer_gone);
	c.close();
	EXPECT_EQ(testutil::errc_of([&] { Client::connect(name, 50ms, 4096); }), errc::timed_out);
}

TEST(Service, NameIsHeldWhileItRuns)
{
	auto name = testutil::unique_name("svc");
	service_files f{name};
	fixture fx;
	auto svc = fx.serve(name);
	EXPECT_EQ(testutil::errc_of([&] { fx.serve(name); }), errc::in_use);
	svc.close();
	auto again = fx.serve(name);
	again.close();
}

// A reply to a call that timed out is discarded by the next call.
TEST(Service, ClientDiscardsAStaleReply)
{
	auto name = testutil::unique_name("svc");
	service_files f{name};
	fixture fx;
	auto svc = fx.serve(name);
	auto c = Client::connect(name, 5s, 4096);
	EXPECT_EQ(testutil::errc_of([&] { c.call(t_park, {}, 50ms); }), errc::timed_out);
	fx.release();
	EXPECT_EQ(echo(c, "fresh"), "fresh");
	c.close();
	svc.close();
}

TEST(Service, RejectsBadCalls)
{
	auto name = testutil::unique_name("svc");
	service_files f{name};
	fixture fx;
	auto svc = fx.serve(name);
	auto c = Client::connect(name, 5s, 4096);
	EXPECT_EQ(testutil::errc_of([&] { c.call(goipc::wire::service_type_knock, {}, 5s); }), errc::reserved_type);
	std::vector<std::byte> big(c.max_payload_size() + 1);
	EXPECT_EQ(testutil::errc_of([&] { c.call(t_echo, big, 5s); }), errc::too_large);
	EXPECT_EQ(echo(c, "still fine"), "still fine");
	c.close();
	svc.close();

	EXPECT_EQ(testutil::errc_of([&] { fx.serve("a/b"); }), errc::invalid_name);
	EXPECT_EQ(testutil::errc_of([&] { Service::serve(name, nullptr); }), errc::invalid);
	EXPECT_EQ(testutil::errc_of([&] { Client::connect("a/b", 50ms); }), errc::invalid_name);
}

// A handler that returns a reserved reply type sends an error instead.
TEST(Service, HandlerCannotReplyWithAReservedType)
{
	auto name = testutil::unique_name("svc");
	service_files f{name};
	auto svc = Service::serve(
		name, [](goipc::Session &, std::uint32_t, std::span<const std::byte>) { return goipc::reply{goipc::wire::service_type_hello, {}}; },
		{}, 4096);
	auto c = Client::connect(name, 5s, 4096);
	try {
		c.call(t_echo, testutil::bytes_of("x"), 5s);
		FAIL() << "a reserved reply type reached the client";
	} catch (const goipc::call_error &e) {
		EXPECT_EQ(e.message(), "goipc: reply type 4294967281 is reserved");
	}
	c.close();
	svc.close();
}

// The typed layer: a handler over ipcgen messages, and a typed call.
TEST(Service, TypedCalls)
{
	auto name = testutil::unique_name("svc");
	service_files f{name};
	auto svc = Service::serve(
		name,
		goipc::typed_handler<demo::Vec2>([](goipc::Session &s, const demo::Vec2 &v) {
			demo::Vec2 out = v;
			out.x += static_cast<float>(s.ordinal()) + 1.0f;
			return out;
		}),
		{}, 4096);
	auto c = Client::connect(name, 5s, 4096);
	demo::Vec2 req{};
	req.x = 1.5f;
	req.y = -2.0f;
	demo::Vec2 rep = c.call<demo::Vec2>(req, 5s);
	EXPECT_EQ(rep.x, 2.5f);
	EXPECT_EQ(rep.y, -2.0f);
	try {
		c.call(demo::Scalars::type_id, {}, 5s);
		FAIL() << "an unknown message type returned a reply";
	} catch (const goipc::call_error &e) {
		EXPECT_EQ(e.message().find("goipc: no message has type ID"), 0u) << e.message();
	}
	c.close();
	svc.close();
}

// ---- the service cells of spec/peer.md, through goipc-peer ----

struct peer {
	pid_t pid;
	int out;
};

peer spawn_peer(std::vector<std::string> args)
{
	args.insert(args.begin(), GOIPC_PEER_BIN);
	std::vector<char *> argv;
	for (auto &a : args)
		argv.push_back(a.data());
	argv.push_back(nullptr);
	int fds[2];
	if (::pipe2(fds, O_CLOEXEC) != 0)
		throw std::runtime_error("pipe2 failed");
	posix_spawn_file_actions_t fa;
	posix_spawn_file_actions_init(&fa);
	posix_spawn_file_actions_adddup2(&fa, fds[1], STDOUT_FILENO);
	pid_t pid;
	int err = ::posix_spawn(&pid, argv[0], &fa, nullptr, argv.data(), environ);
	posix_spawn_file_actions_destroy(&fa);
	::close(fds[1]);
	if (err != 0) {
		::close(fds[0]);
		throw std::runtime_error("posix_spawn " + args[0] + ": " + std::to_string(err));
	}
	return {pid, fds[0]};
}

std::string read_line(int fd)
{
	std::string line;
	char c;
	while (::read(fd, &c, 1) == 1) {
		if (c == '\n')
			break;
		line += c;
	}
	return line;
}

int finish(peer &p)
{
	::close(p.out);
	return testutil::exit_code(testutil::wait_child(p.pid).status);
}

TEST(Peer, ServiceCell)
{
	auto name = testutil::unique_name("peersvc");
	service_files f{name};
	auto serve = spawn_peer({"service-serve", name, "4096", "2"});
	ASSERT_EQ(read_line(serve.out), "ready");
	auto closer = spawn_peer({"service-call", name, "200", "close"});
	auto exiter = spawn_peer({"service-call", name, "200", "exit"});
	EXPECT_EQ(read_line(closer.out), "ok 200");
	EXPECT_EQ(finish(closer), 0);
	EXPECT_EQ(read_line(exiter.out), "ok 200");
	EXPECT_EQ(finish(exiter), 0);
	EXPECT_EQ(read_line(serve.out), "ok 2");
	EXPECT_EQ(finish(serve), 0);
	EXPECT_FALSE(testutil::file_exists("/dev/shm/go-ipc-" + name + ".svc.name"));
}

TEST(Peer, ServiceEarlyCell)
{
	auto name = testutil::unique_name("peerearly");
	service_files f{name};
	auto caller = spawn_peer({"service-call", name, "200", "close"});
	auto serve = spawn_peer({"service-serve", name, "4096", "1"});
	ASSERT_EQ(read_line(serve.out), "ready");
	EXPECT_EQ(read_line(caller.out), "ok 200");
	EXPECT_EQ(finish(caller), 0);
	EXPECT_EQ(read_line(serve.out), "ok 1");
	EXPECT_EQ(finish(serve), 0);
}

// A child parks one client in a call, and another in a connect to a service that never comes, for the whole window.
TEST(Blocking, ParkedCallConsumesNoCPU)
{
	constexpr auto window = 300ms;
	constexpr auto budget = window / 10;
	auto name = testutil::unique_name("svcpark");
	auto absent = testutil::unique_name("svcabsent");
	service_files f{name};

	auto start = std::chrono::steady_clock::now();
	pid_t pid = testutil::fork_child([&] {
		auto svc = Service::serve(
			name,
			[&](goipc::Session &, std::uint32_t, std::span<const std::byte>) {
				// The handler outlives the client's timeout, so the client parks the whole window.
				std::this_thread::sleep_for(window * 2);
				return goipc::reply{t_parked, {}};
			},
			{}, 4096);
		auto c = Client::connect(name, 5s, 4096);
		std::atomic<int> timed_out{0};
		std::thread waiter([&] {
			try {
				Client::connect(absent, window, 4096);
			} catch (const goipc::error &e) {
				if (e.value() == errc::timed_out)
					timed_out++;
			}
		});
		try {
			c.call(t_park, {}, window);
		} catch (const goipc::error &e) {
			if (e.value() == errc::timed_out)
				timed_out++;
		}
		c.close();
		svc.close();
		waiter.join();
		return timed_out == 2 ? 0 : 1;
	});
	auto res = testutil::wait_child(pid);
	auto wall = std::chrono::steady_clock::now() - start;
	EXPECT_EQ(testutil::exit_code(res.status), 0) << "both parked waits must end in a timeout";
	EXPECT_GE(wall, window);
	EXPECT_LT(res.cpu, budget) << "a parked call and connect burned " << res.cpu.count() << "us of CPU across a "
				   << std::chrono::duration_cast<std::chrono::milliseconds>(window).count()
				   << "ms window: something is spinning";
}

} // namespace
