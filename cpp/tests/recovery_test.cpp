// Peer death, claim recovery and name ownership across processes. Each test
// forks, so the TSan build leaves this file out.
#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <goipc/goipc.hpp>

#include "helpers.hpp"

namespace {

using namespace std::chrono_literals;
using goipc::Channel;
using goipc::errc;
using goipc::Queue;
using testutil::bytes_of;
using testutil::errc_of;
using testutil::pipe_pair;
using testutil::text_of;

int finish(pid_t pid) { return testutil::exit_code(testutil::wait_child(pid).status); }

// A producer that claims and exits without a commit leaves a claim the reader cannot pass.
TEST(Recovery, DeadProducerClaimIsPadded)
{
	auto name = testutil::unique_name("rec");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	pid_t pid = testutil::fork_child([&] {
		auto p = Queue::open(name);
		auto c = p.claim(9, 100, 10s);
		std::memset(c.bytes().data(), 0x5A, c.bytes().size());
		::_exit(0);
		return 1;
	});
	ASSERT_EQ(finish(pid), 0);
	ASSERT_EQ(q.ring().buffered(), 112u);
	ASSERT_TRUE(q.try_send(1, bytes_of("after")));

	auto m = q.recv(10s);
	EXPECT_EQ(m.type, 1u);
	EXPECT_EQ(text_of(m.payload), "after");
	EXPECT_TRUE(q.ring().empty());
	// The reclaimed slot's intent is cleared, so a new producer may take it.
	for (int i = 0; i < static_cast<int>(goipc::wire::claim_slots); i++)
		EXPECT_EQ(q.ring().slot_at(i).load(), goipc::wire::no_intent) << "slot " << i;
}

// A reader parked behind a live producer's claim wakes when that producer exits, with no signal from anyone.
TEST(Recovery, ParkedReaderWakesWhenProducerDies)
{
	auto name = testutil::unique_name("rec");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	pipe_pair ready, die;
	pid_t pid = testutil::fork_child([&] {
		auto p = Queue::open(name);
		auto c = p.claim(9, 16, 10s);
		ready.signal();
		die.wait();
		::_exit(0);
		return 1;
	});
	ASSERT_TRUE(ready.wait());
	ASSERT_TRUE(q.try_send(1, bytes_of("after")));

	goipc::message got{};
	std::atomic<int> err{0};
	std::thread reader([&] {
		try {
			got = q.recv(20s);
		} catch (const goipc::error &e) {
			err = static_cast<int>(e.value());
		}
	});
	ASSERT_TRUE(testutil::eventually([&] { return q.ring().recv_waiters() == 1; }));
	die.signal();
	reader.join();
	EXPECT_EQ(finish(pid), 0);
	EXPECT_EQ(err, 0);
	EXPECT_EQ(got.type, 1u);
	EXPECT_EQ(text_of(got.payload), "after");
}

// A claim that crossed the wrap point is reclaimed one lap segment at a time.
TEST(Recovery, DeadProducerClaimAcrossTheWrap)
{
	auto name = testutil::unique_name("rec");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	ASSERT_TRUE(q.try_send(1, std::vector<std::byte>(3000)));
	ASSERT_EQ(q.recv(1s).type, 1u);
	pid_t pid = testutil::fork_child([&] {
		auto p = Queue::open(name);
		auto c = p.claim(9, 1600, 10s);
		::_exit(0);
		return 1;
	});
	ASSERT_EQ(finish(pid), 0);
	ASSERT_EQ(q.ring().tail(), 3008u + 1088u + 1608u);
	std::memset(q.ring().data() + 3008, 0, 8);
	std::memset(q.ring().data(), 0, 8);
	ASSERT_TRUE(q.try_send(2, bytes_of("next")));
	auto m = q.recv(10s);
	EXPECT_EQ(m.type, 2u);
	EXPECT_EQ(text_of(m.payload), "next");
	EXPECT_TRUE(q.ring().empty());
}

// Dead producers whose slots claim different ranges at the cursor leave no way to tell which claim is real.
TEST(Recovery, DisagreeingDeadProducersAreCorrupt)
{
	auto name = testutil::unique_name("rec");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	pid_t pid = testutil::fork_child([&] {
		auto p = Queue::open(name);
		auto c = p.claim(9, 16, 10s);
		::_exit(0);
		return 1;
	});
	ASSERT_EQ(finish(pid), 0);
	auto &r = q.ring();
	int real = -1;
	for (int i = 0; i < static_cast<int>(goipc::wire::claim_slots); i++)
		if (r.slot_at(i).load() == 0)
			real = i;
	ASSERT_GE(real, 0);
	int fake = real == 0 ? 1 : 0;
	r.slot_owner(fake).store(r.slot_owner(real).load());
	r.slot_size(fake).store(64);
	r.slot_at(fake).store(0);
	EXPECT_EQ(errc_of([&] { q.try_recv(); }), errc::corrupt);
}

TEST(Recovery, ReceiverDeathWakesParkedSender)
{
	auto name = testutil::unique_name("rec");
	testutil::queue_files f{name};
	pipe_pair ready, die;
	pid_t pid = testutil::fork_child([&] {
		auto q = Queue::create(name, 4096);
		ready.signal();
		die.wait();
		::_exit(0);
		return 1;
	});
	ASSERT_TRUE(ready.wait());
	auto p = Queue::open(name);
	while (p.try_send(1, std::vector<std::byte>(500))) {
	}
	std::atomic<int> err{0};
	std::thread sender([&] { err = static_cast<int>(errc_of([&] { p.send(1, std::vector<std::byte>(500), 20s); })); });
	ASSERT_TRUE(testutil::eventually([&] { return p.ring().send_waiters() == 1; }));
	die.signal();
	sender.join();
	EXPECT_EQ(err, static_cast<int>(errc::peer_gone));
	EXPECT_EQ(finish(pid), 0);
	EXPECT_EQ(errc_of([&] { p.try_send(1, {}); }), errc::peer_gone);
	EXPECT_EQ(errc_of([&] { Queue::open(name); }), errc::peer_gone);
}

TEST(Recovery, ReceiverCloseWakesParkedSenderInOtherProcess)
{
	auto name = testutil::unique_name("rec");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	while (q.try_send(1, std::vector<std::byte>(500))) {
	}
	pid_t pid = testutil::fork_child([&] {
		auto p = Queue::open(name);
		try {
			p.send(1, std::vector<std::byte>(500), 20s);
		} catch (const goipc::error &e) {
			return e.value() == errc::peer_gone ? 0 : 2;
		}
		return 3;
	});
	ASSERT_TRUE(testutil::eventually([&] { return q.ring().send_waiters() == 1; }));
	q.close();
	EXPECT_EQ(finish(pid), 0);
}

TEST(Recovery, NameIsInUseWhileItsCreatorLives)
{
	auto name = testutil::unique_name("rec");
	testutil::queue_files f{name};
	pipe_pair ready, die;
	pid_t pid = testutil::fork_child([&] {
		auto q = Queue::create(name, 4096);
		ready.signal();
		die.wait();
		::_exit(0);
		return 1;
	});
	ASSERT_TRUE(ready.wait());
	EXPECT_EQ(errc_of([&] { Queue::create(name, 4096); }), errc::in_use);
	std::string old = testutil::read_text("/dev/shm/go-ipc-" + name + ".inc");
	die.signal();
	ASSERT_EQ(finish(pid), 0);

	auto q = Queue::create(name, 4096);
	EXPECT_NE(q.incarnation(), old);
	EXPECT_FALSE(testutil::file_exists("/dev/shm/go-shm-" + name + "." + old));
	auto p = Queue::open(name);
	ASSERT_TRUE(p.try_send(3, bytes_of("new")));
	EXPECT_EQ(q.recv(1s).type, 3u);
}

// The sweep removes a name that no live process holds, with its instance.
TEST(Recovery, SweepRemovesStaleNamesAndLifeSockets)
{
	auto stale = testutil::unique_name("rec");
	auto live = testutil::unique_name("rec");
	testutil::queue_files f1{stale}, f2{live};
	auto keep = Queue::create(live, 4096);
	pipe_pair report;
	pid_t pid = testutil::fork_child([&] {
		auto q = Queue::create(stale, 4096);
		report.send_u64(goipc::detail::self_id());
		::_exit(0);
		return 1;
	});
	std::uint64_t child = report.recv_u64();
	ASSERT_EQ(finish(pid), 0);
	std::string inc = testutil::read_text("/dev/shm/go-ipc-" + stale + ".inc");
	ASSERT_EQ(inc.size(), 16u);
	std::string life = goipc::detail::life_path(child);
	std::vector<std::string> gone = {"/dev/shm/go-ipc-" + stale + ".name", "/dev/shm/go-ipc-" + stale + ".inc",
					 "/dev/shm/go-shm-" + stale + "." + inc,
					 "/dev/shm/go-ipc-" + stale + "." + inc + ".ne.event",
					 "/dev/shm/go-ipc-" + stale + "." + inc + ".nf.event", life};
	for (const auto &p : gone)
		ASSERT_TRUE(testutil::file_exists(p)) << p;

	goipc::detail::sweep_dir("/dev/shm");

	for (const auto &p : gone)
		EXPECT_FALSE(testutil::file_exists(p)) << p;
	EXPECT_TRUE(testutil::file_exists("/dev/shm/go-ipc-" + live + ".name"));
	EXPECT_TRUE(testutil::file_exists("/dev/shm/go-shm-" + live + "." + keep.incarnation()));
	EXPECT_TRUE(testutil::file_exists(goipc::detail::life_path(goipc::detail::self_id())));
	EXPECT_TRUE(keep.try_send(1, {}));
}

TEST(Recovery, ForkedChildHasItsOwnIdentity)
{
	std::uint64_t parent = goipc::detail::self_id();
	pipe_pair report;
	pid_t pid = testutil::fork_child([&] {
		report.send_u64(goipc::detail::self_id());
		return 0;
	});
	std::uint64_t child = report.recv_u64();
	EXPECT_EQ(finish(pid), 0);
	EXPECT_NE(child, parent);
	EXPECT_TRUE(goipc::detail::watchable(child));
	EXPECT_TRUE(goipc::detail::is_dead(child));
	EXPECT_FALSE(goipc::detail::is_dead(parent));
}

TEST(Channel, PeerConnects)
{
	auto name = testutil::unique_name("rch");
	testutil::channel_files f{name};
	auto creator = Channel::create(name, 4096);
	EXPECT_EQ(creator.tx().ring().consumer(), goipc::wire::consumer_pending);
	EXPECT_FALSE(creator.tx().is_receiver());
	// A send before the peer connects waits in the ring.
	ASSERT_TRUE(creator.try_send(1, bytes_of("early")));

	auto opener = Channel::open(name);
	EXPECT_EQ(creator.tx().ring().consumer(), goipc::detail::self_id());
	EXPECT_TRUE(opener.rx().is_receiver());
	EXPECT_EQ(text_of(opener.recv(1s).payload), "early");
	EXPECT_EQ(errc_of([&] { Channel::open(name); }), errc::in_use);
	EXPECT_EQ(errc_of([&] { creator.tx().try_recv(); }), errc::not_consumer);
}

TEST(Channel, CloseGivesPeerPeerGoneAfterItsMessages)
{
	auto name = testutil::unique_name("rch");
	testutil::channel_files f{name};
	auto creator = Channel::create(name, 4096);
	auto opener = Channel::open(name);
	opener.send(1, bytes_of("last"), 1s);
	opener.close();
	EXPECT_EQ(text_of(creator.recv(1s).payload), "last");
	EXPECT_EQ(errc_of([&] { creator.recv(1s); }), errc::peer_gone);
	EXPECT_EQ(errc_of([&] { creator.try_send(1, {}); }), errc::peer_gone);
}

TEST(Channel, CreatorGoneBeforeOpen)
{
	auto name = testutil::unique_name("rch");
	testutil::channel_files f{name};
	auto creator = Channel::create(name, 4096);
	creator.close();
	EXPECT_EQ(errc_of([&] { Channel::open(name); }), errc::peer_gone);
}

// A peer that sent a message and then died has its message delivered first.
TEST(Channel, PeerDeathWakesParkedReceive)
{
	auto name = testutil::unique_name("rch");
	testutil::channel_files f{name};
	auto creator = Channel::create(name, 4096);
	pipe_pair die;
	pid_t pid = testutil::fork_child([&] {
		auto c = Channel::open(name);
		c.send(5, bytes_of("hi"), 10s);
		die.wait();
		::_exit(0);
		return 1;
	});
	auto m = creator.recv(10s);
	EXPECT_EQ(m.type, 5u);
	EXPECT_EQ(text_of(m.payload), "hi");

	std::atomic<int> err{0};
	std::thread reader([&] { err = static_cast<int>(errc_of([&] { creator.recv(20s); })); });
	ASSERT_TRUE(testutil::eventually([&] { return creator.rx().ring().recv_waiters() == 1; }));
	die.signal();
	reader.join();
	EXPECT_EQ(err, static_cast<int>(errc::peer_gone));
	EXPECT_EQ(finish(pid), 0);
	EXPECT_EQ(errc_of([&] { creator.send(1, {}, 1s); }), errc::peer_gone);
}

} // namespace
