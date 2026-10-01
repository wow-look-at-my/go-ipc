#include <chrono>
#include <map>
#include <string>
#include <vector>

#include <sys/stat.h>

#include <gtest/gtest.h>

#include <goipc/goipc.hpp>

#include "helpers.hpp"

namespace {

using namespace std::chrono_literals;
using goipc::errc;
using goipc::Queue;
using testutil::bytes_of;
using testutil::errc_of;
using testutil::text_of;

TEST(Queue, CreateMakesNameAndInstance)
{
	auto name = testutil::unique_name("q");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 8192);
	const std::string inc = q.incarnation();
	ASSERT_EQ(inc.size(), 16u);
	EXPECT_EQ(inc.find_first_not_of("0123456789abcdef"), std::string::npos) << inc;

	struct stat st;
	ASSERT_EQ(::stat(("/dev/shm/go-ipc-" + name + ".name").c_str(), &st), 0);
	EXPECT_EQ(st.st_mode & 0777, 0600u);
	EXPECT_EQ(testutil::read_text("/dev/shm/go-ipc-" + name + ".inc"), inc);
	ASSERT_EQ(::stat(("/dev/shm/go-shm-" + name + "." + inc).c_str(), &st), 0);
	EXPECT_EQ(st.st_size, static_cast<off_t>(goipc::wire::header_size + 8192));
	EXPECT_EQ(st.st_mode & 0777, 0600u);
	for (const char *suffix : {".ne", ".nf"}) {
		ASSERT_EQ(::stat(("/dev/shm/go-ipc-" + name + "." + inc + suffix + ".event").c_str(), &st), 0) << suffix;
		EXPECT_TRUE(S_ISFIFO(st.st_mode));
	}
	EXPECT_EQ(q.name(), name);
	EXPECT_EQ(q.capacity(), 8192u);
	EXPECT_EQ(q.max_message_size(), 4088u);
	EXPECT_TRUE(q.is_receiver());
	EXPECT_EQ(q.ring().consumer(), goipc::detail::self_id());
}

TEST(Queue, DefaultCapacity)
{
	auto name = testutil::unique_name("q");
	testutil::queue_files f{name};
	auto q = Queue::create(name);
	EXPECT_EQ(q.capacity(), goipc::wire::default_capacity);
}

TEST(Queue, InvalidArguments)
{
	for (std::size_t cap : {0ul, 2048ul, 4095ul, 6000ul})
		EXPECT_EQ(errc_of([&] { Queue::create(testutil::unique_name("q"), cap); }), errc::invalid_capacity) << cap;
	EXPECT_EQ(errc_of([&] { Queue::create("a/b", 4096); }), errc::invalid_name);
	EXPECT_EQ(errc_of([&] { Queue::open(""); }), errc::invalid_name);
	try {
		Queue::open(testutil::unique_name("missing"));
		FAIL() << "no error";
	} catch (const goipc::error &e) {
		EXPECT_EQ(e.value(), errc::sys);
		EXPECT_EQ(e.code().value(), ENOENT);
	}
}

TEST(Queue, SendRecvTyped)
{
	auto name = testutil::unique_name("q");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	auto p = Queue::open(name);
	EXPECT_EQ(p.capacity(), 4096u);

	EXPECT_FALSE(q.try_recv().has_value());
	ASSERT_TRUE(p.try_send(7, bytes_of("seven")));
	p.send(8, bytes_of("eight"));
	auto m = q.try_recv();
	ASSERT_TRUE(m.has_value());
	EXPECT_EQ(m->type, 7u);
	EXPECT_EQ(text_of(m->payload), "seven");
	auto m2 = q.recv(1s);
	EXPECT_EQ(m2.type, 8u);
	EXPECT_EQ(text_of(m2.payload), "eight");
}

TEST(Queue, RecvIntoBuffer)
{
	auto name = testutil::unique_name("q");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	ASSERT_TRUE(q.try_send(1, bytes_of("0123456789")));
	std::vector<std::byte> small(3), big(16);
	try {
		q.recv(small, 1s);
		FAIL() << "no error";
	} catch (const goipc::error &e) {
		EXPECT_EQ(e.value(), errc::buffer);
		EXPECT_EQ(e.needed(), 10u);
	}
	EXPECT_EQ(errc_of([&] { q.try_recv(small); }), errc::buffer);
	auto r = q.recv(big, 1s);
	EXPECT_EQ(r.type, 1u);
	EXPECT_EQ(text_of(std::span(big).first(r.size)), "0123456789");
	EXPECT_FALSE(q.try_recv(big).has_value());
}

TEST(Queue, Timeouts)
{
	auto name = testutil::unique_name("q");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	auto start = std::chrono::steady_clock::now();
	EXPECT_EQ(errc_of([&] { q.recv(20ms); }), errc::timed_out);
	EXPECT_GE(std::chrono::steady_clock::now() - start, 20ms);
	EXPECT_EQ(q.ring().recv_waiters(), 0);

	ASSERT_TRUE(q.try_send(1, std::vector<std::byte>(2040)));
	ASSERT_TRUE(q.try_send(1, std::vector<std::byte>(2040)));
	EXPECT_FALSE(q.try_send(1, {}));
	EXPECT_EQ(errc_of([&] { q.send(1, {}, 20ms); }), errc::timed_out);
	EXPECT_EQ(errc_of([&] { q.claim(1, 0, 20ms); }), errc::timed_out);
	EXPECT_EQ(q.ring().send_waiters(), 0);
	EXPECT_EQ(errc_of([&] { q.send(1, std::vector<std::byte>(2041), 20ms); }), errc::too_large);
}

TEST(Queue, ReadBatch)
{
	auto name = testutil::unique_name("q");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	for (std::uint32_t i = 0; i < 5; i++)
		ASSERT_TRUE(q.try_send(i, bytes_of("m" + std::to_string(i))));
	std::vector<std::string> got;
	auto collect = [&](std::uint32_t t, std::span<const std::byte> p) {
		got.push_back(std::to_string(t) + "=" + text_of(p));
	};
	EXPECT_EQ(q.read_batch(2, collect, 1s), 2u);
	EXPECT_EQ(q.read_batch(10, collect, 1s), 3u);
	EXPECT_EQ(got, (std::vector<std::string>{"0=m0", "1=m1", "2=m2", "3=m3", "4=m4"}));
	EXPECT_EQ(q.try_read_batch(10, collect), 0u);
	EXPECT_EQ(errc_of([&] { q.read_batch(10, collect, 10ms); }), errc::timed_out);
}

TEST(Queue, ClaimCommitAbort)
{
	auto name = testutil::unique_name("q");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);

	auto c = q.claim(4, 5, 1s);
	std::memcpy(c.bytes().data(), "claim", 5);
	EXPECT_FALSE(q.try_recv().has_value());
	c.commit();
	auto m = q.try_recv();
	ASSERT_TRUE(m.has_value());
	EXPECT_EQ(m->type, 4u);
	EXPECT_EQ(text_of(m->payload), "claim");

	auto a = q.claim(5, 100, 1s);
	a.abort();
	EXPECT_FALSE(q.try_recv().has_value());
	EXPECT_TRUE(q.ring().empty());

	{
		auto dropped = q.try_claim(6, 10);
		ASSERT_TRUE(dropped.has_value());
	}
	EXPECT_FALSE(q.try_recv().has_value());
	EXPECT_TRUE(q.ring().empty());

	while (q.try_send(1, {})) {
	}
	EXPECT_FALSE(q.try_claim(1, 0).has_value());
}

TEST(Queue, ClaimAfterCloseIsInert)
{
	auto name = testutil::unique_name("q");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	auto c = q.claim(1, 8, 1s);
	q.close();
	EXPECT_NO_THROW(c.commit());
}

TEST(Queue, UseAfterClose)
{
	auto name = testutil::unique_name("q");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	q.close();
	q.close();
	EXPECT_EQ(errc_of([&] { q.try_send(1, {}); }), errc::closed);
	EXPECT_EQ(errc_of([&] { q.send(1, {}); }), errc::closed);
	EXPECT_EQ(errc_of([&] { q.try_recv(); }), errc::closed);
	EXPECT_EQ(errc_of([&] { q.recv(); }), errc::closed);
	EXPECT_EQ(errc_of([&] { q.claim(1, 1); }), errc::closed);
	EXPECT_EQ(errc_of([&] { q.read_batch(1, [](std::uint32_t, std::span<const std::byte>) {}); }), errc::closed);
}

TEST(Queue, UnlinkRemovesNameAndInstance)
{
	auto name = testutil::unique_name("q");
	auto q = Queue::create(name, 4096);
	std::string inst = name + "." + q.incarnation();
	std::vector<std::string> paths = {"/dev/shm/go-ipc-" + name + ".name", "/dev/shm/go-ipc-" + name + ".inc",
					  "/dev/shm/go-shm-" + inst, "/dev/shm/go-ipc-" + inst + ".ne.event",
					  "/dev/shm/go-ipc-" + inst + ".nf.event"};
	for (const auto &p : paths)
		ASSERT_TRUE(testutil::file_exists(p)) << p;
	q.unlink();
	for (const auto &p : paths)
		EXPECT_FALSE(testutil::file_exists(p)) << p;
	ASSERT_TRUE(q.try_send(1, bytes_of("still works")));
	EXPECT_TRUE(q.try_recv().has_value());
	EXPECT_NO_THROW(Queue::remove(name));
}

TEST(Queue, CreateReplacesStaleInstance)
{
	auto name = testutil::unique_name("q");
	testutil::queue_files f{name};
	std::string old_inc;
	{
		auto old = Queue::create(name, 8192);
		old_inc = old.incarnation();
		ASSERT_TRUE(old.try_send(1, bytes_of("stale")));
	}
	auto q = Queue::create(name, 4096);
	EXPECT_NE(q.incarnation(), old_inc);
	EXPECT_FALSE(testutil::file_exists("/dev/shm/go-shm-" + name + "." + old_inc));
	EXPECT_EQ(q.capacity(), 4096u);
	EXPECT_FALSE(q.try_recv().has_value());
}

TEST(Queue, OnlyTheCreatorReceives)
{
	auto name = testutil::unique_name("q");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	auto p = Queue::open(name);
	EXPECT_FALSE(p.is_receiver());
	ASSERT_TRUE(p.try_send(1, bytes_of("x")));
	std::vector<std::byte> buf(8);
	EXPECT_EQ(errc_of([&] { p.try_recv(); }), errc::not_consumer);
	EXPECT_EQ(errc_of([&] { p.try_recv(buf); }), errc::not_consumer);
	EXPECT_EQ(errc_of([&] { p.recv(10ms); }), errc::not_consumer);
	EXPECT_EQ(errc_of([&] { p.read_batch(1, [](std::uint32_t, std::span<const std::byte>) {}, 10ms); }),
		  errc::not_consumer);
	EXPECT_TRUE(q.try_recv().has_value());
}

TEST(Queue, SecondCreatorInSameProcessIsInUse)
{
	auto name = testutil::unique_name("q");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	EXPECT_EQ(errc_of([&] { Queue::create(name, 4096); }), errc::in_use);
	q.close();
	EXPECT_NO_THROW(Queue::create(name, 4096));
}

TEST(Queue, ReceiverCloseGivesSendersPeerGone)
{
	auto name = testutil::unique_name("q");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	auto p = Queue::open(name);
	ASSERT_TRUE(p.try_send(1, bytes_of("before")));
	q.close();
	EXPECT_EQ(p.ring().consumer(), 0u);
	EXPECT_EQ(errc_of([&] { p.try_send(1, bytes_of("after")); }), errc::peer_gone);
	EXPECT_EQ(errc_of([&] { p.send(1, bytes_of("after"), 1s); }), errc::peer_gone);
	EXPECT_EQ(errc_of([&] { p.claim(1, 4, 1s); }), errc::peer_gone);
	EXPECT_EQ(errc_of([&] { Queue::open(name); }), errc::peer_gone);
}

// A claim holds its slot's intent from before the tail swap until its commit.
TEST(Queue, ClaimSlotIntent)
{
	auto name = testutil::unique_name("q");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	auto p = Queue::open(name);
	auto &r = p.ring();
	const std::uint64_t self = goipc::detail::self_id();
	auto open_slots = [&] {
		std::vector<int> out;
		for (int i = 0; i < static_cast<int>(goipc::wire::claim_slots); i++)
			if (r.slot_owner(i).load() == self && r.slot_at(i).load() != goipc::wire::no_intent)
				out.push_back(i);
		return out;
	};

	ASSERT_TRUE(p.try_send(1, bytes_of("abc")));
	EXPECT_TRUE(open_slots().empty());

	std::uint64_t tail = r.tail();
	auto c = p.claim(2, 10, 1s);
	auto held = open_slots();
	ASSERT_EQ(held.size(), 1u);
	EXPECT_EQ(r.slot_at(held[0]).load(), tail);
	EXPECT_EQ(r.slot_size(held[0]).load(), 24u);
	c.commit();
	EXPECT_TRUE(open_slots().empty());
	EXPECT_EQ(r.slot_owner(held[0]).load(), self);

	// A full ring releases the intent of the claim it refused.
	while (p.try_send(1, std::vector<std::byte>(500))) {
	}
	EXPECT_TRUE(open_slots().empty());

	// A close returns the slots to the shared pool.
	p.close();
	EXPECT_EQ(q.ring().slot_owner(held[0]).load(), 0u);
}

TEST(Queue, TooManyClaims)
{
	auto name = testutil::unique_name("q");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 8192);
	std::vector<goipc::Claim> claims;
	for (std::size_t i = 0; i < goipc::wire::claim_slots; i++) {
		auto c = q.try_claim(1, 0);
		ASSERT_TRUE(c.has_value()) << i;
		claims.push_back(std::move(*c));
	}
	EXPECT_EQ(errc_of([&] { q.try_claim(1, 0); }), errc::too_many_claims);
	EXPECT_EQ(errc_of([&] { q.try_send(1, {}); }), errc::too_many_claims);
	claims.back().commit();
	claims.pop_back();
	EXPECT_TRUE(q.try_send(7, {}));
	claims.clear();
	std::size_t n = 0;
	q.try_read_batch(SIZE_MAX, [&](std::uint32_t t, std::span<const std::byte>) { n += t == 1 ? 1 : 0; });
	EXPECT_EQ(n, 1u);
}

TEST(Queue, CrossProcessOrdering)
{
	constexpr int senders = 3;
	constexpr int per_sender = 3000;
	auto name = testutil::unique_name("q");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);

	std::vector<pid_t> pids;
	for (int s = 0; s < senders; s++) {
		pids.push_back(testutil::fork_child([&, s] {
			auto p = Queue::open(name);
			for (int i = 0; i < per_sender; i++) {
				std::string payload = std::to_string(s) + ":" + std::to_string(i);
				p.send(static_cast<std::uint32_t>(i), bytes_of(payload), 30s);
			}
			p.close();
			return 0;
		}));
	}

	std::map<std::string, int> next;
	std::vector<std::byte> buf(q.max_message_size());
	for (int i = 0; i < senders * per_sender; i++) {
		auto r = q.recv(buf, 30s);
		std::string text = text_of(std::span(buf).first(r.size));
		auto colon = text.find(':');
		ASSERT_NE(colon, std::string::npos) << text;
		int seq = std::stoi(text.substr(colon + 1));
		ASSERT_EQ(static_cast<int>(r.type), seq);
		ASSERT_EQ(seq, next[text.substr(0, colon)]++) << text;
	}
	for (pid_t pid : pids)
		EXPECT_EQ(testutil::exit_code(testutil::wait_child(pid).status), 0);
	EXPECT_FALSE(q.try_recv().has_value());
}

TEST(Queue, CrossProcessBlockingRecvWakes)
{
	auto name = testutil::unique_name("q");
	auto ping = testutil::unique_name("ping");
	testutil::queue_files f{name}, f2{ping};
	auto q = Queue::create(name, 4096);
	testutil::pipe_pair go;
	pid_t pid = testutil::fork_child([&] {
		auto in = Queue::create(ping, 4096);
		auto out = Queue::open(name);
		go.signal();
		auto m = in.recv(30s);
		out.send(m.type + 1, m.payload, 30s);
		return 0;
	});
	ASSERT_TRUE(go.wait());
	auto p = Queue::open(ping);
	// Wait for the child to park so the wake path is the one under test.
	ASSERT_TRUE(testutil::eventually([&] { return p.ring().recv_waiters() > 0; }));
	p.send(41, bytes_of("ping"), 10s);
	auto m = q.recv(10s);
	EXPECT_EQ(m.type, 42u);
	EXPECT_EQ(text_of(m.payload), "ping");
	EXPECT_EQ(testutil::exit_code(testutil::wait_child(pid).status), 0);
}

} // namespace
