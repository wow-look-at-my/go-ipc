// Threaded tests. The TSan build runs this file alone, so nothing here forks.
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
using goipc::errc;
using goipc::Event;
using goipc::Queue;
using goipc::Ring;
using testutil::errc_of;

struct stamp {
	std::uint32_t producer;
	std::uint32_t seq;
};

std::vector<std::byte> encode(std::uint32_t producer, std::uint32_t seq)
{
	// The length varies with seq, so records land on every alignment.
	std::vector<std::byte> b(sizeof(stamp) + seq % 61, static_cast<std::byte>(seq));
	stamp s{producer, seq};
	std::memcpy(b.data(), &s, sizeof s);
	return b;
}

stamp decode(std::span<const std::byte> p)
{
	stamp s;
	std::memcpy(&s, p.data(), sizeof s);
	return s;
}

TEST(Concurrency, RingMPSCStress)
{
	constexpr std::uint32_t producers = 4;
	constexpr std::uint32_t per_producer = 20000;
	testutil::aligned_buffer buf(Ring::size_for(1 << 16));
	auto ring = Ring::init(buf.span());

	std::atomic<bool> stop{false};
	std::vector<std::thread> threads;
	for (std::uint32_t p = 0; p < producers; p++) {
		threads.emplace_back([&, p] {
			auto view = Ring::attach(buf.span());
			for (std::uint32_t i = 0; i < per_producer && !stop; i++) {
				auto msg = encode(p, i);
				// A Ring alone has no way to block, so a full ring is retried.
				while (!view.try_write(p, msg) && !stop) {
				}
			}
		});
	}

	std::vector<std::uint32_t> next(producers, 0);
	std::uint64_t total = 0;
	std::string failure;
	try {
		while (total < producers * per_producer && failure.empty()) {
			total += ring.read(SIZE_MAX, [&](std::uint32_t t, std::span<const std::byte> p) {
				stamp s = decode(p);
				std::size_t extra = s.seq % 61;
				if (failure.empty() &&
				    (t >= producers || s.producer != t || s.seq != next[t] || p.size() != sizeof(stamp) + extra ||
				     (extra && p.back() != static_cast<std::byte>(s.seq))))
					failure = "type " + std::to_string(t) + " producer " + std::to_string(s.producer) + " seq " +
						  std::to_string(s.seq) + " size " + std::to_string(p.size()) + " after " +
						  std::to_string(total) + " records";
				if (t < producers)
					next[t]++;
			});
		}
	} catch (const std::exception &e) {
		failure = std::string(e.what()) + " after " + std::to_string(total) + " records";
	}
	stop = true;
	for (auto &t : threads)
		t.join();
	ASSERT_EQ(failure, "");
	for (std::uint32_t p = 0; p < producers; p++)
		EXPECT_EQ(next[p], per_producer);
	EXPECT_TRUE(ring.empty());
}

TEST(Concurrency, QueueMPSCBlocking)
{
	constexpr std::uint32_t producers = 4;
	constexpr std::uint32_t per_producer = 5000;
	auto name = testutil::unique_name("cq");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);

	std::vector<std::thread> threads;
	for (std::uint32_t p = 0; p < producers; p++) {
		threads.emplace_back([&, p] {
			auto tx = Queue::open(name);
			for (std::uint32_t i = 0; i < per_producer; i++) {
				if (i % 3 == 0) {
					auto msg = encode(p, i);
					auto c = tx.claim(p, msg.size(), 30s);
					std::memcpy(c.bytes().data(), msg.data(), msg.size());
					c.commit();
				} else {
					tx.send(p, encode(p, i), 30s);
				}
				if (i % 7 == 0)
					tx.claim(p, 100, 30s).abort();
			}
		});
	}

	std::vector<std::uint32_t> next(producers, 0);
	std::uint64_t total = 0;
	bool ok = true;
	std::vector<std::byte> buf(q.max_message_size());
	while (total < producers * per_producer) {
		if (total % 2 == 0) {
			auto r = q.recv(buf, 30s);
			stamp s = decode(std::span(buf).first(r.size));
			ok = ok && s.producer == r.type && s.seq == next[r.type];
			next[r.type]++;
			total++;
		} else {
			total += q.read_batch(
				16,
				[&](std::uint32_t t, std::span<const std::byte> p) {
					stamp s = decode(p);
					ok = ok && s.producer == t && s.seq == next[t];
					next[t]++;
				},
				30s);
		}
	}
	for (auto &t : threads)
		t.join();
	EXPECT_TRUE(ok);
	for (std::uint32_t p = 0; p < producers; p++)
		EXPECT_EQ(next[p], per_producer);
	EXPECT_FALSE(q.try_recv().has_value());
	EXPECT_EQ(q.ring().send_waiters(), 0);
	EXPECT_EQ(q.ring().recv_waiters(), 0);
}

TEST(Concurrency, BlockingRecvWokenBySend)
{
	auto name = testutil::unique_name("cq");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	auto tx = Queue::open(name);
	goipc::message got{};
	std::thread t([&] { got = q.recv(30s); });
	ASSERT_TRUE(testutil::eventually([&] { return q.ring().recv_waiters() == 1; }));
	tx.send(3, testutil::bytes_of("wake"));
	t.join();
	EXPECT_EQ(got.type, 3u);
	EXPECT_EQ(testutil::text_of(got.payload), "wake");
}

TEST(Concurrency, FullQueueBlocksSenderUntilDrained)
{
	auto name = testutil::unique_name("cq");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	std::vector<std::byte> big(2040);
	ASSERT_TRUE(q.try_send(1, big));
	ASSERT_TRUE(q.try_send(2, big));

	std::atomic<bool> sent{false};
	std::thread t([&] {
		q.send(3, big, 30s);
		sent = true;
	});
	ASSERT_TRUE(testutil::eventually([&] { return q.ring().send_waiters() == 1; }));
	EXPECT_FALSE(sent);
	EXPECT_EQ(q.recv(1s).type, 1u);
	t.join();
	EXPECT_TRUE(sent);
	EXPECT_EQ(q.recv(1s).type, 2u);
	EXPECT_EQ(q.recv(1s).type, 3u);
}

TEST(Concurrency, AbortWakesBlockedSender)
{
	auto name = testutil::unique_name("cq");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	auto c = q.claim(1, 2040, 1s);
	ASSERT_TRUE(q.try_send(2, std::vector<std::byte>(2040)));
	std::thread t([&] { q.send(3, std::vector<std::byte>(2040), 30s); });
	ASSERT_TRUE(testutil::eventually([&] { return q.ring().send_waiters() == 1; }));
	c.abort();
	// The receiver steps over the aborted claim, and that frees the room.
	EXPECT_EQ(q.recv(1s).type, 2u);
	t.join();
	EXPECT_EQ(q.recv(1s).type, 3u);
}

TEST(Concurrency, CloseReleasesBlockedRecvAndSend)
{
	auto name = testutil::unique_name("cq");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	auto other = testutil::unique_name("cq");
	testutil::queue_files f2{other};
	auto full = Queue::create(other, 4096);
	std::vector<std::byte> big(2040);
	ASSERT_TRUE(full.try_send(1, big));
	ASSERT_TRUE(full.try_send(1, big));

	std::atomic<int> recv_err{0}, send_err{0};
	std::thread r([&] { recv_err = static_cast<int>(errc_of([&] { q.recv(); })); });
	std::thread s([&] { send_err = static_cast<int>(errc_of([&] { full.send(1, big); })); });
	ASSERT_TRUE(testutil::eventually([&] { return q.ring().recv_waiters() == 1 && full.ring().send_waiters() == 1; }));
	q.close();
	full.close();
	r.join();
	s.join();
	EXPECT_EQ(recv_err, static_cast<int>(errc::closed));
	EXPECT_EQ(send_err, static_cast<int>(errc::closed));
}

TEST(Concurrency, CloseWhileSendersRun)
{
	auto name = testutil::unique_name("cq");
	testutil::queue_files f{name};
	auto q = Queue::create(name, 4096);
	std::atomic<int> started{0};
	std::vector<std::thread> threads;
	for (int i = 0; i < 4; i++) {
		threads.emplace_back([&] {
			started++;
			try {
				for (;;)
					q.send(1, std::vector<std::byte>(300));
			} catch (const goipc::error &e) {
				EXPECT_EQ(e.value(), errc::closed);
			}
		});
	}
	std::thread reader([&] {
		try {
			for (;;)
				q.read_batch(8, [](std::uint32_t, std::span<const std::byte>) {});
		} catch (const goipc::error &e) {
			EXPECT_EQ(e.value(), errc::closed);
		}
	});
	ASSERT_TRUE(testutil::eventually([&] { return started == 4 && q.ring().tail() > 100000; }));
	q.close();
	for (auto &t : threads)
		t.join();
	reader.join();
}

TEST(Concurrency, EventCloseReleasesWaiters)
{
	auto name = testutil::unique_name("cev");
	struct cleanup {
		std::string n;
		~cleanup() { Event::remove(n); }
	} c{name};
	auto e = Event::create(name);
	std::atomic<int> closed{0};
	std::vector<std::thread> threads;
	for (int i = 0; i < 3; i++) {
		threads.emplace_back([&] {
			if (errc_of([&] { e.wait(); }) == errc::closed)
				closed++;
		});
	}
	// No observable counter exists for an Event, so the waiters get a moment to park. The test holds either way.
	std::this_thread::sleep_for(50ms);
	e.close();
	for (auto &t : threads)
		t.join();
	EXPECT_EQ(closed, 3);
}

TEST(Concurrency, EventManyWaitersEachTakeOneToken)
{
	auto name = testutil::unique_name("cev");
	struct cleanup {
		std::string n;
		~cleanup() { Event::remove(n); }
	} c{name};
	auto e = Event::create(name);
	auto opener = Event::open(name);
	std::atomic<int> woke{0};
	std::vector<std::thread> threads;
	for (int i = 0; i < 6; i++) {
		threads.emplace_back([&, i] {
			Event &mine = i % 2 ? e : opener;
			if (mine.wait(30s))
				woke++;
		});
	}
	e.signal(6);
	for (auto &t : threads)
		t.join();
	EXPECT_EQ(woke, 6);
	EXPECT_FALSE(e.wait(0ms));
}

TEST(Concurrency, ConnFullDuplex)
{
	auto name = testutil::unique_name("cconn");
	testutil::channel_files f{name};
	auto l = goipc::Conn::listen(name, 4096);
	auto d = goipc::Conn::dial(name);
	constexpr std::size_t n = 300000;
	std::vector<std::byte> want(n);
	for (std::size_t i = 0; i < n; i++)
		want[i] = static_cast<std::byte>((i * 31 + 7) % 256);

	std::thread echo([&] {
		std::vector<std::byte> buf(1500);
		for (;;) {
			std::size_t k = l.read(buf, 30s);
			if (k == 0)
				break;
			l.write(std::span(buf).first(k), 30s);
		}
		l.close();
	});
	std::thread writer([&] {
		d.write(want, 30s);
		d.close_write(30s);
	});
	std::vector<std::byte> got;
	std::vector<std::byte> buf(777);
	for (;;) {
		std::size_t k = d.read(buf, 30s);
		if (k == 0)
			break;
		got.insert(got.end(), buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(k));
	}
	writer.join();
	echo.join();
	EXPECT_EQ(got, want);
}

TEST(Concurrency, ConnCloseReleasesBlockedRead)
{
	auto name = testutil::unique_name("cconn");
	testutil::channel_files f{name};
	auto l = goipc::Conn::listen(name, 4096);
	auto d = goipc::Conn::dial(name);
	std::atomic<int> err{0};
	std::thread t([&] {
		std::vector<std::byte> buf(8);
		err = static_cast<int>(errc_of([&] { d.read(buf); }));
	});
	ASSERT_TRUE(testutil::eventually([&] { return d.channel().rx().ring().recv_waiters() == 1; }));
	d.close();
	t.join();
	EXPECT_EQ(err, static_cast<int>(errc::closed));
}

} // namespace
