#include <chrono>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <goipc/goipc.hpp>

#include "helpers.hpp"

namespace {

using namespace std::chrono_literals;
using goipc::errc;
using goipc::Queue;

// A child parks one receiver on an empty queue and several senders on a full one for a fixed window.
TEST(Blocking, ParkedEndpointsConsumeNoCPU)
{
	constexpr auto window = 1000ms;
	constexpr auto budget = window / 10;
	constexpr int senders = 4;

	auto idle_name = testutil::unique_name("idle");
	auto full_name = testutil::unique_name("full");
	testutil::queue_files f1{idle_name}, f2{full_name};
	auto full = Queue::create(full_name, 4096);
	std::vector<std::byte> payload(504);
	while (full.try_send(0, payload)) {
	}

	testutil::pipe_pair ready;
	pid_t pid = testutil::fork_child([&] {
		// Only the creating handle receives, so the child owns the idle queue.
		auto rx = Queue::create(idle_name, 4096);
		auto tx = Queue::open(full_name);
		ready.signal();
		std::vector<std::thread> threads;
		std::atomic<int> timed_out{0};
		threads.emplace_back([&] {
			try {
				rx.recv(window);
			} catch (const goipc::error &e) {
				if (e.value() == errc::timed_out)
					timed_out++;
			}
		});
		for (int i = 0; i < senders; i++) {
			threads.emplace_back([&] {
				try {
					tx.send(0, payload, window);
				} catch (const goipc::error &e) {
					if (e.value() == errc::timed_out)
						timed_out++;
				}
			});
		}
		for (auto &t : threads)
			t.join();
		return timed_out == senders + 1 ? 0 : 1;
	});

	ASSERT_TRUE(ready.wait());
	auto idle = Queue::open(idle_name);
	ASSERT_TRUE(testutil::eventually([&] {
		return idle.ring().recv_waiters() == 1 && full.ring().send_waiters() == senders;
	}));
	auto res = testutil::wait_child(pid);
	EXPECT_EQ(testutil::exit_code(res.status), 0) << "every parked call must end in a timeout";
	EXPECT_LT(res.cpu, budget) << "parked endpoints burned " << res.cpu.count() << "us of CPU across a "
				   << std::chrono::duration_cast<std::chrono::milliseconds>(window).count()
				   << "ms window: something is spinning";
}

} // namespace
