// Queue: a named multi-producer single-consumer message queue in shared memory.
#ifndef GOIPC_QUEUE_HPP
#define GOIPC_QUEUE_HPP

#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "detail.hpp"
#include "error.hpp"
#include "event.hpp"
#include "ring.hpp"

namespace goipc {

namespace detail {

struct queue_state final : claim_sink {
	std::string name;
	std::byte *map = nullptr;
	std::size_t map_len = 0;
	Ring ring;
	Event ne;
	Event nf;
	gate g;

	~queue_state()
	{
		if (map)
			::munmap(map, map_len);
	}

	std::atomic_ref<std::int32_t> waiters(std::size_t off) noexcept
	{
		return std::atomic_ref<std::int32_t>(*reinterpret_cast<std::int32_t *>(map + off));
	}

	// wake_receiver signals only when a receiver is parked, so an active
	// queue makes no system call.
	void wake_receiver()
	{
		if (waiters(wire::offset::recv_waiters).load() > 0)
			ne.signal(1);
	}

	// wake_senders releases every parked sender. Freed space fits an unknown
	// number of them, so each re-checks its own size.
	void wake_senders()
	{
		std::int32_t w = waiters(wire::offset::send_waiters).load();
		if (w > 0)
			nf.signal(w);
	}

	void publish(std::byte *rec, std::int32_t total, bool abort) override
	{
		if (!g.enter())
			return;
		struct leave_on_exit {
			gate &g;
			~leave_on_exit() { g.leave(); }
		} leave{g};
		if (abort)
			abort_record(rec, total);
		else
			commit_record(rec, total);
		wake_receiver();
	}
};

// park runs attempt until it succeeds, and between attempts waits on ev. It
// returns false when the deadline ends first.
template <class Attempt>
bool park(Event &ev, std::atomic_ref<std::int32_t> waiters, const deadline &d, Attempt &&attempt)
{
	for (;;) {
		if (attempt())
			return true;
		waiters.fetch_add(1);
		struct withdraw {
			std::atomic_ref<std::int32_t> w;
			~withdraw() { w.fetch_sub(1); }
		} undo{waiters};
		if (attempt())
			return true;
		if (!ev.wait_until(d))
			return false;
	}
}

[[noreturn]] inline void throw_timed_out(const std::string &name)
{
	throw error(errc::timed_out, "goipc: queue " + name + ": operation timed out");
}

} // namespace detail

// Queue is a named message queue. Any number of threads in any number of
// processes may send. One thread receives. A side that cannot proceed blocks in
// the kernel. close may run on any thread and releases every blocked call.
class Queue {
public:
	Queue() = default;
	Queue(Queue &&) noexcept = default;
	Queue &operator=(Queue &&o) noexcept
	{
		if (this != &o) {
			close();
			s_ = std::move(o.s_);
		}
		return *this;
	}
	~Queue() { close(); }

	// create makes the queue's files, replacing any stale instance of them.
	static Queue create(std::string_view name, std::size_t capacity = wire::default_capacity)
	{
		detail::validate_name(name);
		if (capacity < wire::min_capacity || !std::has_single_bit(capacity))
			throw error(errc::invalid_capacity, "goipc: invalid capacity " + std::to_string(capacity));
		auto s = std::make_shared<detail::queue_state>();
		s->name = std::string(name);
		try {
			s->ne = Event::create(s->name + std::string(wire::not_empty_suffix));
			s->nf = Event::create(s->name + std::string(wire::not_full_suffix));
			std::string path = detail::segment_path(name);
			int fd = ::open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC, 0600);
			if (fd < 0)
				detail::throw_errno("goipc: open " + path);
			std::size_t size = Ring::size_for(capacity);
			if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
				int err = errno;
				::close(fd);
				detail::throw_errno(err, "goipc: ftruncate " + path);
			}
			map_into(*s, fd, size, path);
			s->ring = Ring::init({s->map, s->map_len});
		} catch (...) {
			s->ne.close();
			s->nf.close();
			remove_files(name);
			throw;
		}
		return Queue(std::move(s));
	}

	// open attaches to a queue that another party created.
	static Queue open(std::string_view name)
	{
		detail::validate_name(name);
		auto s = std::make_shared<detail::queue_state>();
		s->name = std::string(name);
		std::string path = detail::segment_path(name);
		int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
		if (fd < 0)
			detail::throw_errno("goipc: open " + path);
		struct stat st;
		if (::fstat(fd, &st) != 0) {
			int err = errno;
			::close(fd);
			detail::throw_errno(err, "goipc: fstat " + path);
		}
		std::size_t size = static_cast<std::size_t>(st.st_size);
		if (size < wire::header_size + wire::min_capacity) {
			::close(fd);
			throw error(errc::too_small, "goipc: segment " + path + " holds " + std::to_string(size) + " bytes");
		}
		map_into(*s, fd, size, path);
		s->ring = Ring::attach({s->map, s->map_len});
		s->ne = Event::open(s->name + std::string(wire::not_empty_suffix));
		s->nf = Event::open(s->name + std::string(wire::not_full_suffix));
		return Queue(std::move(s));
	}

	// remove deletes the named queue's segment and both event files. Missing
	// files are not an error.
	static void remove(std::string_view name)
	{
		detail::validate_name(name);
		remove_files(name);
	}

	const std::string &name() const { return state().name; }
	std::size_t capacity() const { return state().ring.capacity(); }
	std::size_t max_message_size() const { return state().ring.max_message_size(); }

	// ring has no close guard. Do not use it after close.
	Ring &ring() { return state().ring; }

	// try_send copies payload into the queue. It returns false when full.
	bool try_send(std::uint32_t type, std::span<const std::byte> payload)
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		if (!s.ring.try_write(type, payload))
			return false;
		s.wake_receiver();
		return true;
	}

	// send copies payload into the queue, and waits for room when it is full.
	void send(std::uint32_t type, std::span<const std::byte> payload, timeout t = forever)
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		bool ok = detail::park(s.nf, s.waiters(wire::offset::send_waiters), detail::deadline(t),
				       [&] { return s.ring.try_write(type, payload); });
		if (!ok)
			detail::throw_timed_out(s.name);
		s.wake_receiver();
	}

	// try_claim reserves len payload bytes to fill in place. Committing the
	// claim wakes the receiver. Finish it before the queue closes.
	std::optional<Claim> try_claim(std::uint32_t type, std::size_t len)
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		std::byte *rec;
		std::int32_t total;
		if (!s.ring.claim_slot(type, len, rec, total))
			return std::nullopt;
		return Claim(rec, total, s_);
	}

	Claim claim(std::uint32_t type, std::size_t len, timeout t = forever)
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		std::byte *rec = nullptr;
		std::int32_t total = 0;
		bool ok = detail::park(s.nf, s.waiters(wire::offset::send_waiters), detail::deadline(t),
				       [&] { return s.ring.claim_slot(type, len, rec, total); });
		if (!ok)
			detail::throw_timed_out(s.name);
		return Claim(rec, total, s_);
	}

	// try_recv copies the next message into dst. It returns nullopt when the
	// queue is empty.
	std::optional<received> try_recv(std::span<std::byte> dst)
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		received out{};
		if (!recv_into(dst, out))
			return std::nullopt;
		return out;
	}

	std::optional<message> try_recv()
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		message out{};
		if (!recv_message(out))
			return std::nullopt;
		return out;
	}

	received recv(std::span<std::byte> dst, timeout t = forever)
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		received out{};
		bool ok = detail::park(s.ne, s.waiters(wire::offset::recv_waiters), detail::deadline(t),
				       [&] { return recv_into(dst, out); });
		if (!ok)
			detail::throw_timed_out(s.name);
		return out;
	}

	message recv(timeout t = forever)
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		message out{};
		bool ok = detail::park(s.ne, s.waiters(wire::offset::recv_waiters), detail::deadline(t),
				       [&] { return recv_message(out); });
		if (!ok)
			detail::throw_timed_out(s.name);
		return out;
	}

	// try_read_batch passes up to limit ready messages to fn(type, payload)
	// without blocking. Each payload aliases shared memory and is valid only
	// during the call.
	template <class F>
	std::size_t try_read_batch(std::size_t limit, F &&fn)
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		return receive(limit, deliver_all(fn));
	}

	// read_batch is try_read_batch that waits for at least one message.
	template <class F>
	std::size_t read_batch(std::size_t limit, F &&fn, timeout t = forever)
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		std::size_t n = 0;
		bool ok = detail::park(s.ne, s.waiters(wire::offset::recv_waiters), detail::deadline(t), [&] {
			n = receive(limit, deliver_all(fn));
			return n > 0;
		});
		if (!ok)
			detail::throw_timed_out(s.name);
		return n;
	}

	// close releases this process's handles. Blocked calls raise errc::closed,
	// and close waits for every call in flight before it unmaps the segment.
	// It does not remove the files.
	void close()
	{
		if (!s_ || !s_->g.close())
			return;
		auto &s = *s_;
		std::exception_ptr first;
		auto attempt = [&](auto &&f) {
			try {
				f();
			} catch (...) {
				if (!first)
					first = std::current_exception();
			}
		};
		attempt([&] { s.ne.shutdown(); });
		attempt([&] { s.nf.shutdown(); });
		s.g.drain();
		attempt([&] { s.ne.close(); });
		attempt([&] { s.nf.close(); });
		std::byte *map = std::exchange(s.map, nullptr);
		if (map && ::munmap(map, s.map_len) != 0)
			attempt([&] { detail::throw_errno("goipc: munmap " + s.name); });
		if (first)
			std::rethrow_exception(first);
	}

	// unlink removes the queue's files. Handles already open keep working.
	void unlink() { remove_files(state().name); }

private:
	explicit Queue(std::shared_ptr<detail::queue_state> s) : s_(std::move(s)) {}

	detail::queue_state &state() const
	{
		if (!s_)
			throw error(errc::closed, "goipc: queue has no state");
		return *s_;
	}

	static void map_into(detail::queue_state &s, int fd, std::size_t size, const std::string &path)
	{
		void *p = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
		int err = errno;
		::close(fd);
		if (p == MAP_FAILED)
			detail::throw_errno(err, "goipc: mmap " + path);
		s.map = static_cast<std::byte *>(p);
		s.map_len = size;
	}

	static void remove_files(std::string_view name)
	{
		std::string path = detail::segment_path(name);
		int err = 0;
		if (::unlink(path.c_str()) != 0 && errno != ENOENT)
			err = errno;
		std::string n(name);
		Event::remove(n + std::string(wire::not_empty_suffix));
		Event::remove(n + std::string(wire::not_full_suffix));
		if (err)
			detail::throw_errno(err, "goipc: unlink " + path);
	}

	template <class F>
	static auto deliver_all(F &fn)
	{
		return [&fn](std::uint32_t t, std::span<const std::byte> p) {
			fn(t, p);
			return true;
		};
	}

	// receive runs one read, and wakes the senders when it moved head. A
	// read that only stepped over padding still frees space.
	template <class F>
	std::size_t receive(std::size_t limit, F &&fn)
	{
		auto &s = *s_;
		std::uint64_t before = s.ring.head();
		std::size_t n;
		try {
			n = s.ring.read_accept(limit, fn);
		} catch (...) {
			if (s.ring.head() != before)
				s.wake_senders();
			throw;
		}
		if (s.ring.head() != before)
			s.wake_senders();
		return n;
	}

	bool recv_into(std::span<std::byte> dst, received &out)
	{
		std::size_t needed = 0;
		std::size_t n = receive(1, [&](std::uint32_t t, std::span<const std::byte> p) {
			if (p.size() > dst.size()) {
				needed = p.size();
				return false;
			}
			if (!p.empty())
				std::memcpy(dst.data(), p.data(), p.size());
			out = received{t, p.size()};
			return true;
		});
		if (needed)
			throw error(errc::buffer,
				    "goipc: message of " + std::to_string(needed) + " bytes does not fit a buffer of " +
					    std::to_string(dst.size()),
				    needed);
		return n > 0;
	}

	bool recv_message(message &out)
	{
		return receive(1, [&](std::uint32_t t, std::span<const std::byte> p) {
			       out.type = t;
			       out.payload.assign(p.begin(), p.end());
			       return true;
		       }) > 0;
	}

	std::shared_ptr<detail::queue_state> s_;
};

} // namespace goipc

#endif
