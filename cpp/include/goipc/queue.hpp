// Queue: a named multi-producer single-consumer message queue in shared memory.
#ifndef GOIPC_QUEUE_HPP
#define GOIPC_QUEUE_HPP

#include <atomic>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "detail.hpp"
#include "error.hpp"
#include "event.hpp"
#include "identity.hpp"
#include "names.hpp"
#include "ring.hpp"

namespace goipc {

class Channel;

namespace detail {

[[noreturn]] inline void throw_peer_gone(const std::string &name)
{
	throw error(errc::peer_gone, "goipc: queue " + name + ": peer is gone");
}

struct queue_state final : claim_sink {
	std::string name;
	std::string inc;
	std::byte *map = nullptr;
	std::size_t map_len = 0;
	Ring ring;
	Event ne;
	Event nf;
	gate g;
	std::uint64_t self = 0;
	// lock holds the name. Only the creator has one.
	name_lock lock;
	// reader reports whether this handle owns the receiving end.
	bool reader = false;
	std::mutex recv_mu;

	std::mutex slot_mu;
	std::vector<int> idle;
	std::vector<int> owned;

	// The watch on the receiver of this queue, for a handle that sends.
	std::mutex peer_mu;
	std::atomic<std::uint64_t> peer_id{0};
	std::atomic<int> peer_fd{-1};
	std::atomic<bool> peer_gone{false};
	int peer_err = 0;
	// retired holds watch descriptors of earlier receivers.
	std::vector<int> retired;

	// producers maps each producer whose claim stops the reader to a watch on its process.
	std::map<std::uint64_t, int> producers;
	int producer_err = 0;

	// peer_queue is the outbound queue of a channel, set on its inbound queue.
	std::shared_ptr<queue_state> peer_queue;

	~queue_state()
	{
		if (map)
			::munmap(map, map_len);
		close_watches();
	}

	void close_watches() noexcept
	{
		if (int fd = peer_fd.exchange(-1); fd >= 0)
			::close(fd);
		for (int fd : retired)
			::close(fd);
		retired.clear();
		for (auto &p : producers)
			::close(p.second);
		producers.clear();
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

	void publish(std::byte *rec, std::int32_t total, bool abort, int slot) override
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
		ring.release(slot);
		put_slot(slot);
		wake_receiver();
	}

	// check_peer raises errc::peer_gone once the receiver of the queue has
	// closed or exited. The first check of a new receiver opens a watch on its
	// process. Every later check is an atomic load.
	void check_peer()
	{
		std::uint64_t id = ring.consumer();
		if (id == wire::consumer_none)
			throw_peer_gone(name);
		if (id == wire::consumer_pending || id == self)
			return;
		// A receiver with no life socket cannot be watched. Its close still
		// reaches this sender through the consumer field.
		if (!watchable(id))
			return;
		if (peer_id.load() != id)
			watch_peer(id);
		peer_state();
	}

	void peer_state()
	{
		if (!peer_gone.load())
			return;
		std::lock_guard l(peer_mu);
		if (peer_err)
			throw_errno(peer_err, "goipc: queue " + name + ": cannot watch the receiver");
		throw_peer_gone(name);
	}

	void watch_peer(std::uint64_t id)
	{
		std::lock_guard l(peer_mu);
		if (peer_id.load() == id)
			return;
		if (int old = peer_fd.exchange(-1); old >= 0)
			retired.push_back(old);
		peer_err = 0;
		peer_id.store(0);
		// A receiver that is already dead is reported now.
		int fd = open_watch(id);
		peer_gone.store(fd < 0);
		peer_fd.store(fd);
		peer_id.store(id);
	}

	// service_peer reads the receiver watch after a parked wait saw it turn
	// readable.
	void service_peer()
	{
		int fd = peer_fd.load();
		if (fd < 0)
			return;
		int err = 0;
		switch (poll_watch(fd, peer_id.load(), err)) {
		case watch_state::alive:
			return;
		case watch_state::exited:
			peer_gone.store(true);
			return;
		case watch_state::failed: {
			std::lock_guard l(peer_mu);
			peer_err = err;
			peer_gone.store(true);
			return;
		}
		}
	}

	std::vector<int> sender_watches() const
	{
		int fd = peer_fd.load();
		if (fd < 0)
			return {};
		return {fd};
	}

	// receiver_watches holds the producers whose claims stop the reader, and
	// the far side of a channel.
	std::vector<int> receiver_watches() const
	{
		std::vector<int> fds;
		for (const auto &p : producers)
			fds.push_back(p.second);
		if (peer_queue) {
			int fd = peer_queue->peer_fd.load();
			if (fd >= 0)
				fds.push_back(fd);
		}
		return fds;
	}

	void service_receiver()
	{
		for (auto it = producers.begin(); it != producers.end();) {
			int err = 0;
			watch_state ws = poll_watch(it->second, it->first, err);
			if (ws == watch_state::alive) {
				++it;
				continue;
			}
			if (ws == watch_state::failed && producer_err == 0)
				producer_err = err;
			::close(it->second);
			it = producers.erase(it);
		}
		if (peer_queue && peer_queue->g.enter()) {
			peer_queue->service_peer();
			peer_queue->g.leave();
		}
	}

	void watch_producer(std::uint64_t id)
	{
		if (producer_err)
			throw_errno(producer_err, "goipc: queue " + name + ": cannot watch a producer");
		if (producers.count(id))
			return;
		int fd = open_watch(id);
		if (fd < 0) {
			// The producer exited since the check. A token makes the parked wait run the recovery again.
			ne.signal(1);
			return;
		}
		producers.emplace(id, fd);
	}

	// unstall looks at the claim that stops the receiver, if one does. It
	// returns true after it turns the claim of a dead producer into padding. A
	// claim whose producer lives gets a watch on that producer instead.
	bool unstall()
	{
		std::optional<stall> st = ring.stalled();
		if (!st)
			return false;
		std::vector<int> dead;
		std::uint64_t end = 0;
		for (int idx : st->slots) {
			std::uint64_t owner = ring.slot_owner(idx).load();
			std::uint64_t at = ring.slot_at(idx).load();
			std::uint64_t size = ring.slot_size(idx).load();
			// The slot may have changed since stalled read it. Only a slot
			// that still covers the stall counts.
			if (owner == wire::consumer_none || at == wire::no_intent || at > st->at || st->at >= at + size)
				continue;
			// A claim of this process, or of a process with no life socket, is left alone.
			if (owner == self || !watchable(owner))
				return false;
			if (!is_dead(owner)) {
				watch_producer(owner);
				return false;
			}
			// Dead producers that claim different ranges here leave no way to
			// tell which claim is real.
			if (!dead.empty() && at + size != end)
				throw error(errc::corrupt, "goipc: queue " + name + ": dead producers disagree on a claim");
			end = at + size;
			dead.push_back(idx);
		}
		if (dead.empty())
			return false;
		return ring.reclaim(*st, end, dead);
	}

	// take_slot hands out a claim slot this handle owns, and takes a new one
	// from the ring when none is idle.
	int take_slot()
	{
		{
			std::lock_guard l(slot_mu);
			if (!idle.empty()) {
				int slot = idle.back();
				idle.pop_back();
				return slot;
			}
		}
		std::map<std::uint64_t, bool> known;
		int slot = ring.find_slot(self, [&](std::uint64_t id) {
			auto it = known.find(id);
			if (it == known.end())
				it = known.emplace(id, is_dead(id)).first;
			return it->second;
		});
		if (slot < 0)
			throw error(errc::too_many_claims, "goipc: queue " + name + ": too many claims in progress");
		std::lock_guard l(slot_mu);
		owned.push_back(slot);
		return slot;
	}

	void put_slot(int slot)
	{
		if (slot < 0)
			return;
		std::lock_guard l(slot_mu);
		idle.push_back(slot);
	}

	// write is a single attempt to copy payload into the ring under a claim
	// slot of this process.
	bool write(std::uint32_t type, std::span<const std::byte> payload)
	{
		check_peer();
		int slot = take_slot();
		struct give_back {
			queue_state &s;
			int slot;
			~give_back() { s.put_slot(slot); }
		} back{*this, slot};
		return ring.write_record(slot, type, payload);
	}

	// claim is a single attempt to reserve len bytes. The slot stays with the
	// claim until its commit or abort.
	bool claim(std::uint32_t type, std::size_t len, std::byte *&rec, std::int32_t &total, int &slot)
	{
		check_peer();
		slot = take_slot();
		bool ok = false;
		try {
			ok = ring.claim_record(slot, type, len, rec, total);
		} catch (...) {
			put_slot(slot);
			throw;
		}
		if (!ok)
			put_slot(slot);
		return ok;
	}
};

// park runs attempt until it succeeds, and between attempts waits on ev. The
// wait also ends when a descriptor from watches turns readable; service then
// reads it. A timeout raises errc::timed_out.
template <class Attempt, class Watches, class Service>
void park(Event &ev, std::atomic_ref<std::int32_t> waiters, const deadline &d, const std::string &name, Attempt &&attempt,
	  Watches &&watches, Service &&service)
{
	for (;;) {
		if (attempt())
			return;
		waiters.fetch_add(1);
		struct withdraw {
			std::atomic_ref<std::int32_t> w;
			~withdraw() { w.fetch_sub(1); }
		} undo{waiters};
		if (attempt())
			return;
		std::vector<int> fds = watches();
		switch (ev.wait_until(d, fds)) {
		case Event::wake::token:
			break;
		case Event::wake::timed_out:
			throw error(errc::timed_out, "goipc: queue " + name + ": operation timed out");
		case Event::wake::watch:
			service();
			break;
		}
	}
}

} // namespace detail

// Queue is a named message queue. Any number of threads in any number of
// processes may send. Only the handle create returns may receive, one call at
// a time. A side that cannot proceed blocks in the kernel. close may run on
// any thread and releases every blocked call.
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

	// create makes a new instance of the named queue and returns its receiving
	// end. It raises errc::in_use while a live process holds the name.
	static Queue create(std::string_view name, std::size_t capacity = wire::default_capacity)
	{
		return create_queue(name, capacity, false);
	}

	// open attaches to a queue another process created, as a sender. It
	// raises errc::peer_gone when the receiver has already exited or closed.
	static Queue open(std::string_view name)
	{
		Queue q = open_queue(name);
		try {
			q.s_->check_peer();
		} catch (...) {
			q.close();
			throw;
		}
		return q;
	}

	// remove deletes the named queue's instance and name files, whoever holds
	// them. Missing files are not an error.
	static void remove(std::string_view name)
	{
		detail::validate_name(name);
		std::string inc = detail::read_incarnation(name);
		if (!inc.empty())
			detail::remove_instance(name, inc);
		detail::remove_name(name);
	}

	const std::string &name() const { return state().name; }
	const std::string &incarnation() const { return state().inc; }
	std::size_t capacity() const { return state().ring.capacity(); }
	std::size_t max_message_size() const { return state().ring.max_message_size(); }
	bool is_receiver() const { return state().reader; }

	// ring has no close guard. Do not use it after close.
	Ring &ring() { return state().ring; }

	// try_send copies payload into the queue. It returns false when full.
	bool try_send(std::uint32_t type, std::span<const std::byte> payload)
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		if (!s.write(type, payload))
			return false;
		s.wake_receiver();
		return true;
	}

	// send copies payload into the queue, and waits for room when it is full.
	void send(std::uint32_t type, std::span<const std::byte> payload, timeout t = forever)
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		park_sender(s, detail::deadline(t), [&] { return s.write(type, payload); });
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
		int slot;
		if (!s.claim(type, len, rec, total, slot))
			return std::nullopt;
		return Claim(rec, total, slot, s_);
	}

	Claim claim(std::uint32_t type, std::size_t len, timeout t = forever)
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		std::byte *rec = nullptr;
		std::int32_t total = 0;
		int slot = -1;
		park_sender(s, detail::deadline(t), [&] { return s.claim(type, len, rec, total, slot); });
		return Claim(rec, total, slot, s_);
	}

	// try_recv copies the next message into dst. It returns nullopt when the
	// queue is empty.
	std::optional<received> try_recv(std::span<std::byte> dst)
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		auto l = receiving(s);
		received out{};
		if (!recv_into(dst, out))
			return std::nullopt;
		return out;
	}

	std::optional<message> try_recv()
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		auto l = receiving(s);
		message out{};
		if (!recv_message(out))
			return std::nullopt;
		return out;
	}

	received recv(std::span<std::byte> dst, timeout t = forever)
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		auto l = receiving(s);
		received out{};
		park_receiver(s, detail::deadline(t), [&] { return recv_into(dst, out); });
		return out;
	}

	message recv(timeout t = forever)
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		auto l = receiving(s);
		message out{};
		park_receiver(s, detail::deadline(t), [&] { return recv_message(out); });
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
		auto l = receiving(s);
		return receive(limit, deliver_all(fn));
	}

	// read_batch is try_read_batch that waits for at least one message.
	template <class F>
	std::size_t read_batch(std::size_t limit, F &&fn, timeout t = forever)
	{
		auto &s = state();
		detail::gate_guard in(s.g);
		auto l = receiving(s);
		std::size_t n = 0;
		park_receiver(s, detail::deadline(t), [&] {
			n = receive(limit, deliver_all(fn));
			return n > 0;
		});
		return n;
	}

	// close releases this process's handles. Blocked calls raise errc::closed,
	// and close waits for every call in flight before it unmaps the segment.
	// The receiving end clears the consumer first, so a sender parked in any
	// process wakes and finds nobody left to drain the queue. close does not
	// remove the files; see unlink.
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
		if (s.reader && s.map) {
			std::uint64_t self = s.self;
			std::atomic_ref<std::uint64_t>(*reinterpret_cast<std::uint64_t *>(s.map + wire::offset::consumer))
				.compare_exchange_strong(self, wire::consumer_none);
			attempt([&] { s.wake_senders(); });
		}
		attempt([&] { s.ne.shutdown(); });
		attempt([&] { s.nf.shutdown(); });
		s.g.drain();
		attempt([&] { s.ne.close(); });
		attempt([&] { s.nf.close(); });
		if (s.map) {
			std::lock_guard l(s.slot_mu);
			for (int slot : s.owned)
				s.ring.drop_slot(s.self, slot);
			s.owned.clear();
			s.idle.clear();
		}
		std::byte *map = std::exchange(s.map, nullptr);
		if (map && ::munmap(map, s.map_len) != 0)
			attempt([&] { detail::throw_errno("goipc: munmap " + s.name); });
		s.close_watches();
		s.lock.release();
		if (first)
			std::rethrow_exception(first);
	}

	// unlink removes the queue's name so no further process can open it.
	// Handles already open keep working.
	void unlink()
	{
		auto &s = state();
		detail::remove_instance(s.name, s.inc);
		detail::unlink_name(s.name, s.inc);
	}

private:
	friend class Channel;

	explicit Queue(std::shared_ptr<detail::queue_state> s) : s_(std::move(s)) {}

	detail::queue_state &state() const
	{
		if (!s_)
			throw error(errc::closed, "goipc: queue has no state");
		return *s_;
	}

	// create_queue makes a new instance of the named queue. This process
	// reads it, unless pending leaves the reader to a channel peer that
	// connects later.
	static Queue create_queue(std::string_view name, std::size_t capacity, bool pending)
	{
		detail::validate_name(name);
		if (capacity < wire::min_capacity || !std::has_single_bit(capacity))
			throw error(errc::invalid_capacity, "goipc: invalid capacity " + std::to_string(capacity));
		detail::sweep_stale();
		auto s = std::make_shared<detail::queue_state>();
		s->name = std::string(name);
		s->self = detail::self_id();
		s->lock = detail::name_lock::take(name);
		s->reader = !pending;
		try {
			build(*s, capacity, pending ? wire::consumer_pending : s->self);
		} catch (...) {
			unwind(*s);
			throw;
		}
		return Queue(std::move(s));
	}

	// build makes the instance while this handle holds the name. The name
	// points at the instance only after the instance is complete. A stale
	// instance is removed, never reused, because its senders may still hold it.
	static void build(detail::queue_state &s, std::size_t capacity, std::uint64_t consumer)
	{
		std::string old = s.lock.previous();
		if (!old.empty())
			detail::remove_instance(s.name, old);
		s.inc = detail::new_incarnation();
		std::string inst = detail::instance_name(s.name, s.inc);
		s.ne = Event::create(inst + std::string(wire::not_empty_suffix));
		s.nf = Event::create(inst + std::string(wire::not_full_suffix));
		std::string path = detail::segment_path(inst);
		int fd = ::open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC, 0600);
		if (fd < 0)
			detail::throw_errno("goipc: open " + path);
		std::size_t size = Ring::size_for(capacity);
		if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
			int err = errno;
			::close(fd);
			detail::throw_errno(err, "goipc: ftruncate " + path);
		}
		map_into(s, fd, size, path);
		s.ring = Ring::init({s.map, s.map_len}, consumer);
		s.lock.publish(s.inc);
	}

	// unwind releases whatever a failed constructor managed to acquire.
	static void unwind(detail::queue_state &s) noexcept
	{
		try {
			s.ne.close();
		} catch (...) {
		}
		try {
			s.nf.close();
		} catch (...) {
		}
		if (s.map) {
			::munmap(s.map, s.map_len);
			s.map = nullptr;
		}
		if (s.lock && !s.inc.empty()) {
			try {
				detail::remove_instance(s.name, s.inc);
			} catch (...) {
			}
		}
		s.lock.release();
	}

	static Queue open_queue(std::string_view name)
	{
		detail::validate_name(name);
		auto s = std::make_shared<detail::queue_state>();
		s->name = std::string(name);
		s->self = detail::self_id();
		s->inc = detail::read_name(name);
		try {
			attach(*s);
		} catch (...) {
			unwind(*s);
			throw;
		}
		return Queue(std::move(s));
	}

	static void attach(detail::queue_state &s)
	{
		std::string inst = detail::instance_name(s.name, s.inc);
		std::string path = detail::segment_path(inst);
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
		map_into(s, fd, size, path);
		s.ring = Ring::attach({s.map, s.map_len});
		s.ne = Event::open(inst + std::string(wire::not_empty_suffix));
		s.nf = Event::open(inst + std::string(wire::not_full_suffix));
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

	static std::unique_lock<std::mutex> receiving(detail::queue_state &s)
	{
		if (!s.reader)
			throw error(errc::not_consumer, "goipc: queue " + s.name + ": handle is not the receiving end");
		return std::unique_lock<std::mutex>(s.recv_mu);
	}

	// unless_closing runs attempt only while the handle is open. Close clears
	// the consumer before it shuts the events, so a woken sender must see the
	// closing mark before it can see peer-gone.
	template <class Attempt>
	static auto unless_closing(detail::queue_state &s, Attempt &attempt)
	{
		return [&s, &attempt] {
			if (s.g.closing())
				throw error(errc::closed, "goipc: queue " + s.name + " is closed");
			return attempt();
		};
	}

	template <class Attempt>
	static void park_sender(detail::queue_state &s, const detail::deadline &d, Attempt &&attempt)
	{
		detail::park(s.nf, s.waiters(wire::offset::send_waiters), d, s.name, unless_closing(s, attempt),
			     [&] { return s.sender_watches(); }, [&] { s.service_peer(); });
	}

	template <class Attempt>
	static void park_receiver(detail::queue_state &s, const detail::deadline &d, Attempt &&attempt)
	{
		detail::park(s.ne, s.waiters(wire::offset::recv_waiters), d, s.name, unless_closing(s, attempt),
			     [&] { return s.receiver_watches(); }, [&] { s.service_receiver(); });
	}

	template <class F>
	static auto deliver_all(F &fn)
	{
		return [&fn](std::uint32_t t, std::span<const std::byte> p) {
			fn(t, p);
			return true;
		};
	}

	// check_far_side reports why the far side of a channel is gone, or nothing.
	// Its result counts only when the read after it finds nothing, so a peer
	// that sent a message and then exited has its message. That message is
	// delivered first.
	std::exception_ptr check_far_side()
	{
		auto &pq = s_->peer_queue;
		if (!pq)
			return nullptr;
		if (!pq->g.enter())
			return std::make_exception_ptr(error(errc::closed, "goipc: queue " + pq->name + " is closed"));
		std::exception_ptr gone;
		try {
			pq->check_peer();
		} catch (...) {
			gone = std::current_exception();
		}
		pq->g.leave();
		return gone;
	}

	// receive runs one read, and wakes the senders when it moved head. A read
	// that only stepped over padding still frees space. A read that finds a
	// claim in its way checks the claim's producer. A dead producer's claim
	// becomes padding, and the read runs again.
	template <class F>
	std::size_t receive(std::size_t limit, F &&fn)
	{
		auto &s = *s_;
		std::exception_ptr gone = check_far_side();
		bool declined = false;
		auto accept = [&](std::uint32_t t, std::span<const std::byte> p) {
			bool ok = fn(t, p);
			declined = declined || !ok;
			return ok;
		};
		for (;;) {
			std::uint64_t before = s.ring.head();
			std::size_t n;
			try {
				n = s.ring.read_accept(limit, accept);
			} catch (...) {
				if (s.ring.head() != before)
					s.wake_senders();
				throw;
			}
			if (s.ring.head() != before)
				s.wake_senders();
			if (n > 0 || declined)
				return n;
			if (!s.unstall())
				break;
		}
		if (gone)
			std::rethrow_exception(gone);
		return 0;
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
