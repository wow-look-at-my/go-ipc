// Ring: a lock-free multi-producer single-consumer ring of records inside a
// caller buffer, laid out as spec/README.md describes.
#ifndef GOIPC_RING_HPP
#define GOIPC_RING_HPP

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "detail.hpp"
#include "error.hpp"

namespace goipc {

// received describes a message copied into a caller buffer.
struct received {
	std::uint32_t type;
	std::size_t size;
};

// message is a received message with a payload of its own.
struct message {
	std::uint32_t type;
	std::vector<std::byte> payload;
};

namespace detail {

// claim_sink publishes a claim that a queue handed out, so that the commit
// also wakes the receiver.
class claim_sink {
public:
	virtual void publish(std::byte *rec, std::int32_t total, bool abort) = 0;

protected:
	~claim_sink() = default;
};

inline std::uint64_t align8(std::uint64_t n) noexcept { return (n + 7) & ~std::uint64_t(7); }

inline std::atomic_ref<std::int32_t> length_at(std::byte *rec) noexcept
{
	return std::atomic_ref<std::int32_t>(*reinterpret_cast<std::int32_t *>(rec));
}

inline void store_type(std::byte *rec, std::uint32_t type) noexcept { std::memcpy(rec + 4, &type, 4); }

inline std::uint32_t load_type(const std::byte *rec) noexcept
{
	std::uint32_t t;
	std::memcpy(&t, rec + 4, 4);
	return t;
}

inline void commit_record(std::byte *rec, std::int32_t total) noexcept { length_at(rec).store(total); }

inline void abort_record(std::byte *rec, std::int32_t total) noexcept
{
	store_type(rec, wire::type_padding);
	length_at(rec).store(total);
}

} // namespace detail

// Claim is a reserved record that a producer fills in place. It must be
// committed or aborted. The destructor aborts a claim that is still open.
class Claim {
public:
	Claim() = default;
	Claim(Claim &&o) noexcept
		: rec_(std::exchange(o.rec_, nullptr)), total_(o.total_), sink_(std::move(o.sink_)) {}
	Claim &operator=(Claim &&o) noexcept
	{
		if (this != &o) {
			if (rec_)
				abort();
			rec_ = std::exchange(o.rec_, nullptr);
			total_ = o.total_;
			sink_ = std::move(o.sink_);
		}
		return *this;
	}
	Claim(const Claim &) = delete;
	Claim &operator=(const Claim &) = delete;
	~Claim()
	{
		if (rec_)
			abort();
	}

	// bytes is the payload region. Writes become visible to the reader on commit.
	std::span<std::byte> bytes() const noexcept
	{
		if (!rec_)
			return {};
		return {rec_ + wire::record_header_size, static_cast<std::size_t>(total_) - wire::record_header_size};
	}

	explicit operator bool() const noexcept { return rec_ != nullptr; }

	void commit() { finish(false); }

	// abort turns the region into padding, which the reader skips and reclaims.
	void abort() { finish(true); }

private:
	friend class Ring;
	friend class Queue;

	Claim(std::byte *rec, std::int32_t total, std::shared_ptr<detail::claim_sink> sink)
		: rec_(rec), total_(total), sink_(std::move(sink)) {}

	void finish(bool abort)
	{
		if (!rec_)
			throw error(errc::invalid, "goipc: claim is already finished");
		std::byte *rec = std::exchange(rec_, nullptr);
		auto sink = std::move(sink_);
		if (sink) {
			sink->publish(rec, total_, abort);
		} else if (abort) {
			detail::abort_record(rec, total_);
		} else {
			detail::commit_record(rec, total_);
		}
	}

	std::byte *rec_ = nullptr;
	std::int32_t total_ = 0;
	std::shared_ptr<detail::claim_sink> sink_;
};

// Ring is a view of a ring in a caller buffer. Any number of threads in any
// number of processes may write. Exactly one thread may read.
class Ring {
public:
	Ring() = default;
	Ring(Ring &&o) noexcept
		: base_(std::exchange(o.base_, nullptr)), data_(std::exchange(o.data_, nullptr)), mask_(std::exchange(o.mask_, 0)) {}
	Ring &operator=(Ring &&o) noexcept
	{
		base_ = std::exchange(o.base_, nullptr);
		data_ = std::exchange(o.data_, nullptr);
		mask_ = std::exchange(o.mask_, 0);
		return *this;
	}
	Ring(const Ring &) = delete;
	Ring &operator=(const Ring &) = delete;

	static constexpr std::size_t size_for(std::size_t capacity) noexcept { return wire::header_size + capacity; }

	// init formats buf as an empty ring. Every byte of buf is overwritten.
	static Ring init(std::span<std::byte> buf)
	{
		check_buffer(buf);
		std::uint64_t capacity = std::bit_floor(static_cast<std::uint64_t>(buf.size() - wire::header_size));
		std::memset(buf.data(), 0, buf.size());
		std::byte *base = buf.data();
		std::memcpy(base + wire::offset::capacity, &capacity, 8);
		std::memcpy(base + wire::offset::version, &wire::ring_version, 4);
		u64(base, wire::offset::magic).store(wire::ring_magic);
		return Ring(base, capacity);
	}

	// attach returns a view of a ring that another party formatted. It never
	// writes to buf.
	static Ring attach(std::span<std::byte> buf)
	{
		check_buffer(buf);
		std::byte *base = buf.data();
		std::uint32_t version;
		std::uint64_t capacity;
		bool magic_ok = u64(base, wire::offset::magic).load() == wire::ring_magic;
		std::memcpy(&version, base + wire::offset::version, 4);
		std::memcpy(&capacity, base + wire::offset::capacity, 8);
		if (!magic_ok || version != wire::ring_version)
			throw error(errc::bad_layout, "goipc: buffer does not hold a compatible ring");
		if (capacity < wire::min_capacity || !std::has_single_bit(capacity) || buf.size() - wire::header_size < capacity)
			throw error(errc::bad_layout, "goipc: ring capacity does not fit the buffer");
		return Ring(base, capacity);
	}

	std::size_t capacity() const noexcept { return static_cast<std::size_t>(mask_ + 1); }

	std::size_t max_message_size() const noexcept
	{
		std::size_t limit = capacity() / 2 - wire::record_header_size;
		std::size_t cap = static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max()) - wire::record_header_size;
		return limit < cap ? limit : cap;
	}

	std::uint64_t head() const noexcept { return u64(base_, wire::offset::head).load(); }
	std::uint64_t tail() const noexcept { return u64(base_, wire::offset::tail).load(); }
	std::int32_t recv_waiters() const noexcept { return i32(wire::offset::recv_waiters).load(); }
	std::int32_t send_waiters() const noexcept { return i32(wire::offset::send_waiters).load(); }

	// buffered is a sample of the bytes claimed but not yet consumed.
	std::size_t buffered() const noexcept
	{
		std::uint64_t h = head();
		return static_cast<std::size_t>(tail() - h);
	}

	bool empty() const noexcept
	{
		std::uint64_t h = head();
		return h == tail();
	}

	// try_claim reserves a record of len payload bytes. It returns nullopt when
	// the ring is full.
	std::optional<Claim> try_claim(std::uint32_t type, std::size_t len)
	{
		std::byte *rec;
		std::int32_t total;
		if (!claim_slot(type, len, rec, total))
			return std::nullopt;
		return Claim(rec, total, nullptr);
	}

	// try_write copies payload into one record. It returns false when full.
	bool try_write(std::uint32_t type, std::span<const std::byte> payload)
	{
		std::byte *rec;
		std::int32_t total;
		if (!claim_slot(type, payload.size(), rec, total))
			return false;
		if (!payload.empty())
			std::memcpy(rec + wire::record_header_size, payload.data(), payload.size());
		detail::commit_record(rec, total);
		return true;
	}

	// read passes up to limit committed records to fn(type, payload) and returns
	// the count. The payload aliases the ring and is valid only during the call.
	template <class F>
	std::size_t read(std::size_t limit, F &&fn)
	{
		return read_accept(limit, [&](std::uint32_t t, std::span<const std::byte> p) {
			fn(t, p);
			return true;
		});
	}

	// try_recv copies the next message into dst. It returns nullopt when no
	// message is ready. A dst smaller than the message raises errc::buffer and
	// leaves the message queued.
	std::optional<received> try_recv(std::span<std::byte> dst)
	{
		std::optional<received> out;
		std::size_t needed = 0;
		read_accept(1, [&](std::uint32_t t, std::span<const std::byte> p) {
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
			throw_buffer(needed, dst.size());
		return out;
	}

	std::optional<message> try_recv()
	{
		std::optional<message> out;
		read(1, [&](std::uint32_t t, std::span<const std::byte> p) {
			out = message{t, std::vector<std::byte>(p.begin(), p.end())};
		});
		return out;
	}

	// read_accept is read with a callback that may decline a record by
	// returning false. A declined record stays queued and ends the read.
	template <class F>
	std::size_t read_accept(std::size_t limit, F &&fn)
	{
		if (limit == 0)
			return 0;
		auto head_ref = u64(base_, wire::offset::head);
		std::uint64_t head = head_ref.load();
		std::uint64_t available = u64(base_, wire::offset::tail).load() - head;
		std::uint64_t consumed = 0;
		std::size_t count = 0;
		struct publish_head {
			std::atomic_ref<std::uint64_t> &ref;
			std::uint64_t head;
			std::uint64_t &consumed;
			~publish_head()
			{
				if (consumed)
					ref.store(head + consumed);
			}
		} publish{head_ref, head, consumed};

		while (count < limit && consumed < available) {
			std::byte *rec = data_ + ((head + consumed) & mask_);
			std::int32_t len = detail::length_at(rec).load();
			if (len <= 0)
				break;
			std::uint64_t step = detail::align8(static_cast<std::uint64_t>(len));
			if (static_cast<std::size_t>(len) < wire::record_header_size || step > available - consumed)
				throw error(errc::corrupt, "goipc: record length " + std::to_string(len) + " is corrupt");
			std::uint32_t type = detail::load_type(rec);
			if (type != wire::type_padding) {
				std::span<const std::byte> payload(rec + wire::record_header_size,
								   static_cast<std::size_t>(len) - wire::record_header_size);
				if (!fn(type, payload))
					break;
				count++;
			}
			// Zeroing before head moves keeps the slot unreadable on the next lap until its new producer commits.
			detail::length_at(rec).store(0);
			consumed += step;
		}
		return count;
	}

private:
	friend class Queue;

	Ring(std::byte *base, std::uint64_t capacity)
		: base_(base), data_(base + wire::header_size), mask_(capacity - 1) {}

	static std::atomic_ref<std::uint64_t> u64(std::byte *base, std::size_t off) noexcept
	{
		return std::atomic_ref<std::uint64_t>(*reinterpret_cast<std::uint64_t *>(base + off));
	}

	std::atomic_ref<std::int32_t> i32(std::size_t off) const noexcept
	{
		return std::atomic_ref<std::int32_t>(*reinterpret_cast<std::int32_t *>(base_ + off));
	}

	static void check_buffer(std::span<std::byte> buf)
	{
		if (buf.size() < wire::header_size + wire::min_capacity)
			throw error(errc::too_small, "goipc: buffer of " + std::to_string(buf.size()) + " bytes cannot hold a ring");
		if (reinterpret_cast<std::uintptr_t>(buf.data()) % 8 != 0)
			throw error(errc::unaligned, "goipc: buffer is not 8-byte aligned");
	}

	[[noreturn]] static void throw_buffer(std::size_t needed, std::size_t have)
	{
		throw error(errc::buffer,
			    "goipc: message of " + std::to_string(needed) + " bytes does not fit a buffer of " + std::to_string(have),
			    needed);
	}

	// claim_slot runs the claim algorithm. It returns false when the ring is
	// full, and leaves the record header holding -total and the type.
	bool claim_slot(std::uint32_t type, std::size_t len, std::byte *&rec, std::int32_t &total)
	{
		if (type == wire::type_padding)
			throw error(errc::reserved_type, "goipc: message type 0xFFFFFFFF is reserved");
		if (len > max_message_size())
			throw error(errc::too_large, "goipc: message of " + std::to_string(len) + " bytes exceeds the maximum of " +
							      std::to_string(max_message_size()));

		total = static_cast<std::int32_t>(wire::record_header_size + len);
		std::uint64_t aligned = detail::align8(static_cast<std::uint64_t>(total));
		std::uint64_t capacity = mask_ + 1;
		auto tail_ref = u64(base_, wire::offset::tail);
		auto cache_ref = u64(base_, wire::offset::head_cache);

		std::uint64_t index;
		for (;;) {
			std::uint64_t tail = tail_ref.load();
			std::uint64_t head = cache_ref.load();
			index = tail & mask_;
			std::uint64_t to_end = capacity - index;
			std::uint64_t need = aligned;
			if (to_end < aligned)
				need = aligned + to_end;

			if (capacity - (tail - head) < need) {
				head = u64(base_, wire::offset::head).load();
				if (capacity - (tail - head) < need)
					return false;
				cache_ref.store(head);
			}
			if (!tail_ref.compare_exchange_strong(tail, tail + need))
				continue;
			if (need != aligned) {
				std::byte *pad = data_ + index;
				detail::store_type(pad, wire::type_padding);
				detail::length_at(pad).store(static_cast<std::int32_t>(to_end));
				index = 0;
			}
			break;
		}
		rec = data_ + index;
		detail::length_at(rec).store(-total);
		detail::store_type(rec, type);
		return true;
	}

	std::byte *base_ = nullptr;
	std::byte *data_ = nullptr;
	std::uint64_t mask_ = 0;
};

} // namespace goipc

#endif
