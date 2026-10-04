// Wire constants and internals shared by every layer.
#ifndef GOIPC_DETAIL_HPP
#define GOIPC_DETAIL_HPP

#include <atomic>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <optional>
#include <string>
#include <string_view>

#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "error.hpp"

namespace goipc {

static_assert(std::endian::native == std::endian::little, "the wire format is little-endian");

// The constants of spec/wire.json.
namespace wire {

inline constexpr std::uint64_t ring_magic = 0x676F2D6970632D31ULL;
inline constexpr std::uint32_t ring_version = 2;
inline constexpr std::size_t cache_line = 128;
inline constexpr std::size_t control_size = 4 * cache_line;
inline constexpr std::size_t claim_slots = 256;
inline constexpr std::size_t slot_size = 64;
inline constexpr std::size_t header_size = control_size + claim_slots * slot_size;
// no_intent marks a claim slot whose owner claims nothing right now.
inline constexpr std::uint64_t no_intent = ~std::uint64_t(0);
inline constexpr std::size_t min_capacity = 4096;
inline constexpr std::size_t record_header_size = 8;
inline constexpr std::size_t record_alignment = 8;
inline constexpr std::uint32_t type_padding = 0xFFFFFFFFu;

inline constexpr std::size_t default_capacity = 1048576;
inline constexpr int signal_max_tokens = 4096;
inline constexpr std::string_view runtime_dir = "/dev/shm";
inline constexpr std::string_view segment_prefix = "/dev/shm/go-shm-";
inline constexpr std::string_view event_prefix = "/dev/shm/go-ipc-";
inline constexpr std::string_view event_suffix = ".event";
inline constexpr std::string_view name_prefix = "go-ipc-";
inline constexpr std::string_view name_suffix = ".name";
inline constexpr std::string_view inc_suffix = ".inc";
inline constexpr std::string_view life_prefix = "go-ipc-life-";
inline constexpr std::string_view life_suffix = ".sock";
// incarnation_len is the count of hex digits in an instance id.
inline constexpr std::size_t incarnation_len = 16;

// Values of the consumer field that are not a procID.
inline constexpr std::uint64_t consumer_none = 0;
inline constexpr std::uint64_t consumer_pending = 1;
// proc_watchable marks a procID with a life socket behind it.
inline constexpr std::uint64_t proc_watchable = std::uint64_t(1) << 63;
inline constexpr std::uint64_t proc_any = std::uint64_t(1) << 62;
inline constexpr std::string_view not_empty_suffix = ".ne";
inline constexpr std::string_view not_full_suffix = ".nf";
inline constexpr std::string_view creator_to_opener_suffix = ".c2o";
inline constexpr std::string_view opener_to_creator_suffix = ".o2c";

inline constexpr std::uint32_t conn_type_data = 0;
inline constexpr std::uint32_t conn_type_eof = 1;

// The service layer of spec/service.md.
inline constexpr std::string_view service_registry_suffix = ".svc";
inline constexpr std::string_view service_client_prefix = ".c.";
// service_reserved_type_min starts the record types the service layer keeps for itself.
inline constexpr std::uint32_t service_reserved_type_min = 0xFFFFFFF0u;
inline constexpr std::uint32_t service_type_knock = 0xFFFFFFF0u;
inline constexpr std::uint32_t service_type_hello = 0xFFFFFFF1u;
inline constexpr std::uint32_t service_type_error = 0xFFFFFFF2u;
// service_sequence_size is the sequence number that starts every request and reply payload.
inline constexpr std::size_t service_sequence_size = 8;
inline constexpr std::uint64_t service_first_sequence = 1;

// Offsets of the control block fields.
namespace offset {
inline constexpr std::size_t magic = 0;
inline constexpr std::size_t version = 8;
inline constexpr std::size_t flags = 12;
inline constexpr std::size_t capacity = 16;
inline constexpr std::size_t consumer = 24;
inline constexpr std::size_t tail = 128;
inline constexpr std::size_t head = 256;
inline constexpr std::size_t head_cache = 384;
inline constexpr std::size_t recv_waiters = 392;
inline constexpr std::size_t send_waiters = 396;
inline constexpr std::size_t slots = control_size;
// Offsets inside one claim slot.
inline constexpr std::size_t slot_owner = 0;
inline constexpr std::size_t slot_at = 8;
inline constexpr std::size_t slot_claim_size = 16;
} // namespace offset

} // namespace wire

// timeout bounds a blocking call. std::nullopt waits forever.
using timeout = std::optional<std::chrono::nanoseconds>;
inline constexpr timeout forever = std::nullopt;

namespace detail {

inline void validate_name(std::string_view name)
{
	if (name.empty() || name == "." || name == ".." || name.find_first_of("/\\") != std::string_view::npos)
		throw error(errc::invalid_name, "goipc: invalid name \"" + std::string(name) + "\"");
}

inline std::string event_path(std::string_view name)
{
	std::string p(wire::event_prefix);
	p.append(name);
	p.append(wire::event_suffix);
	return p;
}

inline std::string segment_path(std::string_view name)
{
	std::string p(wire::segment_prefix);
	p.append(name);
	return p;
}

inline std::string runtime_path(std::string_view file)
{
	std::string p(wire::runtime_dir);
	p.push_back('/');
	p.append(file);
	return p;
}

// name_path is the file that a creator locks for the life of a queue.
inline std::string name_path(std::string_view name)
{
	return runtime_path(std::string(wire::name_prefix) + std::string(name) + std::string(wire::name_suffix));
}

// inc_path holds the id of the instance that a name points at.
inline std::string inc_path(std::string_view name)
{
	return runtime_path(std::string(wire::name_prefix) + std::string(name) + std::string(wire::inc_suffix));
}

inline std::string instance_name(std::string_view name, std::string_view inc)
{
	std::string s(name);
	s.push_back('.');
	s.append(inc);
	return s;
}

inline std::string life_path(std::uint64_t id)
{
	char hex[17];
	std::snprintf(hex, sizeof hex, "%016llx", static_cast<unsigned long long>(id));
	return runtime_path(std::string(wire::life_prefix) + hex + std::string(wire::life_suffix));
}

[[noreturn]] inline void fatal(const char *what, int err)
{
	std::fprintf(stderr, "goipc: %s failed: errno %d\n", what, err);
	std::abort();
}

inline void futex_wait(std::uint32_t *addr, std::uint32_t expected) noexcept
{
	if (syscall(SYS_futex, addr, FUTEX_WAIT_PRIVATE, expected, nullptr, nullptr, 0) == -1) {
		if (errno != EAGAIN && errno != EINTR)
			fatal("futex wait", errno);
	}
}

inline void futex_wake_all(std::uint32_t *addr) noexcept
{
	// The waker may run after the waiter returned and freed the word.
	syscall(SYS_futex, addr, FUTEX_WAKE_PRIVATE, INT32_MAX, nullptr, nullptr, 0);
}

// gate counts operations in flight so that a close can wait for them before
// it releases memory they use. The top bit marks the gate closed, so a leaver
// reads that mark in the same atomic step as its decrement and touches no
// other field afterwards.
class gate {
public:
	bool enter() noexcept
	{
		if (ref().fetch_add(1) & closing_bit) {
			leave();
			return false;
		}
		return true;
	}

	void leave() noexcept
	{
		if (ref().fetch_sub(1) == (closing_bit | 1))
			futex_wake_all(&word_);
	}

	// close marks the gate. It returns false when another close came first.
	bool close() noexcept { return (ref().fetch_or(closing_bit) & closing_bit) == 0; }

	bool closing() const noexcept { return (cref().load() & closing_bit) != 0; }

	// drain blocks in the kernel until every operation has left.
	void drain() noexcept
	{
		for (;;) {
			std::uint32_t v = ref().load();
			if (v == closing_bit)
				return;
			futex_wait(&word_, v);
		}
	}

private:
	static constexpr std::uint32_t closing_bit = 0x80000000u;

	std::atomic_ref<std::uint32_t> ref() noexcept { return std::atomic_ref<std::uint32_t>(word_); }
	std::atomic_ref<std::uint32_t> cref() const noexcept
	{
		return std::atomic_ref<std::uint32_t>(const_cast<std::uint32_t &>(word_));
	}

	alignas(4) std::uint32_t word_ = 0;
};

// guard holds a gate entry for one scope.
class gate_guard {
public:
	explicit gate_guard(gate &g) : g_(g)
	{
		if (!g_.enter())
			throw error(errc::closed, "goipc: endpoint is closed");
	}
	~gate_guard() { g_.leave(); }
	gate_guard(const gate_guard &) = delete;
	gate_guard &operator=(const gate_guard &) = delete;

private:
	gate &g_;
};

// deadline turns a relative timeout into an absolute point on the monotonic
// clock, so a wait that resumes after a spurious wakeup keeps its budget.
class deadline {
public:
	explicit deadline(timeout t)
	{
		if (t) {
			auto d = *t < std::chrono::nanoseconds::zero() ? std::chrono::nanoseconds::zero() : *t;
			at_ = std::chrono::steady_clock::now() + d;
			finite_ = true;
		}
	}

	bool finite() const noexcept { return finite_; }

	// left is the time that remains, as a timeout for a nested call.
	timeout left() const noexcept
	{
		if (!finite_)
			return forever;
		timespec ts = remaining();
		return std::chrono::seconds(ts.tv_sec) + std::chrono::nanoseconds(ts.tv_nsec);
	}

	// remaining returns the time left, clamped at zero, as a timespec.
	timespec remaining() const noexcept
	{
		auto left = at_ - std::chrono::steady_clock::now();
		if (left < std::chrono::steady_clock::duration::zero())
			left = std::chrono::steady_clock::duration::zero();
		auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(left).count();
		timespec ts{};
		ts.tv_sec = static_cast<time_t>(ns / 1000000000);
		ts.tv_nsec = static_cast<long>(ns % 1000000000);
		return ts;
	}

private:
	std::chrono::steady_clock::time_point at_{};
	bool finite_ = false;
};

} // namespace detail

} // namespace goipc

#endif
