// Error codes and the exception type. The numeric values match goipc_err in
// c/include/goipc.h, so a code means the same thing in every language.
#ifndef GOIPC_ERROR_HPP
#define GOIPC_ERROR_HPP

#include <cerrno>
#include <cstddef>
#include <string>
#include <system_error>
#include <type_traits>

namespace goipc {

enum class errc : int {
	closed = -1,
	full = -2,
	empty = -3,
	too_large = -4,
	reserved_type = -5,
	invalid_name = -6,
	invalid_capacity = -7,
	too_small = -8,
	bad_layout = -9,
	corrupt = -10,
	unaligned = -11,
	timed_out = -12,
	// A system call failed. The error's code() is the errno in system_category.
	sys = -13,
	no_memory = -14,
	invalid = -15,
	// The receive buffer is smaller than the next message, which stays queued.
	buffer = -16,
	// The conn peer ended the stream.
	eof = -17,
	// The process at the other end exited or closed its end.
	peer_gone = -18,
	// A live process holds the name.
	in_use = -19,
	// The handle does not own the receiving end.
	not_consumer = -20,
	// Live claims hold every claim slot of the ring.
	too_many_claims = -21,
	// A service handler returned an error. The error's what() carries its message.
	call = -22,
};

namespace detail {

class category_impl final : public std::error_category {
public:
	const char *name() const noexcept override { return "goipc"; }

	std::string message(int v) const override
	{
		switch (static_cast<errc>(v)) {
		case errc::closed: return "endpoint is closed";
		case errc::full: return "ring is full";
		case errc::empty: return "ring is empty";
		case errc::too_large: return "message exceeds the maximum size";
		case errc::reserved_type: return "message type is reserved";
		case errc::invalid_name: return "name must not be empty or contain a separator";
		case errc::invalid_capacity: return "capacity must be a power of two of at least 4096 bytes";
		case errc::too_small: return "buffer is too small to hold a ring";
		case errc::bad_layout: return "buffer does not hold a compatible ring";
		case errc::corrupt: return "ring contents are corrupt";
		case errc::unaligned: return "buffer is not 8-byte aligned";
		case errc::timed_out: return "operation timed out";
		case errc::sys: return "system call failed";
		case errc::no_memory: return "out of memory";
		case errc::invalid: return "invalid argument";
		case errc::buffer: return "receive buffer is smaller than the next message";
		case errc::eof: return "end of stream";
		case errc::peer_gone: return "peer is gone";
		case errc::in_use: return "name is in use";
		case errc::not_consumer: return "handle is not the receiving end";
		case errc::too_many_claims: return "too many claims in progress";
		case errc::call: return "call failed";
		}
		return "unknown goipc error";
	}
};

} // namespace detail

inline const std::error_category &category() noexcept
{
	static const detail::category_impl c;
	return c;
}

inline std::error_code make_error_code(errc e) noexcept
{
	return {static_cast<int>(e), category()};
}

// to_errc maps any error code this library reports onto the shared numbering.
// A system_category code becomes errc::sys.
inline errc to_errc(const std::error_code &ec) noexcept
{
	if (ec.category() == category())
		return static_cast<errc>(ec.value());
	return errc::sys;
}

// error is what every throwing call raises. code() is a goipc code, or the
// errno of a failed system call in system_category.
class error : public std::system_error {
public:
	error(errc e, const std::string &what, std::size_t needed = 0)
		: std::system_error(make_error_code(e), what), needed_(needed) {}

	error(std::error_code ec, const std::string &what)
		: std::system_error(ec, what) {}

	errc value() const noexcept { return to_errc(code()); }

	// needed is the size the next message requires, for errc::buffer.
	std::size_t needed() const noexcept { return needed_; }

private:
	std::size_t needed_ = 0;
};

namespace detail {

[[noreturn]] inline void throw_errno(int err, const std::string &what)
{
	throw error(std::error_code(err, std::system_category()), what);
}

[[noreturn]] inline void throw_errno(const std::string &what)
{
	throw_errno(errno, what);
}

} // namespace detail

} // namespace goipc

template <>
struct std::is_error_code_enum<goipc::errc> : std::true_type {};

#endif
