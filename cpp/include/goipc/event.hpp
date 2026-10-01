// Event: a named cross-process wake channel backed by a FIFO.
#ifndef GOIPC_EVENT_HPP
#define GOIPC_EVENT_HPP

#include <cerrno>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <unistd.h>

#include "detail.hpp"
#include "error.hpp"

namespace goipc {

namespace detail {

struct event_state {
	std::string path;
	int fifo = -1;
	// close_fd turns readable when the event closes, and stays readable, so it releases every waiter that polls it.
	int close_fd = -1;
	gate g;

	~event_state()
	{
		if (fifo >= 0)
			::close(fifo);
		if (close_fd >= 0)
			::close(close_fd);
	}
};

inline int open_fifo(const std::string &path)
{
	int fd = ::open(path.c_str(), O_RDWR | O_NONBLOCK | O_CLOEXEC);
	if (fd < 0)
		throw_errno("goipc: open " + path);
	struct stat st;
	if (::fstat(fd, &st) != 0) {
		int err = errno;
		::close(fd);
		throw_errno(err, "goipc: fstat " + path);
	}
	if (!S_ISFIFO(st.st_mode)) {
		::close(fd);
		throw error(errc::invalid, "goipc: " + path + " is not a FIFO");
	}
	return fd;
}

inline std::unique_ptr<event_state> new_event(const std::string &path)
{
	auto s = std::make_unique<event_state>();
	s->path = path;
	s->fifo = open_fifo(path);
	s->close_fd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
	if (s->close_fd < 0)
		throw_errno("goipc: eventfd");
	return s;
}

} // namespace detail

// Event is a counting wake channel. signal(n) releases up to n waiters, in
// this process or another. A signal with no waiter stays pending.
class Event {
public:
	Event() = default;
	Event(Event &&) noexcept = default;
	Event &operator=(Event &&o) noexcept
	{
		if (this != &o) {
			close();
			s_ = std::move(o.s_);
		}
		return *this;
	}
	~Event() { close(); }

	// create replaces any file at the event's path with a new FIFO.
	static Event create(std::string_view name)
	{
		detail::validate_name(name);
		std::string path = detail::event_path(name);
		if (::unlink(path.c_str()) != 0 && errno != ENOENT)
			detail::throw_errno("goipc: unlink " + path);
		if (::mkfifo(path.c_str(), 0600) != 0 && errno != EEXIST)
			detail::throw_errno("goipc: mkfifo " + path);
		return Event(detail::new_event(path));
	}

	static Event open(std::string_view name)
	{
		detail::validate_name(name);
		return Event(detail::new_event(detail::event_path(name)));
	}

	// remove deletes the named event's file. A missing file is not an error.
	static void remove(std::string_view name)
	{
		detail::validate_name(name);
		std::string path = detail::event_path(name);
		if (::unlink(path.c_str()) != 0 && errno != ENOENT)
			detail::throw_errno("goipc: unlink " + path);
	}

	const std::string &path() const noexcept
	{
		static const std::string none;
		return s_ ? s_->path : none;
	}

	void signal(int n = 1)
	{
		if (!s_)
			throw error(errc::closed, "goipc: event is closed");
		if (n <= 0)
			return;
		if (n > wire::signal_max_tokens)
			n = wire::signal_max_tokens;
		detail::gate_guard in(s_->g);
		static const char tokens[wire::signal_max_tokens] = {};
		ssize_t w = ::write(s_->fifo, tokens, static_cast<std::size_t>(n));
		// A full pipe already holds more wakeups than there are waiters.
		if (w < 0 && errno != EAGAIN)
			detail::throw_errno("goipc: signal " + s_->path);
	}

	// wait consumes one token.
	bool wait(timeout t = forever) { return wait_until(detail::deadline(t)); }

	bool wait_until(const detail::deadline &d)
	{
		if (!s_)
			throw error(errc::closed, "goipc: event is closed");
		detail::gate_guard in(s_->g);
		pollfd fds[2] = {{s_->close_fd, POLLIN, 0}, {s_->fifo, POLLIN, 0}};
		for (;;) {
			timespec ts{};
			if (d.finite())
				ts = d.remaining();
			int r = ::ppoll(fds, 2, d.finite() ? &ts : nullptr, nullptr);
			if (r < 0) {
				if (errno == EINTR)
					continue;
				detail::throw_errno("goipc: poll " + s_->path);
			}
			if (fds[0].revents)
				throw error(errc::closed, "goipc: event is closed");
			if (r == 0)
				return false;
			if (fds[1].revents & (POLLERR | POLLNVAL))
				throw error(errc::invalid, "goipc: poll reported an error on " + s_->path);
			char b;
			ssize_t n = ::read(s_->fifo, &b, 1);
			if (n == 1)
				return true;
			if (n == 0)
				throw error(errc::invalid, "goipc: unexpected end of file on " + s_->path);
			// EAGAIN: another waiter took the token, so block again.
			if (errno != EAGAIN && errno != EINTR)
				detail::throw_errno("goipc: read " + s_->path);
		}
	}

	// shutdown releases every waiter of this handle and fails every later
	// wait with errc::closed. Signals still work until close.
	void shutdown()
	{
		if (!s_ || !s_->g.enter())
			return;
		std::uint64_t one = 1;
		ssize_t w = ::write(s_->close_fd, &one, sizeof one);
		int err = errno;
		s_->g.leave();
		if (w != static_cast<ssize_t>(sizeof one))
			detail::throw_errno(err, "goipc: eventfd write for " + s_->path);
	}

	// close releases every waiter, waits for calls in flight to return, then
	// closes the handles. It does not remove the file.
	void close()
	{
		if (!s_)
			return;
		shutdown();
		if (!s_->g.close())
			return;
		s_->g.drain();
		int fifo = std::exchange(s_->fifo, -1);
		int cfd = std::exchange(s_->close_fd, -1);
		int err = 0;
		if (::close(fifo) != 0)
			err = errno;
		if (::close(cfd) != 0 && err == 0)
			err = errno;
		if (err)
			detail::throw_errno(err, "goipc: close " + s_->path);
	}

	// unlink removes this event's file. A missing file is not an error.
	void unlink()
	{
		if (!s_)
			throw error(errc::closed, "goipc: event is closed");
		if (::unlink(s_->path.c_str()) != 0 && errno != ENOENT)
			detail::throw_errno("goipc: unlink " + s_->path);
	}

private:
	explicit Event(std::unique_ptr<detail::event_state> s) : s_(std::move(s)) {}

	std::unique_ptr<detail::event_state> s_;
};

} // namespace goipc

#endif
