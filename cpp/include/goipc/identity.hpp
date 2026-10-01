// Process identity: a random procID and the life socket behind it, as
// docs/design.md "Process identity: the life socket" describes.
#ifndef GOIPC_IDENTITY_HPP
#define GOIPC_IDENTITY_HPP

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <poll.h>
#include <pthread.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include "detail.hpp"
#include "error.hpp"

namespace goipc::detail {

inline bool watchable(std::uint64_t id) noexcept { return (id & wire::proc_watchable) != 0; }

inline void random_bytes(void *buf, std::size_t n)
{
	auto *p = static_cast<unsigned char *>(buf);
	while (n > 0) {
		ssize_t r = ::getrandom(p, n, 0);
		if (r < 0) {
			if (errno == EINTR)
				continue;
			throw_errno("goipc: getrandom");
		}
		p += r;
		n -= static_cast<std::size_t>(r);
	}
}

// unix_address fills addr with path. It returns false when path does not fit.
inline bool unix_address(const std::string &path, sockaddr_un &addr) noexcept
{
	std::memset(&addr, 0, sizeof addr);
	addr.sun_family = AF_UNIX;
	if (path.size() >= sizeof addr.sun_path)
		return false;
	std::memcpy(addr.sun_path, path.c_str(), path.size() + 1);
	return true;
}

// dial_life connects to the life socket at path. The socket never blocks: a
// full backlog fails with EAGAIN instead.
inline int dial_life(const std::string &path) noexcept
{
	sockaddr_un addr;
	if (!unix_address(path, addr)) {
		errno = ENAMETOOLONG;
		return -1;
	}
	int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
	if (fd < 0)
		return -1;
	if (::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof addr) != 0) {
		int err = errno;
		::close(fd);
		errno = err;
		return -1;
	}
	return fd;
}

// life_gone reports whether a dial errno means that no process listens.
inline bool life_gone(int err) noexcept { return err == ENOENT || err == ECONNREFUSED; }

// life holds this process's identity. It is never freed, so the accept thread
// can outlive every static destructor.
struct life {
	std::mutex mu;
	bool ready = false;
	bool atfork = false;
	std::uint64_t id = 0;
	int listen_fd = -1;
	// conns are the accepted connections. Each stays open until its peer closes it or this process exits.
	std::vector<int> conns;
	int error = 0;
};

inline life &life_state()
{
	static life *l = new life;
	return *l;
}

// serve_life accepts every connection to the life socket and holds it open.
// It parks in poll() and never writes to a connection.
inline void serve_life()
{
	life &l = life_state();
	std::vector<pollfd> fds;
	for (;;) {
		fds.clear();
		{
			std::lock_guard lock(l.mu);
			if (l.listen_fd < 0)
				return;
			fds.push_back({l.listen_fd, POLLIN, 0});
			for (int c : l.conns)
				fds.push_back({c, POLLIN, 0});
		}
		if (::poll(fds.data(), fds.size(), -1) < 0) {
			if (errno == EINTR)
				continue;
			fatal("poll on the life socket", errno);
		}
		std::lock_guard lock(l.mu);
		for (std::size_t i = 1; i < fds.size(); i++) {
			if (fds[i].revents == 0)
				continue;
			char buf[64];
			ssize_t n = ::recv(fds[i].fd, buf, sizeof buf, MSG_DONTWAIT);
			if (n > 0 || (n < 0 && (errno == EAGAIN || errno == EINTR)))
				continue;
			::close(fds[i].fd);
			std::erase(l.conns, fds[i].fd);
		}
		if (fds[0].revents & (POLLERR | POLLNVAL))
			fatal("poll on the life socket", EBADF);
		if (fds[0].revents & POLLIN) {
			for (;;) {
				int c = ::accept4(l.listen_fd, nullptr, nullptr, SOCK_CLOEXEC | SOCK_NONBLOCK);
				if (c >= 0) {
					l.conns.push_back(c);
					continue;
				}
				if (errno == EINTR || errno == ECONNABORTED)
					continue;
				if (errno == EAGAIN || errno == EMFILE || errno == ENFILE || errno == ENOBUFS || errno == ENOMEM)
					break;
				fatal("accept on the life socket", errno);
			}
		}
	}
}

// listen_life binds the socket under a temporary name, then renames it into
// place. A bound socket refuses a dial until it listens, and the sweep removes
// a socket that refuses. The sweep never touches a temporary name.
inline int listen_life(std::uint64_t id, int &err) noexcept
{
	std::string path = life_path(id);
	std::string tmp = path + ".tmp";
	sockaddr_un addr;
	if (!unix_address(tmp, addr)) {
		err = ENAMETOOLONG;
		return -1;
	}
	int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
	if (fd < 0) {
		err = errno;
		return -1;
	}
	if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof addr) != 0 || ::listen(fd, SOMAXCONN) != 0 ||
	    ::chmod(tmp.c_str(), 0600) != 0 || ::rename(tmp.c_str(), path.c_str()) != 0) {
		err = errno;
		::close(fd);
		::unlink(tmp.c_str());
		return -1;
	}
	return fd;
}

// The fork handlers give a child a fresh identity.
inline void life_prepare() { life_state().mu.lock(); }
inline void life_parent() { life_state().mu.unlock(); }
inline void life_child()
{
	life &l = life_state();
	if (l.listen_fd >= 0)
		::close(l.listen_fd);
	for (int c : l.conns)
		::close(c);
	l.listen_fd = -1;
	l.conns.clear();
	l.ready = false;
	l.id = 0;
	l.error = 0;
	l.mu.unlock();
}

// self_id returns the procID of this process, and starts its life socket on
// the first call. A process that cannot listen gets an id without the
// watchable bit. Its queues work, but no peer detects its death.
inline std::uint64_t self_id()
{
	life &l = life_state();
	std::lock_guard lock(l.mu);
	if (l.ready)
		return l.id;
	if (!l.atfork) {
		int rc = ::pthread_atfork(life_prepare, life_parent, life_child);
		if (rc != 0)
			throw_errno(rc, "goipc: pthread_atfork");
		l.atfork = true;
	}
	std::uint64_t id;
	random_bytes(&id, sizeof id);
	id |= wire::proc_watchable | wire::proc_any;
	int err = 0;
	int fd = listen_life(id, err);
	if (fd < 0) {
		l.id = id & ~wire::proc_watchable;
		l.error = err;
	} else {
		l.id = id;
		l.listen_fd = fd;
		std::thread(serve_life).detach();
	}
	l.ready = true;
	return l.id;
}

inline int self_error()
{
	self_id();
	life &l = life_state();
	std::lock_guard lock(l.mu);
	return l.error;
}

// is_dead reports whether the process id names has exited. A process that
// cannot be checked counts as alive. The only use of a false answer is to
// leave its claims alone.
inline bool is_dead(std::uint64_t id) noexcept
{
	if (!watchable(id))
		return false;
	int fd = dial_life(life_path(id));
	if (fd >= 0) {
		::close(fd);
		return false;
	}
	return life_gone(errno);
}

// open_watch connects to the life socket of id. The connection ends when that
// process exits.
inline int open_watch(std::uint64_t id)
{
	int fd = dial_life(life_path(id));
	if (fd >= 0)
		return fd;
	if (life_gone(errno))
		return -1;
	throw_errno("goipc: cannot watch process " + life_path(id));
}

enum class watch_state { alive, exited, failed };

// poll_watch reads a watch connection without blocking. The watched process
// never writes, so an end of file means that it exited.
inline watch_state poll_watch(int fd, std::uint64_t id, int &err) noexcept
{
	char buf[64];
	for (;;) {
		ssize_t n = ::recv(fd, buf, sizeof buf, MSG_DONTWAIT);
		if (n == 0)
			return watch_state::exited;
		if (n > 0)
			continue;
		if (errno == EINTR)
			continue;
		if (errno == EAGAIN)
			return watch_state::alive;
		err = errno;
		return is_dead(id) ? watch_state::exited : watch_state::failed;
	}
}

} // namespace goipc::detail

#endif
