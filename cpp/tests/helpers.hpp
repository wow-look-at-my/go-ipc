// Shared test helpers.
#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <stdexcept>

#include <fcntl.h>
#include <poll.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <goipc/goipc.hpp>

namespace testutil {

// unique_name returns a name no other test or process uses.
inline std::string unique_name(std::string_view tag)
{
	static std::atomic<int> counter{0};
	return "cpptest-" + std::string(tag) + "-" + std::to_string(::getpid()) + "-" + std::to_string(counter++);
}

// aligned_buffer is a zeroed buffer on an 8-byte boundary.
class aligned_buffer {
public:
	explicit aligned_buffer(std::size_t n) : words_((n + 7) / 8 + 1, 0), size_(n) {}
	std::span<std::byte> span() { return {reinterpret_cast<std::byte *>(words_.data()), size_}; }
	std::byte *data() { return reinterpret_cast<std::byte *>(words_.data()); }
	std::size_t size() const { return size_; }

private:
	std::vector<std::uint64_t> words_;
	std::size_t size_;
};

inline std::span<const std::byte> bytes_of(std::string_view s) { return std::as_bytes(std::span(s.data(), s.size())); }

inline std::string text_of(std::span<const std::byte> b) { return {reinterpret_cast<const char *>(b.data()), b.size()}; }

inline bool file_exists(const std::string &path)
{
	struct stat st;
	return ::stat(path.c_str(), &st) == 0;
}

inline std::string read_text(const std::string &path)
{
	std::string out;
	int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
	if (fd < 0)
		return out;
	char buf[256];
	ssize_t n;
	while ((n = ::read(fd, buf, sizeof buf)) > 0)
		out.append(buf, static_cast<std::size_t>(n));
	::close(fd);
	return out;
}

// pipe_pair carries one-byte commands between a test and a forked child.
class pipe_pair {
public:
	pipe_pair()
	{
		if (::pipe2(fds_, O_CLOEXEC) != 0)
			throw std::runtime_error("pipe2 failed");
	}
	~pipe_pair()
	{
		::close(fds_[0]);
		::close(fds_[1]);
	}
	pipe_pair(const pipe_pair &) = delete;
	pipe_pair &operator=(const pipe_pair &) = delete;

	void signal(char c = 1) const
	{
		if (::write(fds_[1], &c, 1) != 1)
			throw std::runtime_error("pipe write failed");
	}

	bool wait() const
	{
		pollfd p{fds_[0], POLLIN, 0};
		if (::poll(&p, 1, 30000) != 1)
			return false;
		char c;
		return ::read(fds_[0], &c, 1) == 1;
	}

	void send_u64(std::uint64_t v) const
	{
		if (::write(fds_[1], &v, sizeof v) != sizeof v)
			throw std::runtime_error("pipe write failed");
	}

	std::uint64_t recv_u64() const
	{
		std::uint64_t v = 0;
		if (::read(fds_[0], &v, sizeof v) != sizeof v)
			throw std::runtime_error("pipe read failed");
		return v;
	}

private:
	int fds_[2];
};

struct queue_files {
	std::string name;
	~queue_files() { goipc::Queue::remove(name); }
};

struct channel_files {
	std::string name;
	~channel_files() { goipc::Channel::remove(name); }
};

// fn returns the exit code; an exception exits 99.
inline pid_t fork_child(const std::function<int()> &fn)
{
	pid_t pid = ::fork();
	if (pid == 0) {
		int code = 99;
		try {
			code = fn();
		} catch (const std::exception &e) {
			std::fprintf(stderr, "child: %s\n", e.what());
		}
		std::fflush(stderr);
		::_exit(code);
	}
	return pid;
}

struct child_result {
	int status;
	std::chrono::microseconds cpu;
};

inline child_result wait_child(pid_t pid)
{
	int status = 0;
	struct rusage ru {};
	pid_t r = ::wait4(pid, &status, 0, &ru);
	if (r != pid)
		throw std::runtime_error("wait4 failed");
	auto cpu = std::chrono::seconds(ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) +
		   std::chrono::microseconds(ru.ru_utime.tv_usec + ru.ru_stime.tv_usec);
	return {status, std::chrono::duration_cast<std::chrono::microseconds>(cpu)};
}

inline int exit_code(int status) { return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status); }

// eventually polls cond. A test uses it to observe that another thread has
// parked; the library itself never polls.
inline bool eventually(const std::function<bool()> &cond, std::chrono::milliseconds limit = std::chrono::seconds(10))
{
	auto end = std::chrono::steady_clock::now() + limit;
	while (std::chrono::steady_clock::now() < end) {
		if (cond())
			return true;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return cond();
}

// errc_of runs fn and returns the goipc code it raised, or errc::invalid when
// it raised nothing.
template <class F>
goipc::errc errc_of(F &&fn)
{
	try {
		fn();
	} catch (const goipc::error &e) {
		return e.value();
	}
	ADD_FAILURE() << "no goipc::error was raised";
	return goipc::errc::invalid;
}

} // namespace testutil
