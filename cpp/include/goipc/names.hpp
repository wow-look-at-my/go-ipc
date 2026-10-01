// Queue names and their instances, as docs/design.md "Names and instances"
// describes.
#ifndef GOIPC_NAMES_HPP
#define GOIPC_NAMES_HPP

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include <dirent.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include "detail.hpp"
#include "error.hpp"
#include "identity.hpp"

namespace goipc::detail {

inline bool parse_incarnation(std::string_view s) noexcept
{
	if (s.size() != wire::incarnation_len)
		return false;
	for (char c : s) {
		bool hex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
		if (!hex)
			return false;
	}
	return true;
}

inline std::string new_incarnation()
{
	unsigned char raw[wire::incarnation_len / 2];
	random_bytes(raw, sizeof raw);
	static const char digits[] = "0123456789abcdef";
	std::string s;
	for (unsigned char b : raw) {
		s.push_back(digits[b >> 4]);
		s.push_back(digits[b & 15]);
	}
	return s;
}

// read_small reads a file of at most a few bytes. A missing file reads as
// empty. Any other failure sets err.
inline std::string read_small(const std::string &path, int &err)
{
	err = 0;
	int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
	if (fd < 0) {
		if (errno != ENOENT)
			err = errno;
		return {};
	}
	std::string out;
	char buf[64];
	for (;;) {
		ssize_t n = ::read(fd, buf, sizeof buf);
		if (n > 0) {
			out.append(buf, static_cast<std::size_t>(n));
			if (out.size() > wire::incarnation_len)
				break;
			continue;
		}
		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0)
			err = errno;
		break;
	}
	::close(fd);
	return out;
}

// read_incarnation returns the instance id in the .inc file of name, or an
// empty string when the file holds none.
inline std::string read_incarnation(std::string_view name)
{
	int err = 0;
	std::string s = read_small(inc_path(name), err);
	return parse_incarnation(s) ? s : std::string();
}

inline void unlink_quiet(const std::string &path, int &first)
{
	if (::unlink(path.c_str()) != 0 && errno != ENOENT && first == 0)
		first = errno;
}

// remove_instance deletes the segment and both events of an instance. An
// instance that is already gone is not an error.
inline void remove_instance(std::string_view name, std::string_view inc)
{
	std::string inst = instance_name(name, inc);
	int err = 0;
	unlink_quiet(segment_path(inst), err);
	unlink_quiet(event_path(inst + std::string(wire::not_empty_suffix)), err);
	unlink_quiet(event_path(inst + std::string(wire::not_full_suffix)), err);
	if (err)
		throw_errno(err, "goipc: remove instance " + inst);
}

inline void remove_name(std::string_view name)
{
	int err = 0;
	unlink_quiet(inc_path(name), err);
	unlink_quiet(name_path(name), err);
	if (err)
		throw_errno(err, "goipc: remove name " + std::string(name));
}

// unlink_name removes the name files while they still point at inc. A newer
// instance under the same name keeps its files.
inline void unlink_name(std::string_view name, std::string_view inc)
{
	int err = 0;
	std::string current = read_small(inc_path(name), err);
	if (err)
		throw_errno(err, "goipc: read " + inc_path(name));
	if (current != inc)
		return;
	remove_name(name);
}

// name_lock holds a name. The creator keeps it for the life of the queue, and
// the kernel drops the flock when the creator exits.
class name_lock {
public:
	name_lock() = default;
	name_lock(name_lock &&o) noexcept : name_(std::move(o.name_)), fd_(std::exchange(o.fd_, -1)) {}
	name_lock &operator=(name_lock &&o) noexcept
	{
		if (this != &o) {
			release();
			name_ = std::move(o.name_);
			fd_ = std::exchange(o.fd_, -1);
		}
		return *this;
	}
	name_lock(const name_lock &) = delete;
	name_lock &operator=(const name_lock &) = delete;
	~name_lock() { release(); }

	// take locks the name, or fails with errc::in_use while a live process
	// holds it.
	static name_lock take(std::string_view name)
	{
		std::string path = name_path(name);
		for (;;) {
			int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
			if (fd < 0)
				throw_errno("goipc: open " + path);
			if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
				int err = errno;
				::close(fd);
				if (err == EWOULDBLOCK)
					throw error(errc::in_use, "goipc: name \"" + std::string(name) + "\" is in use");
				throw_errno(err, "goipc: flock " + path);
			}
			// Another process can remove or replace the file between the open and the flock.
			struct stat held, now;
			if (::fstat(fd, &held) == 0 && ::stat(path.c_str(), &now) == 0 && held.st_dev == now.st_dev &&
			    held.st_ino == now.st_ino) {
				name_lock l;
				l.name_ = std::string(name);
				l.fd_ = fd;
				return l;
			}
			::close(fd);
		}
	}

	explicit operator bool() const noexcept { return fd_ >= 0; }

	// previous returns the instance the name named before this lock took it.
	std::string previous() const { return read_incarnation(name_); }

	// publish points the name at inc. It is the last step of a create.
	void publish(std::string_view inc) const
	{
		std::string path = inc_path(name_);
		int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
		if (fd < 0)
			throw_errno("goipc: open " + path);
		ssize_t n = ::pwrite(fd, inc.data(), inc.size(), 0);
		int err = n == static_cast<ssize_t>(inc.size()) ? 0 : (n < 0 ? errno : EIO);
		if (err == 0 && ::ftruncate(fd, static_cast<off_t>(inc.size())) != 0)
			err = errno;
		::close(fd);
		if (err)
			throw_errno(err, "goipc: write " + path);
	}

	void release() noexcept
	{
		if (fd_ >= 0)
			::close(std::exchange(fd_, -1));
	}

private:
	std::string name_;
	int fd_ = -1;
};

// read_name returns the instance a name points at.
inline std::string read_name(std::string_view name)
{
	std::string npath = name_path(name);
	struct stat st;
	if (::stat(npath.c_str(), &st) != 0)
		throw_errno("goipc: no endpoint named \"" + std::string(name) + "\"");
	int err = 0;
	std::string inc = read_small(inc_path(name), err);
	if (err)
		throw_errno(err, "goipc: read " + inc_path(name));
	if (!parse_incarnation(inc))
		throw_errno(ENOENT, "goipc: endpoint \"" + std::string(name) + "\" is not ready");
	return inc;
}

inline bool valid_name(std::string_view name) noexcept
{
	return !name.empty() && name != "." && name != ".." && name.find_first_of("/\\") == std::string_view::npos;
}

// sweep_life removes the life socket of a process that has exited. Nothing
// listens there, so a dial is refused.
inline void sweep_life(const std::string &path) noexcept
{
	int fd = dial_life(path);
	if (fd >= 0) {
		::close(fd);
		return;
	}
	if (errno == ECONNREFUSED)
		::unlink(path.c_str());
}

// sweep_name removes a name that no live process holds. The cleanup is on
// behalf of processes that are gone. A failure costs this caller nothing, and
// the next sweep tries again.
inline void sweep_name(std::string_view name) noexcept
{
	try {
		name_lock l = name_lock::take(name);
		std::string inc = l.previous();
		if (!inc.empty())
			remove_instance(name, inc);
		remove_name(name);
	} catch (const std::exception &) {
	}
}

inline bool cut_suffix(std::string_view &s, std::string_view suffix) noexcept
{
	if (s.size() < suffix.size() || s.substr(s.size() - suffix.size()) != suffix)
		return false;
	s.remove_suffix(suffix.size());
	return true;
}

inline void sweep_dir(const std::string &dir)
{
	DIR *d = ::opendir(dir.c_str());
	if (!d)
		return;
	std::vector<std::string> entries;
	while (dirent *e = ::readdir(d))
		entries.emplace_back(e->d_name);
	::closedir(d);
	for (const std::string &entry : entries) {
		std::string_view s = entry;
		if (s.starts_with(wire::life_prefix) && s.ends_with(wire::life_suffix)) {
			sweep_life(dir + "/" + entry);
			continue;
		}
		if (!s.starts_with(wire::name_prefix))
			continue;
		s.remove_prefix(wire::name_prefix.size());
		if (!cut_suffix(s, wire::name_suffix) && !cut_suffix(s, wire::inc_suffix))
			continue;
		if (valid_name(s))
			sweep_name(s);
	}
}

inline std::atomic<bool> &swept()
{
	static std::atomic<bool> done{false};
	return done;
}

// sweep_stale removes every name that no live process holds, with the
// instance it points at. It runs once per process.
inline void sweep_stale()
{
	if (!swept().exchange(true))
		sweep_dir(std::string(wire::runtime_dir));
}

} // namespace goipc::detail

#endif
