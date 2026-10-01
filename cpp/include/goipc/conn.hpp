// Conn: a byte stream over a channel.
#ifndef GOIPC_CONN_HPP
#define GOIPC_CONN_HPP

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstring>
#include <memory>
#include <mutex>
#include <span>
#include <string_view>
#include <vector>

#include "channel.hpp"
#include "detail.hpp"
#include "error.hpp"

namespace goipc {

namespace detail {

struct conn_state {
	Channel ch;
	std::vector<std::byte> buf;
	std::size_t pos = 0;
	std::size_t len = 0;
	bool eof = false;
	std::mutex read_mu;
	std::mutex write_mu;
	bool write_closed = false;
	std::atomic<bool> eof_sent{false};
	std::atomic<bool> closed{false};
};

} // namespace detail

// Conn is a byte stream. write splits its input into messages of at most the
// maximum message size, and read reassembles them. One thread may read while
// another writes.
class Conn {
public:
	Conn() = default;
	Conn(Conn &&) noexcept = default;
	Conn &operator=(Conn &&o) noexcept
	{
		if (this != &o) {
			close();
			s_ = std::move(o.s_);
		}
		return *this;
	}
	~Conn() { close(); }

	// listen creates the channel. The peer calls dial with the same name.
	static Conn listen(std::string_view name, std::size_t capacity = wire::default_capacity)
	{
		return Conn(Channel::create(name, capacity));
	}

	static Conn dial(std::string_view name) { return Conn(Channel::open(name)); }

	Channel &channel() { return state().ch; }

	// eof reports whether the peer ended the stream and every byte is read.
	bool eof() const
	{
		auto &s = state();
		std::lock_guard lock(s.read_mu);
		return s.eof && s.pos == s.len;
	}

	// read copies up to buf.size() bytes into buf.
	std::size_t read(std::span<std::byte> buf, timeout t = forever)
	{
		auto &s = state();
		std::lock_guard lock(s.read_mu);
		if (buf.empty())
			return 0;
		detail::deadline d(t);
		while (s.pos == s.len) {
			if (s.eof)
				return 0;
			received r = s.ch.recv(s.buf, d.left());
			if (r.type == wire::conn_type_eof) {
				s.eof = true;
				return 0;
			}
			s.pos = 0;
			s.len = r.size;
		}
		std::size_t n = std::min(buf.size(), s.len - s.pos);
		std::memcpy(buf.data(), s.buf.data() + s.pos, n);
		s.pos += n;
		return n;
	}

	// write sends all of buf. It raises an error when it cannot.
	void write(std::span<const std::byte> buf, timeout t = forever)
	{
		auto &s = state();
		std::lock_guard lock(s.write_mu);
		if (s.write_closed)
			throw error(errc::closed, "goipc: conn write side is closed");
		detail::deadline d(t);
		std::size_t limit = s.ch.max_message_size();
		while (!buf.empty()) {
			std::size_t n = std::min(limit, buf.size());
			s.ch.send(wire::conn_type_data, buf.first(n), d.left());
			buf = buf.subspan(n);
		}
	}

	// close_write ends the stream for the peer and waits for room to say so.
	// Later writes raise errc::closed. Reads keep working.
	void close_write(timeout t = forever)
	{
		auto &s = state();
		std::lock_guard lock(s.write_mu);
		if (s.write_closed)
			return;
		s.write_closed = true;
		s.ch.send(wire::conn_type_eof, {}, t);
		s.eof_sent.store(true);
	}

	// close sends end-of-stream, best effort and without blocking, unless
	// close_write sent it. Then it closes the channel.
	void close()
	{
		if (!s_ || s_->closed.exchange(true))
			return;
		if (s_->eof_sent.load()) {
			s_->ch.close();
			return;
		}
		try {
			s_->ch.tx().try_send(wire::conn_type_eof, {});
		} catch (const error &e) {
			if (e.value() != errc::closed)
				throw;
		}
		s_->ch.close();
	}

	// unlink removes the channel's files. Only the listening side owns them.
	void unlink() { state().ch.unlink(); }

private:
	explicit Conn(Channel ch) : s_(std::make_unique<detail::conn_state>())
	{
		s_->ch = std::move(ch);
		s_->buf.resize(s_->ch.max_message_size());
	}

	detail::conn_state &state() const
	{
		if (!s_)
			throw error(errc::closed, "goipc: conn has no state");
		return *s_;
	}

	std::unique_ptr<detail::conn_state> s_;
};

} // namespace goipc

#endif
