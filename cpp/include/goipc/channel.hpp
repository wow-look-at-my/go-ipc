// Channel: a pair of queues with opposite directions.
#ifndef GOIPC_CHANNEL_HPP
#define GOIPC_CHANNEL_HPP

#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include "detail.hpp"
#include "queue.hpp"

namespace goipc {

// Channel is a bidirectional named endpoint between a pair of processes. The
// creator sends on name.c2o and receives on name.o2c. The opener swaps both.
// Each side's peer is the receiver of its outbound queue.
class Channel {
public:
	Channel() = default;
	Channel(Channel &&) noexcept = default;
	Channel &operator=(Channel &&o) noexcept
	{
		if (this != &o) {
			close();
			name_ = std::move(o.name_);
			tx_ = std::move(o.tx_);
			rx_ = std::move(o.rx_);
		}
		return *this;
	}
	~Channel()
	{
		try {
			close();
		} catch (...) {
		}
	}

	// create makes both directions. It raises errc::in_use while a live
	// process holds the name. The direction to the peer has no reader until a
	// peer connects; sends to it wait in the ring.
	static Channel create(std::string_view name, std::size_t capacity = wire::default_capacity)
	{
		detail::validate_name(name);
		std::string n(name);
		Channel c;
		c.name_ = n;
		c.tx_ = Queue::create_queue(n + std::string(wire::creator_to_opener_suffix), capacity, true);
		try {
			c.rx_ = Queue::create_queue(n + std::string(wire::opener_to_creator_suffix), capacity, false);
		} catch (...) {
			c.tx_.close();
			c.tx_.unlink();
			throw;
		}
		c.link();
		return c;
	}

	// open attaches to a channel the peer created. It raises errc::peer_gone
	// when the creator has gone, and errc::in_use when another peer holds the
	// channel.
	static Channel open(std::string_view name)
	{
		detail::validate_name(name);
		std::string n(name);
		Channel c;
		c.name_ = n;
		c.tx_ = Queue::open(n + std::string(wire::opener_to_creator_suffix));
		c.rx_ = Queue::open_queue(n + std::string(wire::creator_to_opener_suffix));
		auto &rx = *c.rx_.s_;
		std::uint64_t pending = wire::consumer_pending;
		std::atomic_ref<std::uint64_t> consumer(*reinterpret_cast<std::uint64_t *>(rx.map + wire::offset::consumer));
		if (!consumer.compare_exchange_strong(pending, rx.self))
			throw error(errc::in_use, "goipc: channel \"" + n + "\" is in use");
		rx.reader = true;
		c.link();
		// The creator may already be parked. It wakes to find its peer, and to start watching the peer's process.
		c.tx_.s_->ne.signal(1);
		rx.wake_senders();
		return c;
	}

	// remove deletes both directions' files. Missing files are not an error.
	static void remove(std::string_view name)
	{
		detail::validate_name(name);
		std::string n(name);
		Queue::remove(n + std::string(wire::creator_to_opener_suffix));
		Queue::remove(n + std::string(wire::opener_to_creator_suffix));
	}

	const std::string &name() const noexcept { return name_; }
	Queue &tx() noexcept { return tx_; }
	Queue &rx() noexcept { return rx_; }
	std::size_t max_message_size() const { return tx_.max_message_size(); }

	bool try_send(std::uint32_t type, std::span<const std::byte> payload) { return tx_.try_send(type, payload); }
	void send(std::uint32_t type, std::span<const std::byte> payload, timeout t = forever) { tx_.send(type, payload, t); }
	Claim claim(std::uint32_t type, std::size_t len, timeout t = forever) { return tx_.claim(type, len, t); }

	std::optional<received> try_recv(std::span<std::byte> dst) { return rx_.try_recv(dst); }
	std::optional<message> try_recv() { return rx_.try_recv(); }
	received recv(std::span<std::byte> dst, timeout t = forever) { return rx_.recv(dst, t); }
	message recv(timeout t = forever) { return rx_.recv(t); }

	template <class F>
	std::size_t read_batch(std::size_t limit, F &&fn, timeout t = forever)
	{
		return rx_.read_batch(limit, fn, t);
	}

	// close releases this process's handles on both directions. The receiving
	// end closes first. The signal after it wakes a peer parked in recv, which
	// then finds this side gone.
	void close()
	{
		std::exception_ptr first;
		try {
			rx_.close();
		} catch (...) {
			first = std::current_exception();
		}
		if (tx_.s_ && !tx_.s_->g.closing()) {
			try {
				tx_.s_->ne.signal(1);
			} catch (...) {
			}
		}
		try {
			tx_.close();
		} catch (...) {
			if (!first)
				first = std::current_exception();
		}
		if (first)
			std::rethrow_exception(first);
	}

	void unlink()
	{
		tx_.unlink();
		rx_.unlink();
	}

private:
	// link makes the receive side report the end of the far side, which is the receiver of tx.
	void link() { rx_.s_->peer_queue = tx_.s_; }

	std::string name_;
	Queue tx_;
	Queue rx_;
};

} // namespace goipc

#endif
