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

// Channel is a bidirectional named endpoint. The creator sends on name.c2o and
// receives on name.o2c. The opener swaps both.
class Channel {
public:
	Channel() = default;
	Channel(Channel &&) noexcept = default;
	Channel &operator=(Channel &&) noexcept = default;
	~Channel() = default;

	static Channel create(std::string_view name, std::size_t capacity = wire::default_capacity)
	{
		detail::validate_name(name);
		std::string n(name);
		Channel c;
		c.name_ = n;
		c.tx_ = Queue::create(n + std::string(wire::creator_to_opener_suffix), capacity);
		try {
			c.rx_ = Queue::create(n + std::string(wire::opener_to_creator_suffix), capacity);
		} catch (...) {
			c.tx_.close();
			c.tx_.unlink();
			throw;
		}
		return c;
	}

	static Channel open(std::string_view name)
	{
		detail::validate_name(name);
		std::string n(name);
		Channel c;
		c.name_ = n;
		c.tx_ = Queue::open(n + std::string(wire::opener_to_creator_suffix));
		c.rx_ = Queue::open(n + std::string(wire::creator_to_opener_suffix));
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

	// close releases this process's handles on both directions.
	void close()
	{
		std::exception_ptr first;
		try {
			tx_.close();
		} catch (...) {
			first = std::current_exception();
		}
		try {
			rx_.close();
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
	std::string name_;
	Queue tx_;
	Queue rx_;
};

} // namespace goipc

#endif
