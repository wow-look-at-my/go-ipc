// Service: typed request and reply over a channel per client (spec/service.md).
#ifndef GOIPC_SERVICE_HPP
#define GOIPC_SERVICE_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <dirent.h>

#include "channel.hpp"
#include "detail.hpp"
#include "error.hpp"
#include "names.hpp"
#include "queue.hpp"

namespace goipc {

// call_error is the error a service handler raised, carried to the client.
class call_error : public error {
public:
	explicit call_error(std::string message)
		: error(errc::call, "goipc: call failed: " + message), message_(std::move(message)) {}

	const std::string &message() const noexcept { return message_; }

private:
	std::string message_;
};

// reply is what a handler returns: the reply's type and payload.
struct reply {
	std::uint32_t type = 0;
	std::vector<std::byte> payload;
};

// encode_reply turns an ipcgen message into a reply.
template <class M>
reply encode_reply(const M &m)
{
	reply r{M::type_id, std::vector<std::byte>(m.size())};
	m.encode(r.payload);
	return r;
}

class Session;

// A handler answers one request. It raises to report an error; the what() of
// the exception reaches the client as a call_error.
using handler = std::function<reply(Session &, std::uint32_t type, std::span<const std::byte> payload)>;
// A gone_fn runs once after a client exits or closes.
using gone_fn = std::function<void(Session &)>;

// typed_handler builds a handler over ipcgen messages. f(Session &, const Req &)
// is called with the request decoded as whichever of Reqs has its type ID,
// and returns the reply message.
template <class... Reqs, class F>
handler typed_handler(F f)
{
	return [f = std::move(f)](Session &s, std::uint32_t type, std::span<const std::byte> payload) -> reply {
		std::optional<reply> out;
		auto one = [&]<class Req>() {
			if (type != Req::type_id)
				return false;
			std::optional<Req> req = Req::decode(payload);
			if (!req)
				throw error(errc::corrupt, "goipc: a request of type " + std::to_string(type) + " does not decode");
			out = encode_reply(f(s, *req));
			return true;
		};
		if (!(one.template operator()<Reqs>() || ...))
			throw error(errc::invalid, "goipc: no message has type ID " + std::to_string(type));
		return *out;
	};
}

namespace detail {

inline void put_u64(std::byte *b, std::uint64_t v) noexcept { std::memcpy(b, &v, sizeof v); }

inline std::uint64_t get_u64(const std::byte *b) noexcept
{
	std::uint64_t v;
	std::memcpy(&v, b, sizeof v);
	return v;
}

// frame prefixes payload with its sequence number.
inline std::vector<std::byte> frame(std::uint64_t seq, std::span<const std::byte> payload)
{
	std::vector<std::byte> b(wire::service_sequence_size + payload.size());
	put_u64(b.data(), seq);
	if (!payload.empty())
		std::memcpy(b.data() + wire::service_sequence_size, payload.data(), payload.size());
	return b;
}

struct service_state;

} // namespace detail

// Session is one client of a Service, as the handler sees it.
class Session {
public:
	// ordinal is the number the service gave this client.
	std::uint64_t ordinal() const noexcept { return ordinal_; }

private:
	friend struct detail::service_state;
	friend class Service;
	Session(Channel ch, std::uint64_t ordinal) : ch_(std::move(ch)), ordinal_(ordinal) {}

	Channel ch_;
	std::uint64_t ordinal_;
	std::thread thread_;
};

namespace detail {

struct service_state : std::enable_shared_from_this<service_state> {
	std::string name;
	handler handle;
	gone_fn gone;
	Queue reg;
	std::thread registry;

	std::mutex mu;
	bool closed = false;
	std::uint64_t next = 0;
	// live holds the sessions being served; done holds those whose thread has ended and awaits a join.
	std::list<std::shared_ptr<Session>> live;
	std::list<std::shared_ptr<Session>> done;

	// run adopts each client that knocks until the queue closes.
	void run()
	{
		std::vector<std::byte> buf(reg.max_message_size());
		for (;;) {
			received r{};
			try {
				r = reg.recv(buf);
			} catch (const error &) {
				return;
			}
			std::string_view id(reinterpret_cast<const char *>(buf.data()), r.size);
			if (r.type != wire::service_type_knock || !parse_incarnation(id))
				continue;
			adopt(std::string(id));
		}
	}

	// scan_clients adopts every client channel that exists under the service
	// name. A client that created its channel before the service existed is
	// found here.
	void scan_clients()
	{
		std::string prefix = std::string(wire::name_prefix) + name + std::string(wire::service_client_prefix);
		std::string suffix = std::string(wire::opener_to_creator_suffix) + std::string(wire::name_suffix);
		DIR *d = ::opendir(std::string(wire::runtime_dir).c_str());
		if (d == nullptr)
			return;
		std::vector<std::string> ids;
		while (dirent *e = ::readdir(d)) {
			std::string_view n(e->d_name);
			if (!n.starts_with(prefix) || !n.ends_with(suffix) || n.size() < prefix.size() + suffix.size())
				continue;
			std::string_view id = n.substr(prefix.size(), n.size() - prefix.size() - suffix.size());
			if (parse_incarnation(id))
				ids.emplace_back(id);
		}
		::closedir(d);
		for (const auto &id : ids)
			adopt(id);
	}

	// adopt opens a client's channel and serves it. An open that fails means
	// the channel was adopted already, its client has gone, or the client has
	// not finished creating it and knocks when it has.
	void adopt(const std::string &id)
	{
		reap();
		Channel ch;
		try {
			ch = Channel::open(name + std::string(wire::service_client_prefix) + id);
		} catch (const error &) {
			return;
		}
		std::shared_ptr<Session> sess;
		{
			std::lock_guard l(mu);
			if (closed)
				return;
			sess = std::shared_ptr<Session>(new Session(std::move(ch), next++));
			live.push_back(sess);
		}
		std::byte hello[wire::service_sequence_size];
		put_u64(hello, sess->ordinal());
		try {
			sess->ch_.send(wire::service_type_hello, hello);
			sess->thread_ = std::thread([self = shared_from_this(), sess] { self->serve(*sess); });
		} catch (...) {
			std::lock_guard l(mu);
			live.remove(sess);
		}
	}

	// reap joins the threads of sessions that have ended.
	void reap()
	{
		std::list<std::shared_ptr<Session>> ended;
		{
			std::lock_guard l(mu);
			ended.swap(done);
		}
		for (auto &sess : ended)
			sess->thread_.join();
	}

	// serve answers one client until it goes or the service closes. A client
	// that went has its channel removed before the handler hears of it.
	void serve(Session &sess)
	{
		std::vector<std::byte> buf(sess.ch_.max_message_size());
		for (;;) {
			received r{};
			try {
				r = sess.ch_.recv(buf);
			} catch (const error &e) {
				if (e.value() == errc::peer_gone) {
					try {
						sess.ch_.close();
						sess.ch_.unlink();
					} catch (...) {
					}
					if (gone)
						gone(sess);
				}
				break;
			}
			try {
				answer(sess, r.type, std::span<const std::byte>(buf).first(r.size));
			} catch (...) {
				break;
			}
		}
		std::lock_guard l(mu);
		for (auto it = live.begin(); it != live.end(); ++it) {
			if (it->get() == &sess) {
				done.splice(done.end(), live, it);
				break;
			}
		}
	}

	// answer runs the handler on one request and sends its reply.
	void answer(Session &sess, std::uint32_t type, std::span<const std::byte> msg)
	{
		if (msg.size() < wire::service_sequence_size) {
			send_error(sess, 0, "goipc: request is shorter than its sequence number");
			return;
		}
		std::uint64_t seq = get_u64(msg.data());
		if (type >= wire::service_reserved_type_min) {
			send_error(sess, seq, "goipc: request type " + std::to_string(type) + " is reserved");
			return;
		}
		reply out;
		try {
			out = handle(sess, type, msg.subspan(wire::service_sequence_size));
		} catch (const std::exception &e) {
			send_error(sess, seq, e.what());
			return;
		}
		if (out.type >= wire::service_reserved_type_min) {
			send_error(sess, seq, "goipc: reply type " + std::to_string(out.type) + " is reserved");
			return;
		}
		sess.ch_.send(out.type, frame(seq, out.payload));
	}

	void send_error(Session &sess, std::uint64_t seq, std::string_view message)
	{
		sess.ch_.send(wire::service_type_error, frame(seq, std::as_bytes(std::span(message.data(), message.size()))));
	}
};

} // namespace detail

// Service answers calls under a name. serve starts it, and it runs until
// close.
class Service {
public:
	Service() = default;
	Service(Service &&) noexcept = default;
	Service &operator=(Service &&o) noexcept
	{
		if (this != &o) {
			close();
			s_ = std::move(o.s_);
		}
		return *this;
	}
	~Service()
	{
		try {
			close();
		} catch (...) {
		}
	}

	// serve creates the service and returns once a client can reach it. It
	// raises errc::in_use while another process serves the name. Every
	// client that created its connection before this call is adopted now;
	// every later one is adopted when it knocks. Each client's handler runs
	// on a thread of its own, one call at a time.
	static Service serve(std::string_view name, handler h, gone_fn gone = {}, std::size_t capacity = wire::default_capacity)
	{
		detail::validate_name(name);
		if (!h)
			throw error(errc::invalid, "goipc: serve needs a handler");
		auto s = std::make_shared<detail::service_state>();
		s->name = std::string(name);
		s->handle = std::move(h);
		s->gone = std::move(gone);
		s->reg = Queue::create(s->name + std::string(wire::service_registry_suffix), capacity);
		// The queue is published, so a client whose channel the scan misses opens the queue and knocks.
		s->scan_clients();
		try {
			s->registry = std::thread([s] { s->run(); });
		} catch (...) {
			Service failed(s);
			failed.close();
			throw;
		}
		return Service(s);
	}

	const std::string &name() const { return state().name; }

	// close stops the service. Clients parked in a call find it gone. It
	// waits for every handler to return, then removes the name.
	void close()
	{
		if (!s_)
			return;
		auto &s = *s_;
		{
			std::lock_guard l(s.mu);
			if (s.closed)
				return;
			s.closed = true;
		}
		std::exception_ptr first;
		try {
			s.reg.close();
		} catch (...) {
			first = std::current_exception();
		}
		if (s.registry.joinable())
			s.registry.join();
		// Each session thread wakes from its receive, finds the channel closed
		// and leaves. A handler that is running finishes its call first.
		std::list<std::shared_ptr<Session>> all;
		{
			std::lock_guard l(s.mu);
			for (auto &sess : s.live) {
				try {
					sess->ch_.close();
				} catch (...) {
				}
			}
			all = s.live;
			all.splice(all.end(), s.done);
		}
		for (auto &sess : all) {
			if (sess->thread_.joinable())
				sess->thread_.join();
		}
		{
			std::lock_guard l(s.mu);
			s.live.clear();
			s.done.clear();
		}
		try {
			s.reg.unlink();
		} catch (...) {
			if (!first)
				first = std::current_exception();
		}
		if (first)
			std::rethrow_exception(first);
	}

private:
	explicit Service(std::shared_ptr<detail::service_state> s) : s_(std::move(s)) {}

	detail::service_state &state() const
	{
		if (!s_)
			throw error(errc::closed, "goipc: service has no state");
		return *s_;
	}

	std::shared_ptr<detail::service_state> s_;
};

// Client is a connection to a Service. One call is in flight at a time;
// concurrent calls wait for each other.
class Client {
public:
	Client() = default;
	Client(Client &&) noexcept = default;
	Client &operator=(Client &&o) noexcept
	{
		if (this != &o) {
			close();
			s_ = std::move(o.s_);
		}
		return *this;
	}
	~Client()
	{
		try {
			close();
		} catch (...) {
		}
	}

	// connect connects to the named service. A service that does not exist
	// yet is waited for, parked in the kernel, until the timeout ends. A
	// service that exited raises errc::peer_gone from the first call that
	// finds it gone.
	static Client connect(std::string_view name, timeout t = forever, std::size_t capacity = wire::default_capacity)
	{
		detail::validate_name(name);
		std::string id = detail::new_incarnation();
		auto s = std::make_shared<state>();
		s->name = std::string(name);
		s->ch = Channel::create(s->name + std::string(wire::service_client_prefix) + id, capacity);
		detail::deadline d(t);
		try {
			knock(s->name, id, d);
			std::vector<std::byte> hello(s->ch.max_message_size());
			received r = s->ch.recv(hello, d.left());
			if (r.type != wire::service_type_hello || r.size != wire::service_sequence_size)
				throw error(errc::corrupt, "goipc: the service sent type " + std::to_string(r.type) + " before hello");
			s->ordinal = detail::get_u64(hello.data());
		} catch (...) {
			try {
				s->ch.close();
				s->ch.unlink();
			} catch (...) {
			}
			throw;
		}
		s->seq = wire::service_first_sequence - 1;
		s->buf.resize(s->ch.max_message_size());
		return Client(std::move(s));
	}

	const std::string &name() const { return state_().name; }
	// ordinal is the number the service gave this connection.
	std::uint64_t ordinal() const { return state_().ordinal; }
	// max_payload_size is the largest request or reply payload a call carries.
	std::size_t max_payload_size() const { return state_().ch.max_message_size() - wire::service_sequence_size; }

	// call sends one request and returns the reply. A handler error raises a
	// call_error. A service that exited raises errc::peer_gone.
	message call(std::uint32_t type, std::span<const std::byte> payload, timeout t = forever)
	{
		auto &s = state_();
		if (type >= wire::service_reserved_type_min)
			throw error(errc::reserved_type, "goipc: request type " + std::to_string(type) + " is reserved");
		if (payload.size() > max_payload_size())
			throw error(errc::too_large, "goipc: request of " + std::to_string(payload.size()) + " bytes exceeds the maximum");
		detail::deadline d(t);
		std::lock_guard l(s.mu);
		std::uint64_t seq = ++s.seq;
		s.ch.send(type, detail::frame(seq, payload), d.left());
		for (;;) {
			received r = s.ch.recv(s.buf, d.left());
			if (r.size < wire::service_sequence_size)
				throw error(errc::corrupt, "goipc: a reply is shorter than its sequence number");
			std::uint64_t rseq = detail::get_u64(s.buf.data());
			// A reply below the sequence answers a call this client gave up on.
			if (rseq < seq)
				continue;
			if (rseq > seq)
				throw error(errc::corrupt, "goipc: reply sequence " + std::to_string(rseq) + " is ahead of call " + std::to_string(seq));
			auto body = std::span<const std::byte>(s.buf).subspan(wire::service_sequence_size, r.size - wire::service_sequence_size);
			if (r.type == wire::service_type_error)
				throw call_error(std::string(reinterpret_cast<const char *>(body.data()), body.size()));
			if (r.type >= wire::service_reserved_type_min)
				throw error(errc::corrupt, "goipc: reply type " + std::to_string(r.type) + " is reserved");
			return message{r.type, std::vector<std::byte>(body.begin(), body.end())};
		}
	}

	// call<Rep>(req) sends an ipcgen message and decodes the reply as Rep. A
	// reply of another type raises errc::invalid.
	template <class Rep, class Req>
	Rep call(const Req &req, timeout t = forever)
	{
		std::vector<std::byte> payload(req.size());
		req.encode(payload);
		message m = call(Req::type_id, payload, t);
		if (m.type != Rep::type_id)
			throw error(errc::invalid, "goipc: call of type " + std::to_string(Req::type_id) + " got a reply of type " +
							std::to_string(m.type) + ", want " + std::to_string(Rep::type_id));
		std::optional<Rep> rep = Rep::decode(m.payload);
		if (!rep)
			throw error(errc::corrupt, "goipc: a reply of type " + std::to_string(m.type) + " does not decode");
		return *rep;
	}

	// close ends the connection and removes its name. The service sees the client go.
	void close()
	{
		if (!s_ || s_->closed.exchange(true))
			return;
		std::exception_ptr first;
		try {
			s_->ch.close();
		} catch (...) {
			first = std::current_exception();
		}
		try {
			s_->ch.unlink();
		} catch (...) {
			if (!first)
				first = std::current_exception();
		}
		if (first)
			std::rethrow_exception(first);
	}

private:
	struct state {
		std::string name;
		std::uint64_t ordinal = 0;
		Channel ch;
		std::mutex mu;
		std::uint64_t seq = 0;
		std::vector<std::byte> buf;
		std::atomic<bool> closed{false};
	};

	explicit Client(std::shared_ptr<state> s) : s_(std::move(s)) {}

	state &state_() const
	{
		if (!s_)
			throw error(errc::closed, "goipc: client has no state");
		return *s_;
	}

	// knock tells a running service about a new client. A service that is
	// not up yet finds the client's channel when it starts, so a failure here
	// costs nothing.
	static void knock(const std::string &name, const std::string &id, const detail::deadline &d)
	{
		try {
			Queue q = Queue::open(name + std::string(wire::service_registry_suffix));
			q.send(wire::service_type_knock, std::as_bytes(std::span(id.data(), id.size())), d.left());
			q.close();
		} catch (const error &) {
		}
	}

	std::shared_ptr<state> s_;
};

} // namespace goipc

#endif
