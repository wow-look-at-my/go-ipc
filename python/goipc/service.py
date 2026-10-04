"""The service layer of spec/service.md: a server answers calls from any number of clients.

A client connects by name and calls:

	client = goipc.service.connect("policy")
	reply_type, reply = client.call(1, b"payload", timeout=5.0)

A server names a handler and serves:

	def handle(session, type_, payload):
		return 2, payload

	svc = goipc.service.serve("policy", handle)

The handler of each client runs on a thread of its own. An exception it
raises reaches the client as CallError.
"""

from __future__ import annotations

import os
import secrets
import struct
import threading
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

from . import wire
from ._endpoints import Channel, Queue
from ._util import Endpoint
from .errors import CallError, Closed, Corrupt, IpcError, PeerGone, ReservedType

Handler = Callable[["Session", int, bytes], Tuple[int, bytes]]
GoneHandler = Callable[["Session"], None]

_SEQ = struct.Struct("<Q")

# A call carries a whole payload, such as a prompt's token array, so a client channel fits a whole context.
SERVICE_CAPACITY = 1 << 24


def _registry_name(name: str) -> str:
	return name + wire.SERVICE_REGISTRY_SUFFIX


def _channel_name(name: str, client_id: str) -> str:
	return name + wire.SERVICE_CLIENT_PREFIX + client_id


def _is_client_id(text: str) -> bool:
	if len(text) != wire.SERVICE_CLIENT_ID_HEX_DIGITS:
		return False
	return all(c in "0123456789abcdef" for c in text)


def scan_clients(name: str) -> list:
	"""Returns the ids of the client channels that exist under the service name."""
	prefix = "go-ipc-" + name + wire.SERVICE_CLIENT_PREFIX
	suffix = wire.OPENER_TO_CREATOR_SUFFIX + ".name"
	try:
		entries = os.listdir(wire.RUNTIME_DIR)
	except OSError:
		return []
	out = []
	for entry in entries:
		if not entry.startswith(prefix) or not entry.endswith(suffix):
			continue
		client_id = entry[len(prefix):len(entry) - len(suffix)]
		if _is_client_id(client_id):
			out.append(client_id)
	return out


class Session:
	"""One client of a Service, as the handler sees it. ordinal numbers the clients from 0."""

	def __init__(self, ordinal: int, client_id: str, channel: Channel) -> None:
		self.ordinal = ordinal
		self._id = client_id
		self._channel = channel


class Service(Endpoint):
	"""A running service. Use serve. failure holds the error that stopped it, if one did."""

	def __init__(self, name: str, handler: Handler, on_gone: Optional[GoneHandler], capacity: int) -> None:
		self.name = name
		self._handler = handler
		self._on_gone = on_gone
		self._registry = Queue.create(_registry_name(name), capacity)
		self._lock = threading.Lock()
		self._next = 0
		self._sessions: Dict[int, Session] = {}
		self._closing = False
		self.failure: Optional[IpcError] = None
		for client_id in scan_clients(name):
			self._adopt(client_id)
		self._knocks = threading.Thread(target=self._run, name="goipc service " + name, daemon=True)
		self._knocks.start()

	def _run(self) -> None:
		try:
			while True:
				type_, payload = self._registry.recv()
				if type_ != wire.SERVICE_TYPE_KNOCK:
					continue
				client_id = payload.decode("ascii", "replace")
				if _is_client_id(client_id):
					self._adopt(client_id)
		except Closed:
			return
		except IpcError as failure:
			self.failure = failure

	def _adopt(self, client_id: str) -> None:
		"""Opens a client's channel and serves it. An open that fails is a client adopted already, gone, or not finished."""
		try:
			channel = Channel.open(_channel_name(self.name, client_id))
		except IpcError:
			return
		with self._lock:
			session = Session(self._next, client_id, channel)
			self._next += 1
			self._sessions[session.ordinal] = session
		try:
			channel.send(_SEQ.pack(session.ordinal), type=wire.SERVICE_TYPE_HELLO)
		except IpcError:
			self._forget(session)
			return
		thread = threading.Thread(target=self._serve, args=(session,), name="goipc session %d" % session.ordinal, daemon=True)
		thread.start()

	def _forget(self, session: Session) -> None:
		with self._lock:
			self._sessions.pop(session.ordinal, None)
		session._channel.close()

	def _serve(self, session: Session) -> None:
		channel = session._channel
		try:
			while True:
				try:
					type_, payload = channel.recv()
				except PeerGone:
					channel.close()
					channel.unlink()
					if self._on_gone is not None:
						self._on_gone(session)
					return
				self._answer(session, type_, payload)
		except IpcError:
			return
		finally:
			self._forget(session)

	def _answer(self, session: Session, type_: int, msg: bytes) -> None:
		channel = session._channel
		if len(msg) < wire.SERVICE_SEQUENCE_SIZE:
			channel.send(_SEQ.pack(0) + b"goipc: request is shorter than its sequence number", type=wire.SERVICE_TYPE_ERROR)
			return
		seq = msg[:wire.SERVICE_SEQUENCE_SIZE]
		if type_ >= wire.SERVICE_RESERVED_TYPE_MIN:
			channel.send(seq + ("goipc: request type %d is reserved" % type_).encode(), type=wire.SERVICE_TYPE_ERROR)
			return
		try:
			reply_type, reply = self._handler(session, type_, msg[wire.SERVICE_SEQUENCE_SIZE:])
		except Exception as exc:
			channel.send(seq + str(exc).encode("utf-8"), type=wire.SERVICE_TYPE_ERROR)
			return
		if reply_type >= wire.SERVICE_RESERVED_TYPE_MIN:
			channel.send(seq + ("goipc: reply type %d is reserved" % reply_type).encode(), type=wire.SERVICE_TYPE_ERROR)
			return
		channel.send(seq + bytes(reply), type=reply_type)

	def close(self) -> None:
		"""Stops the service. Every client parked in a call finds it gone. A handler still running is not waited for."""
		with self._lock:
			if self._closing:
				return
			self._closing = True
		self._registry.close()
		self._knocks.join()
		with self._lock:
			sessions = list(self._sessions.values())
		for session in sessions:
			session._channel.close()
		self._registry.unlink()

	def wait(self, timeout: Optional[float] = None) -> None:
		"""Blocks until the service stops. Raises the error that stopped it, if one did."""
		self._knocks.join(timeout)
		if self.failure is not None:
			raise self.failure


def serve(
	name: str,
	handler: Handler,
	*,
	on_gone: Optional[GoneHandler] = None,
	capacity: int = wire.DEFAULT_CAPACITY,
) -> Service:
	"""Serves the named service and returns once a client can reach it.

	handler(session, type, payload) returns (reply_type, reply). An exception
	it raises reaches the client as CallError with the exception's text.
	on_gone(session) runs after a client exits or closes. Raises InUse while
	another process serves the name.
	"""
	wire.validate_name(name)
	return Service(name, handler, on_gone, capacity)


class Client(Endpoint):
	"""A connection to a service. One call is in flight at a time. Use connect."""

	def __init__(self, name: str, timeout: Optional[float], capacity: int, messages: Optional[Mapping[int, Any]]) -> None:
		self.name = name
		self.timeout = timeout
		self._messages = messages
		self._lock = threading.Lock()
		self._seq = wire.SERVICE_FIRST_SEQUENCE - 1
		client_id = secrets.token_hex(wire.SERVICE_CLIENT_ID_HEX_DIGITS // 2)
		self._channel = Channel.create(_channel_name(name, client_id), capacity)
		try:
			_knock(name, client_id, timeout)
			type_, payload = self._channel.recv(timeout)
			if type_ != wire.SERVICE_TYPE_HELLO or len(payload) != wire.SERVICE_SEQUENCE_SIZE:
				raise Corrupt("goipc: the service sent type %d before hello" % type_)
		except BaseException:
			self._channel.close()
			self._channel.unlink()
			raise
		self.ordinal = _SEQ.unpack(payload)[0]
		self.max_payload_size = self._channel.max_message_size - wire.SERVICE_SEQUENCE_SIZE

	def call(self, type: int, payload: Any = b"", timeout: Optional[float] = None) -> Tuple[int, bytes]:
		"""Sends one request and returns (reply_type, reply).

		timeout None means the client's timeout. A handler error raises
		CallError. A service that exited raises PeerGone.
		"""
		if type >= wire.SERVICE_RESERVED_TYPE_MIN:
			raise ReservedType("goipc: call type %d is reserved" % type)
		if timeout is None:
			timeout = self.timeout
		with self._lock:
			self._seq += 1
			seq = self._seq
			self._channel.send(_SEQ.pack(seq) + bytes(payload), type=type, timeout=timeout)
			while True:
				reply_type, msg = self._channel.recv(timeout)
				if len(msg) < wire.SERVICE_SEQUENCE_SIZE:
					raise Corrupt("goipc: a reply is shorter than its sequence number")
				rseq = _SEQ.unpack_from(msg)[0]
				if rseq < seq:
					continue
				if rseq > seq:
					raise Corrupt("goipc: reply sequence %d is ahead of call %d" % (rseq, seq))
				body = msg[wire.SERVICE_SEQUENCE_SIZE:]
				if reply_type == wire.SERVICE_TYPE_ERROR:
					raise CallError(body.decode("utf-8", "replace"))
				if reply_type >= wire.SERVICE_RESERVED_TYPE_MIN:
					raise Corrupt("goipc: reply type %d is reserved" % reply_type)
				return reply_type, body

	def call_typed(self, message: Any, timeout: Optional[float] = None) -> Any:
		"""Sends an ipcgen message and returns the decoded reply.

		The client's messages mapping, the generated MESSAGES, picks the reply
		class by type ID. A reply of a type it lacks raises Corrupt.
		"""
		if self._messages is None:
			raise TypeError("goipc: call_typed needs the messages mapping given to connect")
		reply_type, body = self.call(message.TYPE_ID, message.encode(), timeout)
		cls = self._messages.get(reply_type)
		if cls is None:
			raise Corrupt("goipc: no message has type ID %d" % reply_type)
		return cls.decode(body)

	def close(self) -> None:
		"""Ends the connection and removes its name. The service sees the client go."""
		try:
			self._channel.close()
		finally:
			self._channel.unlink()


def _knock(name: str, client_id: str, timeout: Optional[float]) -> None:
	"""Tells a running service about a new client. A service that is not up finds the channel when it starts."""
	try:
		q = Queue.open(_registry_name(name))
	except IpcError:
		return
	try:
		q.send(client_id.encode("ascii"), type=wire.SERVICE_TYPE_KNOCK, timeout=timeout)
	except IpcError:
		pass
	finally:
		q.close()


def connect(
	name: str,
	timeout: Optional[float] = None,
	*,
	capacity: int = SERVICE_CAPACITY,
	messages: Optional[Mapping[int, Any]] = None,
) -> Client:
	"""Connects to the named service.

	A service that does not exist yet is waited for, parked in the kernel,
	until timeout. timeout also bounds each call that names none. messages is
	the generated MESSAGES mapping, for call_typed.
	"""
	wire.validate_name(name)
	return Client(name, timeout, capacity, messages)
