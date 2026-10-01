"""The named endpoints over libgoipc handles: Event, Queue, Channel and Conn."""

from __future__ import annotations

import ctypes
import io
from typing import Any, Callable, Optional, Tuple

from . import _util
from ._lib import lib
from .errors import EOF, error_for
from .ring import Claim, ReadFunc, Ring, _limit, claim, read, recv
from .wire import DEFAULT_CAPACITY

_RecvCall = Callable[[Any, int, Any, Any], int]


class _Handle(_util.Endpoint):
	"""Owns a C handle: destroy on collection, close that tolerates a repeat."""

	_ptr = 0
	_fin: Any = None
	_buf: Any = None
	max_message_size = 0

	def _own(self, ptr: int, close: Callable[[Any], int], destroy: Callable[[Any], None]) -> None:
		self._ptr = ptr
		self._fin = _util.own(self, ptr, close, destroy)

	def _close(self, fn: Callable[[Any], int]) -> None:
		if self._fin is None or self._fin.alive:
			_util.check_close(fn(self._ptr))

	def _recv_copy(self, call: _RecvCall) -> Tuple[int, bytes]:
		buf = self._buf
		if buf is None:
			buf = (ctypes.c_char * max(self.max_message_size, 1))()
			self._buf = buf
		type_, n = recv(call, buf)
		return type_, ctypes.string_at(buf, n)


class Event(_Handle):
	"""A counting wake channel shared by name between processes."""

	def __init__(self, ptr: int, name: str) -> None:
		"""Use create or open."""
		cdll = lib()
		self._own(ptr, cdll.goipc_event_close, cdll.goipc_event_destroy)
		self.name = name

	@classmethod
	def create(cls, name: str) -> "Event":
		"""Creates the event, replacing any file left under the name."""
		return cls(_util.open_handle(lib().goipc_event_create, _util.encode_name(name)), name)

	@classmethod
	def open(cls, name: str) -> "Event":
		"""Opens an event that another side created."""
		return cls(_util.open_handle(lib().goipc_event_open, _util.encode_name(name)), name)

	def signal(self, n: int = 1) -> None:
		"""Releases up to n waiters. It never blocks."""
		_util.check(lib().goipc_event_signal(self._ptr, n))

	def wait(self, timeout: Optional[float] = None) -> None:
		"""Blocks until a signal arrives. Raises Timeout or Closed."""
		_util.check(lib().goipc_event_wait(self._ptr, _util.timeout_ns(timeout)))

	def close(self) -> None:
		"""Releases every waiter. A second close does nothing."""
		self._close(lib().goipc_event_close)

	def unlink(self) -> None:
		"""Removes the name. A missing file is not an error."""
		_util.check(lib().goipc_event_unlink(self._ptr))

	def __repr__(self) -> str:
		return "<goipc.Event %r>" % (self.name,)


class Queue(_Handle):
	"""A named queue. Any number of threads and processes send; one thread receives.

	Attributes: name, capacity, max_message_size.
	"""

	def __init__(self, ptr: int, name: str, owner: Any = None) -> None:
		"""Use create or open. A queue with an owner lives inside the owner's handle."""
		cdll = lib()
		if owner is None:
			self._own(ptr, cdll.goipc_queue_close, cdll.goipc_queue_destroy)
		else:
			self._ptr = ptr
		self._owner = owner
		self.name = name
		self.capacity = int(cdll.goipc_queue_capacity(ptr))
		self.max_message_size = int(cdll.goipc_queue_max_message_size(ptr))

	@classmethod
	def create(cls, name: str, capacity: int = DEFAULT_CAPACITY) -> "Queue":
		"""Creates the queue. capacity is a power of 2 of at least 4096."""
		return cls(_util.open_handle(lib().goipc_queue_create, _util.encode_name(name), capacity), name)

	@classmethod
	def open(cls, name: str) -> "Queue":
		"""Opens a queue that another side created."""
		return cls(_util.open_handle(lib().goipc_queue_open, _util.encode_name(name)), name)

	@property
	def ring(self) -> Ring:
		"""The ring in the shared segment. It has no close guard: do not use it after close."""
		return Ring(None, lib().goipc_queue_ring(self._ptr).contents, self)

	def try_send(self, payload: Any, type: int = 0) -> None:
		"""Sends without waiting. Raises Full when the ring has no room."""
		src, n, keep = _util.readable(payload)
		_util.check(lib().goipc_queue_try_send(self._ptr, type, src, n))

	def send(self, payload: Any, type: int = 0, timeout: Optional[float] = None) -> None:
		"""Sends, waiting for room. Raises Timeout or Closed."""
		src, n, keep = _util.readable(payload)
		_util.check(lib().goipc_queue_send(self._ptr, type, src, n, _util.timeout_ns(timeout)))

	def claim(self, type: int, length: int, timeout: Optional[float] = None) -> Claim:
		"""Reserves a record to fill in place, waiting for room."""
		cdll = lib()
		q = self._ptr
		ns = _util.timeout_ns(timeout)
		return claim(
			self,
			lambda out: cdll.goipc_queue_claim(q, type, length, ns, out),
			lambda c: cdll.goipc_queue_commit(q, c),
			lambda c: cdll.goipc_queue_abort(q, c),
		)

	def try_recv(self) -> Tuple[int, bytes]:
		"""Returns (type, payload) of the next message. Raises Empty when none is ready."""
		cdll = lib()
		q = self._ptr
		return self._recv_copy(lambda d, n, t, ln: cdll.goipc_queue_try_recv(q, d, n, t, ln))

	def recv(self, timeout: Optional[float] = None) -> Tuple[int, bytes]:
		"""Waits for the next message and returns (type, payload)."""
		return self._recv_copy(self._recv_call(timeout))

	def recv_into(self, buffer: Any, timeout: Optional[float] = None) -> Tuple[int, int]:
		"""Waits for the next message, copies it into buffer and returns (type, nbytes).

		Raises BufferTooSmall, with needed set, when buffer cannot hold it. The
		message then stays queued.
		"""
		return recv(self._recv_call(timeout), buffer)

	def _recv_call(self, timeout: Optional[float]) -> _RecvCall:
		cdll = lib()
		q = self._ptr
		ns = _util.timeout_ns(timeout)
		return lambda d, n, t, ln: cdll.goipc_queue_recv(q, d, n, t, ln, ns)

	def read_batch(self, limit: Optional[int], fn: ReadFunc, timeout: Optional[float] = None) -> int:
		"""Waits for a message, then passes up to limit ready messages to fn(type, payload).

		The payload aliases shared memory and is valid only during the call.
		limit None means no limit. Returns the count passed.
		"""
		cdll = lib()
		q = self._ptr
		lim = _limit(limit)
		ns = _util.timeout_ns(timeout)
		return read(lambda cfunc: cdll.goipc_queue_read_batch(q, lim, cfunc, None, ns), fn)

	def close(self) -> None:
		"""Releases this process's handles and every blocked caller. A second close does nothing."""
		self._close(lib().goipc_queue_close)

	def unlink(self) -> None:
		"""Removes the segment and both event files."""
		_util.check(lib().goipc_queue_unlink(self._ptr))

	def __repr__(self) -> str:
		return "<goipc.Queue %r capacity=%d>" % (self.name, self.capacity)


class Channel(_Handle):
	"""A pair of queues with opposite directions. Attributes: name, tx, rx, max_message_size."""

	def __init__(self, ptr: int, name: str) -> None:
		"""Use create or open."""
		cdll = lib()
		self._own(ptr, cdll.goipc_channel_close, cdll.goipc_channel_destroy)
		self.name = name
		self.tx = Queue(int(cdll.goipc_channel_tx(ptr)), name, owner=self)
		self.rx = Queue(int(cdll.goipc_channel_rx(ptr)), name, owner=self)
		self.max_message_size = int(cdll.goipc_channel_max_message_size(ptr))

	@classmethod
	def create(cls, name: str, capacity: int = DEFAULT_CAPACITY) -> "Channel":
		"""Creates both queues. This side sends on name.c2o and receives on name.o2c."""
		return cls(_util.open_handle(lib().goipc_channel_create, _util.encode_name(name), capacity), name)

	@classmethod
	def open(cls, name: str) -> "Channel":
		"""Opens both queues with the roles swapped."""
		return cls(_util.open_handle(lib().goipc_channel_open, _util.encode_name(name)), name)

	def send(self, payload: Any, type: int = 0, timeout: Optional[float] = None) -> None:
		"""Sends on tx, waiting for room."""
		src, n, keep = _util.readable(payload)
		_util.check(lib().goipc_channel_send(self._ptr, type, src, n, _util.timeout_ns(timeout)))

	def try_send(self, payload: Any, type: int = 0) -> None:
		self.tx.try_send(payload, type)

	def claim(self, type: int, length: int, timeout: Optional[float] = None) -> Claim:
		return self.tx.claim(type, length, timeout)

	def recv(self, timeout: Optional[float] = None) -> Tuple[int, bytes]:
		"""Waits for the next message on rx and returns (type, payload)."""
		return self._recv_copy(self._recv_call(timeout))

	def recv_into(self, buffer: Any, timeout: Optional[float] = None) -> Tuple[int, int]:
		return recv(self._recv_call(timeout), buffer)

	def _recv_call(self, timeout: Optional[float]) -> _RecvCall:
		cdll = lib()
		c = self._ptr
		ns = _util.timeout_ns(timeout)
		return lambda d, n, t, ln: cdll.goipc_channel_recv(c, d, n, t, ln, ns)

	def try_recv(self) -> Tuple[int, bytes]:
		return self.rx.try_recv()

	def read_batch(self, limit: Optional[int], fn: ReadFunc, timeout: Optional[float] = None) -> int:
		return self.rx.read_batch(limit, fn, timeout)

	def close(self) -> None:
		"""Closes both directions and releases every blocked caller."""
		self._close(lib().goipc_channel_close)

	def unlink(self) -> None:
		"""Removes both queues' files."""
		_util.check(lib().goipc_channel_unlink(self._ptr))

	def __repr__(self) -> str:
		return "<goipc.Channel %r>" % (self.name,)


class Conn(io.RawIOBase):
	"""A byte stream over a channel, usable with io.BufferedReader and io.BufferedWriter.

	timeout, in float seconds or None for forever, bounds each read and write.
	A read returns b"" (readinto returns 0) once the peer has ended the stream.
	"""

	_fin: Any = None

	def __init__(self, ptr: int, name: str) -> None:
		"""Use listen or dial."""
		super().__init__()
		cdll = lib()
		self._ptr = ptr
		self._fin = _util.own(self, ptr, cdll.goipc_conn_close, cdll.goipc_conn_destroy)
		self.name = name
		self.timeout: Optional[float] = None

	@classmethod
	def listen(cls, name: str, capacity: int = DEFAULT_CAPACITY) -> "Conn":
		"""Creates the endpoint. The peer calls dial with the same name."""
		return cls(_util.open_handle(lib().goipc_conn_listen, _util.encode_name(name), capacity), name)

	@classmethod
	def dial(cls, name: str) -> "Conn":
		"""Attaches to an endpoint the peer created with listen."""
		return cls(_util.open_handle(lib().goipc_conn_dial, _util.encode_name(name)), name)

	def readable(self) -> bool:
		return True

	def writable(self) -> bool:
		return True

	def _ensure_open(self) -> None:
		if self.closed:
			raise ValueError("I/O operation on closed goipc.Conn")

	def readinto(self, buffer: Any) -> int:
		"""Reads up to len(buffer) bytes. Returns 0 at end-of-stream."""
		self._ensure_open()
		dst, n, keep = _util.writable(buffer)
		if n == 0:
			return 0
		got = ctypes.c_size_t()
		rc = lib().goipc_conn_read(self._ptr, dst, n, _util.timeout_ns(self.timeout), ctypes.byref(got))
		if rc == EOF:
			return 0
		_util.check(rc)
		return int(got.value)

	def write(self, data: Any) -> int:
		"""Writes all of data, split into messages. A failure carries written, the bytes sent."""
		self._ensure_open()
		src, n, keep = _util.readable(data)
		if n == 0:
			return 0
		sent = ctypes.c_size_t()
		rc = lib().goipc_conn_write(self._ptr, src, n, _util.timeout_ns(self.timeout), ctypes.byref(sent))
		if rc < 0:
			exc = error_for(rc)
			exc.written = int(sent.value)  # type: ignore[attr-defined]
			raise exc
		return int(sent.value)

	def close_write(self, timeout: Optional[float] = None) -> None:
		"""Ends this side's stream, like socket.shutdown(SHUT_WR). Reads keep working."""
		self._ensure_open()
		_util.check(lib().goipc_conn_close_write(self._ptr, _util.timeout_ns(timeout)))

	def close(self) -> None:
		"""Sends end-of-stream, best effort, and releases every blocked reader and writer."""
		if self.closed:
			return
		try:
			if self._fin is not None and self._fin.alive:
				_util.check_close(lib().goipc_conn_close(self._ptr))
		finally:
			super().close()

	def unlink(self) -> None:
		"""Removes the endpoint's files. Only the side that called listen owns them."""
		_util.check(lib().goipc_conn_unlink(self._ptr))

	def __repr__(self) -> str:
		return "<goipc.Conn %r%s>" % (self.name, " closed" if self.closed else "")
