"""Ring: the lock-free MPSC record ring over a caller buffer, and Claim."""

from __future__ import annotations

import ctypes
from typing import Any, Callable, Optional, Tuple

from . import _util
from ._lib import READ_FN, ClaimStruct, RingStruct, lib
from .errors import BufferTooSmall, CallbackError, error_for
from .wire import CLAIM_SLOT_FIELDS, CLAIM_SLOT_SIZE, CLAIM_SLOTS, CLAIM_SLOTS_OFFSET, RING_FIELDS

ReadFunc = Callable[[int, memoryview], Any]

_INT_MAX = 2 ** 31 - 1


def _limit(limit: Optional[int]) -> int:
	if limit is None:
		return _INT_MAX
	if limit < 0:
		raise ValueError("limit must be None or non-negative, not %r" % (limit,))
	return min(limit, _INT_MAX)


class _Reader:
	"""Adapts a Python read function to goipc_read_fn and keeps its exception."""

	def __init__(self, fn: ReadFunc) -> None:
		self.fn = fn
		self.error: Optional[BaseException] = None
		self.dropped = 0
		self.cfunc = READ_FN(self._call)

	def _call(self, ctx: Any, type_: int, payload: Any, length: int) -> None:
		if self.error is not None:
			self.dropped += 1
			return
		view = _util.view_at(payload or 0, length, writable=False)
		try:
			self.fn(type_, view)
			view.release()
		except BaseException as exc:
			self.error = exc

	def finish(self) -> None:
		"""Raises the callback's exception, if any, once the C call has returned."""
		exc = self.error
		if exc is None:
			return
		self.error = None
		if self.dropped:
			err = CallbackError(
				"goipc: the read callback raised %s; the reader consumed %d later record(s) of the batch without delivering them"
				% (type(exc).__name__, self.dropped)
			)
			err.dropped = self.dropped
			raise err from exc
		raise exc


class Claim:
	"""A reserved record. Fill buffer, then commit() or abort().

	As a context manager it commits on a normal exit and aborts on an exception.
	"""

	__slots__ = ("_struct", "_owner", "_commit", "_abort", "_view", "_done", "__weakref__")

	def __init__(self, struct: ClaimStruct, owner: Any, commit: Callable[[], None], abort: Callable[[], None]) -> None:
		self._struct = struct
		self._owner = owner
		self._commit = commit
		self._abort = abort
		self._view: Optional[memoryview] = _util.view_at(struct.bytes or 0, struct.len)
		self._done = False

	@property
	def buffer(self) -> memoryview:
		"""The payload region. It aliases the ring and is valid until commit or abort."""
		if self._view is None:
			raise ValueError("goipc: the claim is already committed or aborted")
		return self._view

	def __len__(self) -> int:
		return int(self._struct.len)

	@property
	def slot(self) -> int:
		"""The claim slot that names this process as the owner, or -1 for a raw ring claim."""
		return int(self._struct.slot)

	def _finish(self) -> None:
		if self._done:
			raise ValueError("goipc: the claim is already committed or aborted")
		self._done = True
		view = self._view
		self._view = None
		if view is not None:
			view.release()

	def commit(self) -> None:
		"""Publishes the record to the reader."""
		self._finish()
		self._commit()

	def abort(self) -> None:
		"""Discards the record. The reader skips it."""
		self._finish()
		self._abort()

	@property
	def done(self) -> bool:
		return self._done

	def __enter__(self) -> "Claim":
		return self

	def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
		if self._done:
			return
		if exc_type is None:
			self.commit()
		else:
			self.abort()


class Ring:
	"""A ring over a writable, 8-byte aligned buffer such as a bytearray or an mmap.

	The Ring keeps a reference to the buffer. Exactly one thread across all
	processes may read; any number may claim and write.
	"""

	def __init__(self, buffer: Any, struct: RingStruct, keepalive: Any) -> None:
		"""Use init or attach. keepalive holds whatever owns the memory."""
		self.buffer = buffer
		self._keepalive = keepalive
		self._struct = struct
		self._ref = ctypes.byref(struct)

	@staticmethod
	def size(capacity: int) -> int:
		"""Returns the buffer size a ring of this capacity needs."""
		return int(lib().goipc_ring_size(capacity))

	@classmethod
	def _setup(cls, fn: Callable[..., int], buffer: Any) -> "Ring":
		ptr, n, keep = _util.writable(buffer)
		struct = RingStruct()
		_util.check(fn(ctypes.byref(struct), ptr, n))
		return cls(buffer, struct, keep)

	@classmethod
	def init(cls, buffer: Any) -> "Ring":
		"""Zeroes buffer and writes a fresh ring into it."""
		return cls._setup(lib().goipc_ring_init, buffer)

	@classmethod
	def attach(cls, buffer: Any) -> "Ring":
		"""Attaches to a ring that already lives in buffer. It writes nothing."""
		return cls._setup(lib().goipc_ring_attach, buffer)

	@property
	def capacity(self) -> int:
		return int(lib().goipc_ring_capacity(self._ref))

	@property
	def max_message_size(self) -> int:
		return int(lib().goipc_ring_max_message_size(self._ref))

	@property
	def buffered(self) -> int:
		"""The bytes in flight between the cursors."""
		return int(lib().goipc_ring_buffered(self._ref))

	@property
	def empty(self) -> bool:
		return bool(lib().goipc_ring_empty(self._ref))

	def header_field(self, name: str) -> int:
		"""Reads a control block field by its wire.json name. For diagnostics and tests."""
		offset, size = RING_FIELDS[name]
		addr = self._struct.hdr + offset
		if name in ("recv_waiters", "send_waiters"):
			return ctypes.c_int32.from_address(addr).value
		ctype = ctypes.c_uint64 if size == 8 else ctypes.c_uint32
		return int(ctype.from_address(addr).value)

	def claim_slot(self, index: int) -> Tuple[int, int, int]:
		"""Returns (owner, at, size) of claim slot index. For diagnostics and tests."""
		if not 0 <= index < CLAIM_SLOTS:
			raise IndexError("claim slot %r is outside 0..%d" % (index, CLAIM_SLOTS - 1))
		base = self._struct.hdr + CLAIM_SLOTS_OFFSET + index * CLAIM_SLOT_SIZE

		def load(field: str) -> int:
			return int(ctypes.c_uint64.from_address(base + CLAIM_SLOT_FIELDS[field][0]).value)

		return load("owner"), load("at"), load("size")

	@property
	def head(self) -> int:
		return self.header_field("head")

	@property
	def tail(self) -> int:
		return self.header_field("tail")

	def try_claim(self, type: int, length: int) -> Claim:
		"""Reserves a record of length payload bytes. Raises Full when there is no room."""
		cdll = lib()
		ring = self._ref
		return claim(
			self,
			lambda out: cdll.goipc_ring_try_claim(ring, type, length, out),
			cdll.goipc_claim_commit,
			cdll.goipc_claim_abort,
		)

	def try_write(self, type: int, payload: Any) -> None:
		"""Copies payload into the ring as one record. Raises Full when there is no room."""
		ptr, n, keep = _util.readable(payload)
		_util.check(lib().goipc_ring_try_write(self._ref, type, ptr, n))

	def read(self, limit: Optional[int], fn: ReadFunc) -> int:
		"""Passes up to limit committed records to fn(type, payload) and returns the count.

		limit None means no limit. The payload aliases the ring and is valid only
		during the call. It never blocks.
		"""
		cdll = lib()
		ring = self._ref
		return read(lambda cfunc: cdll.goipc_ring_read(ring, _limit(limit), cfunc, None), fn)

	def try_recv(self, buffer: Any) -> Tuple[int, int]:
		"""Copies the next record into buffer and returns (type, nbytes).

		Raises Empty when no record is ready, or BufferTooSmall, with needed set,
		when buffer cannot hold it. The record then stays in the ring.
		"""
		cdll = lib()
		ring = self._ref
		return recv(lambda dst, n, t, ln: cdll.goipc_ring_try_recv(ring, dst, n, t, ln), buffer)


def claim(
	owner: Any,
	call: Callable[[Any], int],
	commit: Callable[[Any], None],
	abort: Callable[[Any], None],
) -> Claim:
	"""Runs call(out claim pointer) and wraps the result. commit and abort take that pointer."""
	struct = ClaimStruct()
	ref = ctypes.byref(struct)
	_util.check(call(ref))
	return Claim(struct, owner, lambda: commit(ref), lambda: abort(ref))


def read(call: Callable[[Any], int], fn: ReadFunc) -> int:
	"""Runs call(goipc_read_fn) with fn adapted, and returns the record count."""
	reader = _Reader(fn)
	rc = call(reader.cfunc)
	reader.finish()
	return int(_util.check(rc))


def recv(call: Callable[[Any, int, Any, Any], int], buffer: Any) -> Tuple[int, int]:
	"""Runs call(dst, cap, type out, len out) into buffer and returns (type, nbytes).

	BufferTooSmall gets needed set from the length out.
	"""
	dst, n, keep = _util.writable(buffer)
	type_ = ctypes.c_uint32()
	length = ctypes.c_size_t()
	rc = call(dst, n, ctypes.byref(type_), ctypes.byref(length))
	if rc < 0:
		exc = error_for(rc)
		if isinstance(exc, BufferTooSmall):
			exc.needed = int(length.value)
		raise exc
	return int(type_.value), int(length.value)
