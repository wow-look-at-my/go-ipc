"""Helpers shared by the wrappers: timeouts, names and buffer pointers."""

from __future__ import annotations

import ctypes
import weakref
from typing import Any, Callable, Optional, Tuple

from .errors import ECLOSED, InvalidName, raise_for
from .wire import validate_name

FOREVER = -1
_MAX_NS = 2 ** 63 - 1

# A valid address for a zero-length buffer, so C never sees NULL.
_EMPTY = (ctypes.c_char * 1)()

_from_memory = ctypes.pythonapi.PyMemoryView_FromMemory
_from_memory.restype = ctypes.py_object
_from_memory.argtypes = [ctypes.c_void_p, ctypes.c_ssize_t, ctypes.c_int]
_PYBUF_READ = 0x100
_PYBUF_WRITE = 0x200


def _release(close: Callable[[Any], int], destroy: Callable[[Any], None], ptr: int) -> None:
	close(ptr)
	destroy(ptr)


def own(owner: Any, ptr: int, close: Callable[[Any], int], destroy: Callable[[Any], None]) -> weakref.finalize:
	"""Closes and destroys the C handle ptr when owner is collected.

	It does not run at interpreter exit: a daemon thread can still be inside a
	call on the handle then, and the process exit frees it anyway. A close
	method must check alive first, because a collected cycle runs this before
	any __del__.
	"""
	fin = weakref.finalize(owner, _release, close, destroy, ptr)
	fin.atexit = False
	return fin


def open_handle(fn: Callable[..., int], *args: Any) -> int:
	"""Calls a C constructor that fills an out pointer, and returns the handle."""
	out = ctypes.c_void_p()
	rc = fn(*args, ctypes.byref(out))
	if rc < 0:
		raise_for(rc)
	if not out.value:
		raise RuntimeError("goipc: the constructor returned success and no handle")
	return int(out.value)


def check(rc: int) -> int:
	"""Raises the exception for a negative goipc_err and returns any other value."""
	if rc < 0:
		raise_for(rc)
	return rc


class Endpoint:
	"""Context manager support for anything with close()."""

	def close(self) -> None:
		raise NotImplementedError

	def __enter__(self) -> Any:
		return self

	def __exit__(self, *exc: Any) -> None:
		self.close()


def check_close(rc: int) -> None:
	"""Raises for a failed close. A second close is not an error."""
	if rc < 0 and rc != ECLOSED:
		raise_for(rc)


def timeout_ns(timeout: Optional[float]) -> int:
	"""Converts float seconds, or None for forever, to the C int64 nanoseconds."""
	if timeout is None:
		return FOREVER
	if timeout != timeout or timeout < 0:
		raise ValueError("timeout must be None or a non-negative number, not %r" % (timeout,))
	ns = timeout * 1e9
	if ns >= _MAX_NS:
		return FOREVER
	return int(ns)


def encode_name(name: str) -> bytes:
	"""Validates a name and encodes it for the C API."""
	if not isinstance(name, str):
		raise TypeError("name must be str, not %s" % type(name).__name__)
	validate_name(name)
	if "\0" in name:
		raise InvalidName("goipc: invalid name %r" % (name,))
	return name.encode("utf-8")


def _view(obj: Any) -> memoryview:
	view = memoryview(obj)
	if not view.c_contiguous:
		raise BufferError("goipc: the buffer must be C-contiguous")
	return view


def readable(obj: Any) -> Tuple[Any, int, Any]:
	"""Returns (pointer argument, length, keepalive) for a bytes-like payload.

	bytes pass by pointer and writable buffers by from_buffer, so neither is
	copied. Only a read-only buffer that is not bytes gets one copy.
	"""
	if isinstance(obj, bytes):
		return obj, len(obj), obj
	view = _view(obj)
	n = view.nbytes
	if n == 0:
		return _EMPTY, 0, None
	if view.readonly:
		data = view.tobytes()
		return data, n, data
	arr = (ctypes.c_char * n).from_buffer(view)
	return arr, n, arr


def writable(obj: Any) -> Tuple[Any, int, Any]:
	"""Returns (pointer argument, length, keepalive) for a writable buffer."""
	view = _view(obj)
	if view.readonly:
		raise TypeError("goipc: the buffer must be writable")
	n = view.nbytes
	if n == 0:
		return _EMPTY, 0, None
	arr = (ctypes.c_char * n).from_buffer(view)
	return arr, n, arr


def view_at(address: int, length: int, writable: bool = True) -> memoryview:
	"""Returns a byte memoryview over length bytes at address. It owns nothing."""
	if length == 0:
		return memoryview(b"")
	return _from_memory(address, length, _PYBUF_WRITE if writable else _PYBUF_READ)
