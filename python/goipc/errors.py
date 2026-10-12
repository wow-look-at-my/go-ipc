"""Exceptions that mirror goipc_err."""

from __future__ import annotations

import os
from typing import Dict, NoReturn, Optional, Type

OK = 0
ECLOSED = -1
EFULL = -2
EEMPTY = -3
ETOOLARGE = -4
ERESERVED = -5
EINVALNAME = -6
EINVALCAP = -7
ETOOSMALL = -8
EBADLAYOUT = -9
ECORRUPT = -10
EUNALIGNED = -11
ETIMEDOUT = -12
ESYS = -13
ENOMEM = -14
EINVAL = -15
EBUFFER = -16
EOF = -17
EPEERGONE = -18
EINUSE = -19
ENOTCONSUMER = -20
ETOOMANYCLAIMS = -21
ECALL = -22


class IpcError(Exception):
	"""Base of every goipc error. code holds the goipc_err value."""

	code = 0


class Closed(IpcError):
	code = ECLOSED


class Full(IpcError):
	code = EFULL


class Empty(IpcError):
	code = EEMPTY


class MessageTooLarge(IpcError):
	code = ETOOLARGE


class ReservedType(IpcError):
	code = ERESERVED


class InvalidName(IpcError, ValueError):
	code = EINVALNAME


class InvalidCapacity(IpcError, ValueError):
	code = EINVALCAP


class TooSmall(IpcError):
	code = ETOOSMALL


class BadLayout(IpcError):
	code = EBADLAYOUT


class Corrupt(IpcError):
	code = ECORRUPT


class Unaligned(IpcError):
	code = EUNALIGNED


class Timeout(IpcError, TimeoutError):
	code = ETIMEDOUT


class SystemCallError(IpcError, OSError):
	"""A system call failed. errno holds the value from goipc_last_errno()."""

	code = ESYS


class NoMemory(IpcError, MemoryError):
	code = ENOMEM


class InvalidArgument(IpcError, ValueError):
	code = EINVAL


class BufferTooSmall(IpcError):
	"""The buffer is smaller than the next message. needed holds the size it requires."""

	code = EBUFFER
	needed = 0


class EndOfStream(IpcError, EOFError):
	code = EOF


class PeerGone(IpcError, ConnectionError):
	"""The process at the other end exited or closed its end."""

	code = EPEERGONE


class InUse(IpcError):
	"""A live process holds the name, or a channel already has its peer."""

	code = EINUSE


class NotConsumer(IpcError):
	"""A receive ran on a handle that does not own the receiving end."""

	code = ENOTCONSUMER


class TooManyClaims(IpcError):
	"""Live claims hold every claim slot of the ring."""

	code = ETOOMANYCLAIMS


class CallError(IpcError):
	"""A service handler raised, or returned an error. The message is the handler's."""

	code = ECALL


class CallbackError(IpcError):
	"""A read callback raised. The C reader had already consumed later records.

	dropped counts the records of that batch that never reached the callback.
	__cause__ holds the original exception.
	"""

	dropped = 0


_BY_CODE: Dict[int, Type[IpcError]] = {
	cls.code: cls
	for cls in (
		Closed, Full, Empty, MessageTooLarge, ReservedType, InvalidName,
		InvalidCapacity, TooSmall, BadLayout, Corrupt, Unaligned, Timeout,
		SystemCallError, NoMemory, InvalidArgument, BufferTooSmall, EndOfStream,
		PeerGone, InUse, NotConsumer, TooManyClaims, CallError,
	)
}


def error_class(code: int) -> Type[IpcError]:
	"""Returns the exception class for a goipc_err code."""
	return _BY_CODE.get(code, IpcError)


def make_error(code: int, message: Optional[str] = None, errno: Optional[int] = None) -> IpcError:
	"""Builds the exception for a negative goipc_err code."""
	cls = error_class(code)
	if message is None:
		message = "goipc error %d" % code
	if cls is SystemCallError:
		e = errno if errno is not None else 0
		return SystemCallError(e, "%s: %s" % (message, os.strerror(e)))
	exc = cls(message)
	if cls is IpcError:
		exc.code = code
	return exc


def error_for(code: int) -> IpcError:
	"""Returns the exception for a negative code, with the C library's message.

	Call it on the thread that made the failed call, so the errno is its own.
	"""
	from ._lib import lib

	cdll = lib()
	errno = cdll.goipc_last_errno() if code == ESYS else None
	raw = cdll.goipc_strerror(code)
	message = raw.decode("utf-8", "replace") if raw else None
	return make_error(code, message, errno)


def raise_for(code: int) -> NoReturn:
	"""Raises the exception for a negative code."""
	raise error_for(code)
