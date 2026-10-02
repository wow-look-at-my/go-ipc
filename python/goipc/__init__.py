"""goipc: shared-memory IPC that interoperates with the Go, C and C++ go-ipc.

The package binds libgoipc.so through ctypes. It loads the library on first
use: GOIPC_LIBRARY first, then ../c/build/libgoipc.so beside the package's
directory, then ctypes.util.find_library("goipc").
"""

from __future__ import annotations

from ._endpoints import Channel, Conn, Event, Queue, release
from ._lib import LibraryNotFound
from .errors import (
	BadLayout,
	BufferTooSmall,
	CallbackError,
	Closed,
	Corrupt,
	Empty,
	EndOfStream,
	Full,
	InUse,
	InvalidArgument,
	InvalidCapacity,
	InvalidName,
	IpcError,
	MessageTooLarge,
	NoMemory,
	NotConsumer,
	PeerGone,
	ReservedType,
	SystemCallError,
	Timeout,
	TooManyClaims,
	TooSmall,
	Unaligned,
)
from .ring import Claim, Ring
from .wire import (
	CONN_TYPE_DATA,
	CONN_TYPE_EOF,
	DEFAULT_CAPACITY,
	HEADER_SIZE,
	MIN_CAPACITY,
	RECORD_HEADER_SIZE,
	RING_MAGIC,
	RING_VERSION,
	TYPE_PADDING,
)

__all__ = [
	"BadLayout", "BufferTooSmall", "CallbackError", "Channel", "Claim", "Closed",
	"Conn", "Corrupt", "Empty", "EndOfStream", "Event", "Full", "InUse",
	"InvalidArgument", "InvalidCapacity", "InvalidName", "IpcError",
	"LibraryNotFound", "MessageTooLarge", "NoMemory", "NotConsumer", "PeerGone",
	"Queue", "ReservedType", "Ring", "SystemCallError", "Timeout",
	"TooManyClaims", "TooSmall", "Unaligned", "release",
	"CONN_TYPE_DATA", "CONN_TYPE_EOF", "DEFAULT_CAPACITY", "HEADER_SIZE",
	"MIN_CAPACITY", "RECORD_HEADER_SIZE", "RING_MAGIC", "RING_VERSION",
	"TYPE_PADDING",
]
