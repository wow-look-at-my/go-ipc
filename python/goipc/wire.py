"""Wire constants from spec/wire.json. The test suite checks each one against that file."""

from __future__ import annotations

SPEC_VERSION = 2

RING_MAGIC = 0x676F2D6970632D31
RING_VERSION = 2
CONTROL_SIZE = 512
HEADER_SIZE = 16896
CACHE_LINE = 128
MIN_CAPACITY = 4096
RECORD_HEADER_SIZE = 8
RECORD_ALIGNMENT = 8
TYPE_PADDING = 0xFFFFFFFF

# Field name: (offset, size) in the ring control block.
RING_FIELDS = {
	"magic": (0, 8),
	"version": (8, 4),
	"flags": (12, 4),
	"capacity": (16, 8),
	"consumer": (24, 8),
	"tail": (128, 8),
	"head": (256, 8),
	"head_cache": (384, 8),
	"recv_waiters": (392, 4),
	"send_waiters": (396, 4),
}

CLAIM_SLOTS_OFFSET = 512
CLAIM_SLOTS = 256
CLAIM_SLOT_SIZE = 64
NO_INTENT = 0xFFFFFFFFFFFFFFFF

# Field name: (offset, size) in one claim slot.
CLAIM_SLOT_FIELDS = {
	"owner": (0, 8),
	"at": (8, 8),
	"size": (16, 8),
}

PROC_NONE = 0
PROC_PENDING = 1
PROC_WATCHABLE_BIT = 63
PROC_ALWAYS_SET_BIT = 62

RUNTIME_DIR = "/dev/shm"
FILE_MODE = 0o600
NAME_FILE_PATH = "/dev/shm/go-ipc-{name}.name"
INSTANCE_FILE_PATH = "/dev/shm/go-ipc-{name}.inc"
INSTANCE_ID_HEX_DIGITS = 16
SEGMENT_PATH = "/dev/shm/go-shm-{name}.{id}"
NOT_EMPTY_EVENT_PATH = "/dev/shm/go-ipc-{name}.{id}.ne.event"
NOT_FULL_EVENT_PATH = "/dev/shm/go-ipc-{name}.{id}.nf.event"
EVENT_PATH = "/dev/shm/go-ipc-{event}.event"
LIFE_SOCKET_PATH = "/dev/shm/go-ipc-life-{procid}.sock"
LIFE_SOCKET_PROCID_HEX_DIGITS = 16
LIFE_SOCKET_TEMP_SUFFIX = ".tmp"

DEFAULT_CAPACITY = 1048576
NOT_EMPTY_SUFFIX = ".ne"
NOT_FULL_SUFFIX = ".nf"
SIGNAL_MAX_TOKENS = 4096

CREATOR_TO_OPENER_SUFFIX = ".c2o"
OPENER_TO_CREATOR_SUFFIX = ".o2c"

CONN_TYPE_DATA = 0
CONN_TYPE_EOF = 1


def align8(n: int) -> int:
	"""Rounds n up to the record alignment."""
	return (n + RECORD_ALIGNMENT - 1) & ~(RECORD_ALIGNMENT - 1)


def max_message_size(capacity: int) -> int:
	"""Returns the largest payload a ring of this capacity accepts."""
	return min(capacity // 2 - RECORD_HEADER_SIZE, 2 ** 31 - 1 - RECORD_HEADER_SIZE)


def ring_size(capacity: int) -> int:
	"""Returns the buffer size a ring of this capacity needs."""
	return HEADER_SIZE + capacity


def validate_name(name: str) -> None:
	"""Raises InvalidName for a name the spec rejects."""
	from .errors import InvalidName

	if not name or "/" in name or "\\" in name or name in (".", ".."):
		raise InvalidName("goipc: invalid name %r" % (name,))
