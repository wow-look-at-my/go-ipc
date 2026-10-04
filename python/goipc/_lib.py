"""Loads libgoipc and declares the prototype of every function the package calls."""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import threading
from typing import Any, List, Optional

ENV_VAR = "GOIPC_LIBRARY"

c_size_p = ctypes.POINTER(ctypes.c_size_t)
c_uint32_p = ctypes.POINTER(ctypes.c_uint32)
c_void_pp = ctypes.POINTER(ctypes.c_void_p)


class RingStruct(ctypes.Structure):
	_fields_ = [
		("hdr", ctypes.c_void_p),
		("data", ctypes.c_void_p),
		("mask", ctypes.c_uint64),
	]


class ClaimStruct(ctypes.Structure):
	_fields_ = [
		("ring", ctypes.c_void_p),
		("index", ctypes.c_uint64),
		("total", ctypes.c_int32),
		# The claim slot of a queue claim, or -1 for a raw ring claim.
		("slot", ctypes.c_int32),
		("bytes", ctypes.c_void_p),
		("len", ctypes.c_size_t),
	]


ring_p = ctypes.POINTER(RingStruct)
claim_p = ctypes.POINTER(ClaimStruct)

READ_FN = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_uint32, ctypes.c_void_p, ctypes.c_size_t)

_i = ctypes.c_int
_sz = ctypes.c_size_t
_u32 = ctypes.c_uint32
_i64 = ctypes.c_int64
_vp = ctypes.c_void_p
_cp = ctypes.c_char_p

# name: (restype, argtypes). Handles are opaque, so they travel as c_void_p.
_PROTOTYPES = {
	"goipc_strerror": (_cp, [_i]),
	"goipc_last_errno": (_i, []),
	"goipc_release": (_i, []),
	"goipc_ring_size": (_sz, [_sz]),
	"goipc_ring_init": (_i, [ring_p, _vp, _sz]),
	"goipc_ring_attach": (_i, [ring_p, _vp, _sz]),
	"goipc_ring_capacity": (_sz, [ring_p]),
	"goipc_ring_max_message_size": (_sz, [ring_p]),
	"goipc_ring_buffered": (_sz, [ring_p]),
	"goipc_ring_empty": (_i, [ring_p]),
	"goipc_ring_try_claim": (_i, [ring_p, _u32, _sz, claim_p]),
	"goipc_claim_commit": (None, [claim_p]),
	"goipc_claim_abort": (None, [claim_p]),
	"goipc_ring_try_write": (_i, [ring_p, _u32, _vp, _sz]),
	"goipc_ring_read": (_i, [ring_p, _i, READ_FN, _vp]),
	"goipc_ring_try_recv": (_i, [ring_p, _vp, _sz, c_uint32_p, c_size_p]),
	"goipc_event_create": (_i, [_cp, c_void_pp]),
	"goipc_event_open": (_i, [_cp, c_void_pp]),
	"goipc_event_signal": (_i, [_vp, _i]),
	"goipc_event_wait": (_i, [_vp, _i64]),
	"goipc_event_close": (_i, [_vp]),
	"goipc_event_unlink": (_i, [_vp]),
	"goipc_event_destroy": (None, [_vp]),
	"goipc_queue_create": (_i, [_cp, _sz, c_void_pp]),
	"goipc_queue_open": (_i, [_cp, c_void_pp]),
	"goipc_queue_name": (_cp, [_vp]),
	"goipc_queue_capacity": (_sz, [_vp]),
	"goipc_queue_max_message_size": (_sz, [_vp]),
	"goipc_queue_ring": (ring_p, [_vp]),
	"goipc_queue_try_send": (_i, [_vp, _u32, _vp, _sz]),
	"goipc_queue_send": (_i, [_vp, _u32, _vp, _sz, _i64]),
	"goipc_queue_claim": (_i, [_vp, _u32, _sz, _i64, claim_p]),
	"goipc_queue_commit": (_i, [_vp, claim_p]),
	"goipc_queue_abort": (_i, [_vp, claim_p]),
	"goipc_queue_try_recv": (_i, [_vp, _vp, _sz, c_uint32_p, c_size_p]),
	"goipc_queue_recv": (_i, [_vp, _vp, _sz, c_uint32_p, c_size_p, _i64]),
	"goipc_queue_read_batch": (_i, [_vp, _i, READ_FN, _vp, _i64]),
	"goipc_queue_close": (_i, [_vp]),
	"goipc_queue_unlink": (_i, [_vp]),
	"goipc_queue_destroy": (None, [_vp]),
	"goipc_channel_create": (_i, [_cp, _sz, c_void_pp]),
	"goipc_channel_open": (_i, [_cp, c_void_pp]),
	"goipc_channel_tx": (_vp, [_vp]),
	"goipc_channel_rx": (_vp, [_vp]),
	"goipc_channel_max_message_size": (_sz, [_vp]),
	"goipc_channel_send": (_i, [_vp, _u32, _vp, _sz, _i64]),
	"goipc_channel_recv": (_i, [_vp, _vp, _sz, c_uint32_p, c_size_p, _i64]),
	"goipc_channel_close": (_i, [_vp]),
	"goipc_channel_unlink": (_i, [_vp]),
	"goipc_channel_destroy": (None, [_vp]),
	"goipc_conn_listen": (_i, [_cp, _sz, c_void_pp]),
	"goipc_conn_dial": (_i, [_cp, c_void_pp]),
	"goipc_conn_channel": (_vp, [_vp]),
	"goipc_conn_read": (_i, [_vp, _vp, _sz, _i64, c_size_p]),
	"goipc_conn_write": (_i, [_vp, _vp, _sz, _i64, c_size_p]),
	"goipc_conn_close_write": (_i, [_vp, _i64]),
	"goipc_conn_close": (_i, [_vp]),
	"goipc_conn_unlink": (_i, [_vp]),
	"goipc_conn_destroy": (None, [_vp]),
}


class LibraryNotFound(OSError):
	"""No libgoipc could be loaded."""


def candidates() -> List[str]:
	"""Returns the paths tried, in order, when GOIPC_LIBRARY is unset."""
	here = os.path.dirname(os.path.abspath(__file__))
	out = [os.path.normpath(os.path.join(here, "..", "..", "c", "build", "libgoipc.so"))]
	found = ctypes.util.find_library("goipc")
	if found:
		out.append(found)
	return out


def _load() -> ctypes.CDLL:
	explicit = os.environ.get(ENV_VAR)
	if explicit:
		try:
			return ctypes.CDLL(explicit)
		except OSError as exc:
			raise LibraryNotFound(
				"goipc: cannot load %s=%s: %s" % (ENV_VAR, explicit, exc)
			) from exc
	tried = []
	for path in candidates():
		if os.sep in path and not os.path.exists(path):
			tried.append("%s (missing)" % path)
			continue
		try:
			return ctypes.CDLL(path)
		except OSError as exc:
			tried.append("%s (%s)" % (path, exc))
	if not ctypes.util.find_library("goipc"):
		tried.append('find_library("goipc") (no match)')
	raise LibraryNotFound(
		"goipc: libgoipc.so not found; set %s to its path. Tried: %s"
		% (ENV_VAR, "; ".join(tried))
	)


def _declare(cdll: ctypes.CDLL) -> None:
	for name, (restype, argtypes) in _PROTOTYPES.items():
		fn = getattr(cdll, name)
		fn.restype = restype
		fn.argtypes = argtypes


_lock = threading.Lock()
_cdll: Optional[ctypes.CDLL] = None


def lib() -> Any:
	"""Returns the loaded library. The first call loads it."""
	global _cdll
	cdll = _cdll
	if cdll is not None:
		return cdll
	with _lock:
		if _cdll is None:
			loaded = _load()
			_declare(loaded)
			_cdll = loaded
		return _cdll
