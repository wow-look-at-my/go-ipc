"""Checks that need no libgoipc: constants, errors, conversions and loading."""

from __future__ import annotations

import os
import subprocess
import sys
import unittest

import goipc
from goipc import _util, errors, wire
from goipc.peer import pattern

import support


class WireConstantsTest(unittest.TestCase):
	spec = support.load_json("wire.json")

	def test_ring_constants(self) -> None:
		r = self.spec["ring"]
		self.assertEqual(wire.SPEC_VERSION, self.spec["spec_version"])
		self.assertEqual(wire.RING_MAGIC, int(r["magic"], 16))
		self.assertEqual(wire.RING_VERSION, r["version"])
		self.assertEqual(wire.CONTROL_SIZE, r["control_size"])
		self.assertEqual(wire.HEADER_SIZE, r["header_size"])
		self.assertEqual(wire.CACHE_LINE, r["cache_line"])
		self.assertEqual(wire.MIN_CAPACITY, r["min_capacity"])
		self.assertEqual(wire.RECORD_HEADER_SIZE, r["record_header_size"])
		self.assertEqual(wire.RECORD_ALIGNMENT, r["record_alignment"])
		self.assertEqual(wire.TYPE_PADDING, r["type_padding"])

	def test_ring_fields(self) -> None:
		want = {f["name"]: (f["offset"], f["size"]) for f in self.spec["ring"]["fields"]}
		self.assertEqual(wire.RING_FIELDS, want)

	def test_claim_slots(self) -> None:
		s = self.spec["ring"]["claim_slots"]
		self.assertEqual(wire.CLAIM_SLOTS_OFFSET, s["offset"])
		self.assertEqual(wire.CLAIM_SLOTS, s["count"])
		self.assertEqual(wire.CLAIM_SLOT_SIZE, s["slot_size"])
		self.assertEqual(wire.NO_INTENT, int(s["no_intent"], 16))
		want = {f["name"]: (f["offset"], f["size"]) for f in s["fields"]}
		self.assertEqual(wire.CLAIM_SLOT_FIELDS, want)
		self.assertEqual(wire.CLAIM_SLOTS_OFFSET, wire.CONTROL_SIZE)
		self.assertEqual(wire.CONTROL_SIZE + wire.CLAIM_SLOTS * wire.CLAIM_SLOT_SIZE, wire.HEADER_SIZE)

	def test_proc_id(self) -> None:
		p = self.spec["proc_id"]
		self.assertEqual(wire.PROC_NONE, p["none"])
		self.assertEqual(wire.PROC_PENDING, p["pending"])
		self.assertEqual(wire.PROC_WATCHABLE_BIT, p["watchable_bit"])
		self.assertEqual(wire.PROC_ALWAYS_SET_BIT, p["always_set_bit"])

	def test_paths(self) -> None:
		p = self.spec["paths"]
		self.assertEqual(wire.RUNTIME_DIR, p["runtime_dir"])
		self.assertEqual(wire.FILE_MODE, int(p["file_mode"], 8))
		self.assertEqual(wire.NAME_FILE_PATH, p["name_file"])
		self.assertEqual(wire.INSTANCE_FILE_PATH, p["instance_file"])
		self.assertEqual(wire.INSTANCE_ID_HEX_DIGITS, p["instance_id_hex_digits"])
		self.assertEqual(wire.SEGMENT_PATH, p["segment"])
		self.assertEqual(wire.NOT_EMPTY_EVENT_PATH, p["not_empty_event"])
		self.assertEqual(wire.NOT_FULL_EVENT_PATH, p["not_full_event"])
		self.assertEqual(wire.EVENT_PATH, p["event"])
		self.assertEqual(wire.LIFE_SOCKET_PATH, p["life_socket"])
		self.assertEqual(wire.LIFE_SOCKET_PROCID_HEX_DIGITS, p["life_socket_procid_hex_digits"])
		self.assertEqual(wire.LIFE_SOCKET_TEMP_SUFFIX, p["life_socket_temp_suffix"])

	def test_queue_channel_conn_constants(self) -> None:
		q = self.spec["queue"]
		self.assertEqual(wire.DEFAULT_CAPACITY, q["default_capacity"])
		self.assertEqual(wire.NOT_EMPTY_SUFFIX, q["not_empty_suffix"])
		self.assertEqual(wire.NOT_FULL_SUFFIX, q["not_full_suffix"])
		self.assertEqual(wire.SIGNAL_MAX_TOKENS, q["signal_max_tokens"])
		c = self.spec["channel"]
		self.assertEqual(wire.CREATOR_TO_OPENER_SUFFIX, c["creator_to_opener_suffix"])
		self.assertEqual(wire.OPENER_TO_CREATOR_SUFFIX, c["opener_to_creator_suffix"])
		self.assertEqual(wire.CONN_TYPE_DATA, self.spec["conn"]["type_data"])
		self.assertEqual(wire.CONN_TYPE_EOF, self.spec["conn"]["type_eof"])

	def test_service_constants(self) -> None:
		s = self.spec["service"]
		self.assertEqual(wire.SERVICE_REGISTRY_SUFFIX, s["registry_suffix"])
		self.assertEqual(wire.SERVICE_CLIENT_PREFIX, s["client_prefix"])
		self.assertEqual(wire.SERVICE_CLIENT_ID_HEX_DIGITS, s["client_id_hex_digits"])
		self.assertEqual(wire.SERVICE_RESERVED_TYPE_MIN, s["reserved_type_min"])
		self.assertEqual(wire.SERVICE_TYPE_KNOCK, s["type_knock"])
		self.assertEqual(wire.SERVICE_TYPE_HELLO, s["type_hello"])
		self.assertEqual(wire.SERVICE_TYPE_ERROR, s["type_error"])
		self.assertEqual(wire.SERVICE_SEQUENCE_SIZE, s["sequence_size"])
		self.assertEqual(wire.SERVICE_FIRST_SEQUENCE, s["first_sequence"])

	def test_package_exports_constants(self) -> None:
		for name in (
			"RING_MAGIC", "RING_VERSION", "HEADER_SIZE", "MIN_CAPACITY", "RECORD_HEADER_SIZE",
			"TYPE_PADDING", "DEFAULT_CAPACITY", "CONN_TYPE_DATA", "CONN_TYPE_EOF",
		):
			self.assertEqual(getattr(goipc, name), getattr(wire, name), name)

	def test_sizes(self) -> None:
		self.assertEqual(wire.align8(0), 0)
		self.assertEqual(wire.align8(9), 16)
		self.assertEqual(wire.max_message_size(4096), 2040)
		self.assertEqual(wire.max_message_size(2 ** 40), 2 ** 31 - 1 - 8)
		self.assertEqual(wire.ring_size(4096), 20992)


class ErrorsTest(unittest.TestCase):
	def test_codes_match_header(self) -> None:
		header = os.path.join(os.path.dirname(support.spec_dir()), "c", "include", "goipc.h")
		with open(header) as f:
			text = f.read()
		names = {
			"ECLOSED": goipc.Closed, "EFULL": goipc.Full, "EEMPTY": goipc.Empty,
			"ETOOLARGE": goipc.MessageTooLarge, "ERESERVED": goipc.ReservedType,
			"EINVALNAME": goipc.InvalidName, "EINVALCAP": goipc.InvalidCapacity,
			"ETOOSMALL": goipc.TooSmall, "EBADLAYOUT": goipc.BadLayout, "ECORRUPT": goipc.Corrupt,
			"EUNALIGNED": goipc.Unaligned, "ETIMEDOUT": goipc.Timeout, "ESYS": goipc.SystemCallError,
			"ENOMEM": goipc.NoMemory, "EINVAL": goipc.InvalidArgument,
			"EBUFFER": goipc.BufferTooSmall, "EOF": goipc.EndOfStream,
			"EPEERGONE": goipc.PeerGone, "EINUSE": goipc.InUse,
			"ENOTCONSUMER": goipc.NotConsumer, "ETOOMANYCLAIMS": goipc.TooManyClaims,
			"ECALL": goipc.CallError,
		}
		self.assertEqual(text.count("\tGOIPC_E"), len(names), "goipc.h has an error code this table lacks")
		for suffix, cls in names.items():
			marker = "GOIPC_%s = " % suffix
			self.assertIn(marker, text, suffix)
			value = int(text.split(marker, 1)[1].split(",")[0].split()[0])
			self.assertEqual(cls.code, value, suffix)
			self.assertIs(errors.error_class(value), cls)
			self.assertTrue(issubclass(cls, goipc.IpcError))

	def test_timeout_is_timeout_error(self) -> None:
		exc = errors.make_error(errors.ETIMEDOUT, "timed out")
		self.assertIsInstance(exc, goipc.Timeout)
		self.assertIsInstance(exc, TimeoutError)
		self.assertIsInstance(exc, goipc.IpcError)
		self.assertEqual(exc.code, errors.ETIMEDOUT)

	def test_system_call_error_carries_errno(self) -> None:
		exc = errors.make_error(errors.ESYS, "open", errno=2)
		self.assertIsInstance(exc, goipc.SystemCallError)
		self.assertIsInstance(exc, OSError)
		self.assertEqual(exc.errno, 2)
		self.assertEqual(exc.code, errors.ESYS)

	def test_peer_gone_is_connection_error(self) -> None:
		exc = errors.make_error(errors.EPEERGONE, "gone")
		self.assertIsInstance(exc, goipc.PeerGone)
		self.assertIsInstance(exc, ConnectionError)
		self.assertEqual(exc.code, errors.EPEERGONE)

	def test_unknown_code(self) -> None:
		exc = errors.make_error(-999)
		self.assertIs(type(exc), goipc.IpcError)
		self.assertEqual(exc.code, -999)


class ConversionTest(unittest.TestCase):
	def test_timeout_ns(self) -> None:
		self.assertEqual(_util.timeout_ns(None), -1)
		self.assertEqual(_util.timeout_ns(0), 0)
		self.assertEqual(_util.timeout_ns(1.5), 1500000000)
		self.assertEqual(_util.timeout_ns(1e30), -1)
		with self.assertRaises(ValueError):
			_util.timeout_ns(-1)
		with self.assertRaises(ValueError):
			_util.timeout_ns(float("nan"))

	def test_names(self) -> None:
		self.assertEqual(_util.encode_name("jobs"), b"jobs")
		for bad in ("", ".", "..", "a/b", "a\\b", "a\0b"):
			with self.assertRaises(goipc.InvalidName, msg=repr(bad)):
				_util.encode_name(bad)

	def test_buffers_are_not_copied(self) -> None:
		payload = bytearray(b"hello")
		arg, n, keep = _util.readable(payload)
		self.assertEqual(n, 5)
		payload[0] = ord("j")
		self.assertEqual(bytes(arg), b"jello")
		arg, n, keep = _util.readable(memoryview(b"abc"))
		self.assertEqual((bytes(arg), n), (b"abc", 3))
		with self.assertRaises(TypeError):
			_util.writable(b"read-only")

	def test_dial_check_pattern(self) -> None:
		p = pattern(600)
		self.assertEqual(len(p), 600)
		self.assertTrue(all(p[i] == (i * 31 + 7) % 256 for i in range(600)))


class LoadingTest(unittest.TestCase):
	def test_missing_library_names_the_variable(self) -> None:
		env = support.env()
		env["GOIPC_LIBRARY"] = "/nonexistent/libgoipc.so"
		code = "import goipc\ntry:\n\tgoipc.Ring.size(4096)\nexcept goipc.LibraryNotFound as e:\n\tprint(e)\n"
		out = subprocess.run(
			[sys.executable, "-c", code], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=support.BOUND
		)
		self.assertEqual(out.returncode, 0, out.stderr.decode())
		self.assertIn("GOIPC_LIBRARY=/nonexistent/libgoipc.so", out.stdout.decode())
