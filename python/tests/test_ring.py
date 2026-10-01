"""Ring conformance against spec/vectors/ring, ring errors and claims."""

from __future__ import annotations

import struct
import unittest
from typing import Any, List, Tuple

import goipc

import support

CASES = support.load_json("vectors", "ring", "manifest.json")["cases"]

ERRORS = {
	"full": goipc.Full,
	"too_large": goipc.MessageTooLarge,
	"reserved_type": goipc.ReservedType,
}


def payload(entry: dict) -> bytes:
	return bytes.fromhex(entry["payload"]) * entry.get("repeat", 1)


def drain(ring: goipc.Ring, limit: Any = None) -> List[Tuple[int, bytes]]:
	out: List[Tuple[int, bytes]] = []
	ring.read(limit, lambda t, p: out.append((t, bytes(p))))
	return out


def image(case: dict) -> bytes:
	return support.spec_file("vectors", "ring", case["name"] + ".bin")


class VectorTest(unittest.TestCase):
	def run_op(self, ring: goipc.Ring, buf: bytearray, op: dict) -> None:
		kind = op["op"]
		if kind == "write":
			data = payload(op)
			if "error" in op:
				before = bytes(buf)
				with self.assertRaises(ERRORS[op["error"]]):
					ring.try_write(op["type"], data)
				self.assertEqual(bytes(buf), before, "a failed write changed the ring")
			else:
				ring.try_write(op["type"], data)
		elif kind == "claim":
			data = payload(op)
			claim = ring.try_claim(op["type"], len(data))
			self.assertEqual(len(claim), len(data))
			claim.buffer[:] = data
			then = op["then"]
			if then == "commit":
				claim.commit()
			elif then == "abort":
				claim.abort()
			else:
				self.assertEqual(then, "none")
		elif kind == "read":
			ring.read(op["limit"], lambda t, p: None)
		else:
			self.fail("unknown op %r" % kind)

	def test_manifest_has_cases(self) -> None:
		self.assertGreaterEqual(len(CASES), 8)

	def test_replay(self) -> None:
		for case in CASES:
			with self.subTest(case=case["name"]):
				buf = bytearray(case["buffer_size"])
				ring = goipc.Ring.init(buf)
				for op in case["ops"]:
					self.run_op(ring, buf, op)
				self.assertEqual(bytes(buf), image(case))

	def test_attach_and_read(self) -> None:
		for case in CASES:
			with self.subTest(case=case["name"]):
				buf = bytearray(image(case))
				ring = goipc.Ring.attach(buf)
				self.assertEqual(ring.head, case["head"])
				self.assertEqual(ring.tail, case["tail"])
				want = [(r["type"], payload(r)) for r in case["records"]]
				self.assertEqual(drain(ring), want)


class RingTest(unittest.TestCase):
	def test_size_and_properties(self) -> None:
		self.assertEqual(goipc.Ring.size(4096), 4608)
		buf = bytearray(9000)
		ring = goipc.Ring.init(buf)
		self.assertIs(ring.buffer, buf)
		self.assertEqual(ring.capacity, 8192)
		self.assertEqual(ring.max_message_size, 4088)
		self.assertTrue(ring.empty)
		ring.try_write(3, b"abc")
		self.assertEqual(ring.buffered, 16)
		self.assertFalse(ring.empty)
		self.assertEqual(ring.header_field("magic"), goipc.RING_MAGIC)

	def test_layout_errors(self) -> None:
		with self.assertRaises(goipc.TooSmall):
			goipc.Ring.init(bytearray(512 + 100))
		with self.assertRaises(goipc.BadLayout):
			goipc.Ring.attach(bytearray(4608))
		unaligned = memoryview(bytearray(4608 + 8))[1:4609]
		with self.assertRaises(goipc.Unaligned):
			goipc.Ring.init(unaligned)
		with self.assertRaises(TypeError):
			goipc.Ring.init(bytes(4608))

	def test_corrupt_length(self) -> None:
		buf = bytearray(4608)
		ring = goipc.Ring.init(buf)
		ring.try_write(1, b"x")
		struct.pack_into("<i", buf, goipc.HEADER_SIZE, 3)
		with self.assertRaises(goipc.Corrupt):
			drain(ring)

	def test_buffer_too_small_keeps_the_record(self) -> None:
		ring = goipc.Ring.init(bytearray(4608))
		ring.try_write(4, b"y" * 100)
		with self.assertRaises(goipc.BufferTooSmall) as cm:
			ring.try_recv(bytearray(10))
		self.assertEqual(cm.exception.needed, 100)
		dst = bytearray(200)
		self.assertEqual(ring.try_recv(dst), (4, 100))
		self.assertEqual(bytes(dst[:100]), b"y" * 100)
		with self.assertRaises(goipc.Empty):
			ring.try_recv(dst)

	def test_payload_kinds(self) -> None:
		ring = goipc.Ring.init(bytearray(4608))
		ring.try_write(1, b"bytes")
		ring.try_write(2, bytearray(b"bytearray"))
		ring.try_write(3, memoryview(b"xxview")[2:])
		ring.try_write(4, b"")
		self.assertEqual(drain(ring), [(1, b"bytes"), (2, b"bytearray"), (3, b"view"), (4, b"")])

	def test_read_limit(self) -> None:
		ring = goipc.Ring.init(bytearray(4608))
		for i in range(5):
			ring.try_write(i, b"%d" % i)
		self.assertEqual(drain(ring, 2), [(0, b"0"), (1, b"1")])
		self.assertEqual(drain(ring, 0), [])
		self.assertEqual(len(drain(ring)), 3)

	def test_callback_error_is_raised(self) -> None:
		ring = goipc.Ring.init(bytearray(4608))
		ring.try_write(1, b"a")

		def boom(t: int, p: memoryview) -> None:
			raise KeyError("boom")

		with self.assertRaises(KeyError):
			ring.read(None, boom)
		for i in range(3):
			ring.try_write(i, b"b")
		with self.assertRaises(goipc.CallbackError) as cm:
			ring.read(None, boom)
		self.assertEqual(cm.exception.dropped, 2)
		self.assertIsInstance(cm.exception.__cause__, KeyError)

	def test_payload_view_ends_with_the_call(self) -> None:
		ring = goipc.Ring.init(bytearray(4608))
		ring.try_write(1, b"abc")
		kept: List[memoryview] = []
		ring.read(None, lambda t, p: kept.append(p))
		with self.assertRaises(ValueError):
			bytes(kept[0])


class ClaimTest(unittest.TestCase):
	def test_context_manager_commits(self) -> None:
		ring = goipc.Ring.init(bytearray(4608))
		with ring.try_claim(5, 3) as claim:
			claim.buffer[:] = b"abc"
		self.assertTrue(claim.done)
		with self.assertRaises(ValueError):
			claim.buffer
		self.assertEqual(drain(ring), [(5, b"abc")])

	def test_context_manager_aborts_on_exception(self) -> None:
		ring = goipc.Ring.init(bytearray(4608))
		with self.assertRaises(RuntimeError):
			with ring.try_claim(5, 3) as claim:
				claim.buffer[:] = b"abc"
				raise RuntimeError("fill failed")
		self.assertEqual(drain(ring), [])
		self.assertTrue(ring.empty)

	def test_open_claim_stalls_the_reader(self) -> None:
		ring = goipc.Ring.init(bytearray(4608))
		claim = ring.try_claim(1, 1)
		ring.try_write(2, b"z")
		self.assertEqual(drain(ring), [])
		claim.buffer[0] = 0x41
		claim.commit()
		self.assertEqual(drain(ring), [(1, b"A"), (2, b"z")])
		with self.assertRaises(ValueError):
			claim.commit()

	def test_claim_errors(self) -> None:
		ring = goipc.Ring.init(bytearray(4608))
		with self.assertRaises(goipc.ReservedType):
			ring.try_claim(goipc.TYPE_PADDING, 1)
		with self.assertRaises(goipc.MessageTooLarge):
			ring.try_claim(1, ring.max_message_size + 1)
