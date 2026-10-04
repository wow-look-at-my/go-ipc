"""The library suite: ring conformance, Event, Queue, Channel and Conn, then peer processes."""

from __future__ import annotations

import array
import errno
import fcntl
import io
import os
import resource
import struct
import subprocess
import sys
import threading
import time
import unittest
from typing import Any, List, Tuple

import goipc
from goipc import service
from goipc.peer import SERVICE_ECHO, SERVICE_ECHOED, SERVICE_FAIL, SERVICE_WHO, SERVICE_WHOAMI, pattern

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


def needs_slot_api(case: dict) -> bool:
	"""Reports whether a case uses a consumer, acquire, drop or an attributed claim."""
	if int(case.get("consumer", "0x0"), 16) != 0:
		return True
	return any(op["op"] in ("acquire", "drop") or "slot" in op for op in case["ops"])


# libgoipc exports no init with a consumer, no slot acquire or drop, and no
# claim with a slot on a raw ring. So these cases are only attached and read.
SLOT_CASES = {"slots", "slot_wrap"}

RING_BYTES = goipc.HEADER_SIZE + 4096

# The release child creates a queue, releases on its first stdin line, and exits at stdin end-of-file.
_RELEASE_CHILD = """
import sys
import goipc
goipc.release()
q = goipc.Queue.create(sys.argv[1], int(sys.argv[2]))
print("ready", flush=True)
sys.stdin.readline()
goipc.release()
goipc.release()
print("released", flush=True)
sys.stdin.read()
q.unlink()
"""


def _consumer(name: str) -> int:
	"""Reads the consumer field of the queue's ring from its segment."""
	with open("/dev/shm/go-ipc-%s.inc" % name) as f:
		inc = f.read()
	with open("/dev/shm/go-shm-%s.%s" % (name, inc), "rb") as f:
		f.seek(24)
		return int(struct.unpack("<Q", f.read(8))[0])


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
		self.assertGreaterEqual(len(CASES), 10)

	def test_replay(self) -> None:
		for case in CASES:
			if case["name"] in SLOT_CASES:
				continue
			with self.subTest(case=case["name"]):
				buf = bytearray(case["buffer_size"])
				ring = goipc.Ring.init(buf)
				for op in case["ops"]:
					self.run_op(ring, buf, op)
				self.assertEqual(bytes(buf), image(case))

	def test_slot_cases_are_the_only_ones_not_replayed(self) -> None:
		need = {c["name"] for c in CASES if needs_slot_api(c)}
		self.assertEqual(need, SLOT_CASES)

	def test_attach_and_read(self) -> None:
		for case in CASES:
			with self.subTest(case=case["name"]):
				buf = bytearray(image(case))
				ring = goipc.Ring.attach(buf)
				self.assertEqual(ring.header_field("consumer"), int(case.get("consumer", "0x0"), 16))
				self.assertEqual(ring.head, case["head"])
				self.assertEqual(ring.tail, case["tail"])
				want = [(r["type"], payload(r)) for r in case["records"]]
				self.assertEqual(drain(ring), want)


class RingTest(unittest.TestCase):
	def test_size_and_properties(self) -> None:
		self.assertEqual(goipc.Ring.size(4096), RING_BYTES)
		buf = bytearray(goipc.HEADER_SIZE + 8192 + 296)
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
			goipc.Ring.init(bytearray(goipc.HEADER_SIZE + 100))
		with self.assertRaises(goipc.BadLayout):
			goipc.Ring.attach(bytearray(RING_BYTES))
		unaligned = memoryview(bytearray(RING_BYTES + 8))[1 : RING_BYTES + 1]
		with self.assertRaises(goipc.Unaligned):
			goipc.Ring.init(unaligned)
		with self.assertRaises(TypeError):
			goipc.Ring.init(bytes(RING_BYTES))

	def test_corrupt_length(self) -> None:
		buf = bytearray(RING_BYTES)
		ring = goipc.Ring.init(buf)
		ring.try_write(1, b"x")
		struct.pack_into("<i", buf, goipc.HEADER_SIZE, 3)
		with self.assertRaises(goipc.Corrupt):
			drain(ring)

	def test_buffer_too_small_keeps_the_record(self) -> None:
		ring = goipc.Ring.init(bytearray(RING_BYTES))
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
		ring = goipc.Ring.init(bytearray(RING_BYTES))
		ring.try_write(1, b"bytes")
		ring.try_write(2, bytearray(b"bytearray"))
		ring.try_write(3, memoryview(b"xxview")[2:])
		ring.try_write(4, b"")
		self.assertEqual(drain(ring), [(1, b"bytes"), (2, b"bytearray"), (3, b"view"), (4, b"")])

	def test_read_limit(self) -> None:
		ring = goipc.Ring.init(bytearray(RING_BYTES))
		for i in range(5):
			ring.try_write(i, b"%d" % i)
		self.assertEqual(drain(ring, 2), [(0, b"0"), (1, b"1")])
		self.assertEqual(drain(ring, 0), [])
		self.assertEqual(len(drain(ring)), 3)

	def test_callback_error_is_raised(self) -> None:
		ring = goipc.Ring.init(bytearray(RING_BYTES))
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
		ring = goipc.Ring.init(bytearray(RING_BYTES))
		ring.try_write(1, b"abc")
		kept: List[memoryview] = []
		ring.read(None, lambda t, p: kept.append(p))
		with self.assertRaises(ValueError):
			bytes(kept[0])


class ClaimTest(unittest.TestCase):
	def test_context_manager_commits(self) -> None:
		ring = goipc.Ring.init(bytearray(RING_BYTES))
		with ring.try_claim(5, 3) as claim:
			claim.buffer[:] = b"abc"
		self.assertTrue(claim.done)
		with self.assertRaises(ValueError):
			claim.buffer
		self.assertEqual(drain(ring), [(5, b"abc")])

	def test_context_manager_aborts_on_exception(self) -> None:
		ring = goipc.Ring.init(bytearray(RING_BYTES))
		with self.assertRaises(RuntimeError):
			with ring.try_claim(5, 3) as claim:
				claim.buffer[:] = b"abc"
				raise RuntimeError("fill failed")
		self.assertEqual(drain(ring), [])
		self.assertTrue(ring.empty)

	def test_open_claim_stalls_the_reader(self) -> None:
		ring = goipc.Ring.init(bytearray(RING_BYTES))
		claim = ring.try_claim(1, 1)
		ring.try_write(2, b"z")
		self.assertEqual(drain(ring), [])
		claim.buffer[0] = 0x41
		claim.commit()
		self.assertEqual(drain(ring), [(1, b"A"), (2, b"z")])
		with self.assertRaises(ValueError):
			claim.commit()

	def test_claim_errors(self) -> None:
		ring = goipc.Ring.init(bytearray(RING_BYTES))
		with self.assertRaises(goipc.ReservedType):
			ring.try_claim(goipc.TYPE_PADDING, 1)
		with self.assertRaises(goipc.MessageTooLarge):
			ring.try_claim(1, ring.max_message_size + 1)


CAP = 4096


class Named(unittest.TestCase):
	"""Creates endpoints with unique names and removes them after the test."""

	def queue(self, label: str, capacity: int = CAP) -> goipc.Queue:
		q = goipc.Queue.create(support.unique_name(label), capacity)
		self.addCleanup(q.close)
		self.addCleanup(q.unlink)
		return q

	def event(self, label: str) -> goipc.Event:
		e = goipc.Event.create(support.unique_name(label))
		self.addCleanup(e.close)
		self.addCleanup(e.unlink)
		return e

	def fill(self, q: goipc.Queue) -> int:
		"""Sends until the queue is full and returns the count sent."""
		n = 0
		while True:
			try:
				q.try_send(b"f" * 100, type=n)
			except goipc.Full:
				return n
			n += 1


class EventTest(Named):
	def test_signal_before_wait(self) -> None:
		e = self.event("ev")
		e.signal()
		e.wait(timeout=support.BOUND)

	def test_signal_crosses_handles(self) -> None:
		e = self.event("ev2")
		other = goipc.Event.open(e.name)
		self.addCleanup(other.close)
		other.signal(2)
		e.wait(timeout=support.BOUND)
		e.wait(timeout=support.BOUND)

	def test_timeout(self) -> None:
		e = self.event("evt")
		start = time.monotonic()
		with self.assertRaises(goipc.Timeout) as cm:
			e.wait(timeout=0.05)
		self.assertIsInstance(cm.exception, TimeoutError)
		self.assertGreaterEqual(time.monotonic() - start, 0.04)

	def test_close_releases_a_waiter(self) -> None:
		e = self.event("evc")
		waiter = support.start(lambda: e.wait(timeout=support.BOUND), "event waiter")
		waiter.started_fn.wait(support.BOUND)
		e.close()
		self.assertIsInstance(waiter.finish_error(self), goipc.Closed)
		e.close()

	def test_open_missing(self) -> None:
		with self.assertRaises(goipc.SystemCallError) as cm:
			goipc.Event.open(support.unique_name("missing"))
		self.assertEqual(cm.exception.errno, errno.ENOENT)


class QueueTest(Named):
	def test_send_recv(self) -> None:
		q = self.queue("q")
		self.assertEqual(q.capacity, CAP)
		self.assertEqual(q.max_message_size, 2040)
		q.send(b"one")
		q.send(bytearray(b"two"), type=2)
		q.try_send(memoryview(b"three"), type=3)
		self.assertEqual(q.recv(timeout=support.BOUND), (0, b"one"))
		self.assertEqual(q.recv(timeout=support.BOUND), (2, b"two"))
		self.assertEqual(q.try_recv(), (3, b"three"))
		with self.assertRaises(goipc.Empty):
			q.try_recv()

	def test_open_shares_the_ring(self) -> None:
		q = self.queue("qo")
		sender = goipc.Queue.open(q.name)
		self.addCleanup(sender.close)
		self.assertEqual(sender.capacity, CAP)
		sender.send(b"hi", type=9)
		self.assertEqual(q.recv(timeout=support.BOUND), (9, b"hi"))

	def test_errors(self) -> None:
		q = self.queue("qe")
		with self.assertRaises(goipc.MessageTooLarge):
			q.send(b"x" * (q.max_message_size + 1))
		with self.assertRaises(goipc.ReservedType):
			q.send(b"x", type=goipc.TYPE_PADDING)
		with self.assertRaises(goipc.Timeout):
			q.recv(timeout=0.01)
		with self.assertRaises(goipc.InvalidCapacity):
			goipc.Queue.create(support.unique_name("cap"), 5000)
		with self.assertRaises(goipc.InvalidName):
			goipc.Queue.create("a/b")
		with self.assertRaises(goipc.SystemCallError) as cm:
			goipc.Queue.open(support.unique_name("missing"))
		self.assertEqual(cm.exception.errno, errno.ENOENT)

	def test_argument_edges(self) -> None:
		q = goipc.Queue.create(support.unique_name("qd"), 0)
		self.addCleanup(q.close)
		self.addCleanup(q.unlink)
		self.assertEqual(q.capacity, goipc.DEFAULT_CAPACITY)
		with self.assertRaises(goipc.Timeout):
			q.recv(timeout=0)
		with self.assertRaises(goipc.InvalidArgument):
			q.read_batch(0, lambda t, p: None, timeout=0)

	def test_commit_after_close(self) -> None:
		q = goipc.Queue.create(support.unique_name("qcc"), CAP)
		self.addCleanup(q.unlink)
		claim = q.claim(1, 4)
		q.close()
		with self.assertRaises(goipc.Closed):
			claim.commit()

	def test_recv_into(self) -> None:
		q = self.queue("qi")
		q.send(b"z" * 300, type=4)
		with self.assertRaises(goipc.BufferTooSmall) as cm:
			q.recv_into(bytearray(10), timeout=support.BOUND)
		self.assertEqual(cm.exception.needed, 300)
		buf = bytearray(512)
		self.assertEqual(q.recv_into(buf, timeout=support.BOUND), (4, 300))
		self.assertEqual(bytes(buf[:300]), b"z" * 300)

	def test_recv_blocks_until_a_send(self) -> None:
		q = self.queue("qb")
		receiver = support.start(lambda: q.recv(timeout=support.BOUND), "receiver")
		q.send(b"wake", type=1)
		self.assertEqual(receiver.finish(self), (1, b"wake"))

	def test_full_queue_blocks_the_sender(self) -> None:
		q = self.queue("qf")
		n = self.fill(q)
		late = b"L" * 100
		with self.assertRaises(goipc.Full):
			q.try_send(late)
		with self.assertRaises(goipc.Timeout):
			q.send(late, timeout=0.01)
		sender = support.start(lambda: q.send(late, type=1000, timeout=support.BOUND), "sender")
		got = [q.recv(timeout=support.BOUND) for _ in range(n + 1)]
		sender.finish(self)
		self.assertEqual([t for t, _ in got], list(range(n)) + [1000])

	def test_close_releases_a_blocked_recv(self) -> None:
		q = goipc.Queue.create(support.unique_name("qcr"), CAP)
		self.addCleanup(q.unlink)
		receiver = support.start(lambda: q.recv(timeout=support.BOUND), "receiver")
		receiver.started_fn.wait(support.BOUND)
		q.close()
		self.assertIsInstance(receiver.finish_error(self), goipc.Closed)
		q.close()
		with self.assertRaises(goipc.Closed):
			q.send(b"after close")

	def test_close_releases_a_blocked_send(self) -> None:
		q = goipc.Queue.create(support.unique_name("qcs"), CAP)
		self.addCleanup(q.unlink)
		self.fill(q)
		sender = support.start(lambda: q.send(b"x" * 100, timeout=support.BOUND), "sender")
		sender.started_fn.wait(support.BOUND)
		q.close()
		# Close clears the consumer before it closes the events, so the sender
		# can wake to find the receiver gone.
		self.assertIsInstance(sender.finish_error(self), (goipc.Closed, goipc.PeerGone))

	def test_read_batch(self) -> None:
		q = self.queue("qr")
		for i in range(5):
			q.send(b"m%d" % i, type=i)
		got: List[Tuple[int, bytes]] = []
		self.assertEqual(q.read_batch(2, lambda t, p: got.append((t, bytes(p))), timeout=support.BOUND), 2)
		self.assertEqual(q.read_batch(None, lambda t, p: got.append((t, bytes(p))), timeout=support.BOUND), 3)
		self.assertEqual(got, [(i, b"m%d" % i) for i in range(5)])
		with self.assertRaises(goipc.Timeout):
			q.read_batch(None, lambda t, p: None, timeout=0.01)

	def test_read_batch_waits(self) -> None:
		q = self.queue("qrw")
		got: List[bytes] = []
		reader = support.start(
			lambda: q.read_batch(None, lambda t, p: got.append(bytes(p)), timeout=support.BOUND), "batch reader"
		)
		q.send(b"late")
		self.assertGreaterEqual(reader.finish(self), 1)
		self.assertEqual(got[0], b"late")

	def test_claim(self) -> None:
		q = self.queue("qc")
		with q.claim(7, 4, timeout=support.BOUND) as c:
			c.buffer[:] = b"abcd"
		with self.assertRaises(ZeroDivisionError):
			with q.claim(8, 1) as c:
				c.buffer[0] = 1
				1 // 0
		q.send(b"after")
		self.assertEqual(q.recv(timeout=support.BOUND), (7, b"abcd"))
		self.assertEqual(q.recv(timeout=support.BOUND), (0, b"after"))

	def test_many_producers(self) -> None:
		q = self.queue("qm")
		senders = 4
		count = 500

		def produce(s: int) -> None:
			for i in range(count):
				q.send(b"%d:%d" % (s, i), type=i, timeout=support.BOUND)

		workers = [support.start(lambda s=s: produce(s), "sender %d" % s) for s in range(senders)]
		seen = [0] * senders
		for _ in range(senders * count):
			t, p = q.recv(timeout=support.BOUND)
			s, i = (int(x) for x in p.split(b":"))
			self.assertEqual((i, t), (seen[s], seen[s]))
			seen[s] += 1
		for w in workers:
			w.finish(self)
		self.assertEqual(seen, [count] * senders)

	def test_recv_releases_the_gil(self) -> None:
		q = self.queue("qg")
		ring = q.ring
		receiver = support.start(lambda: q.recv(timeout=60), "receiver")
		# This thread runs Python while the receiver sits in the C wait. A
		# binding that held the GIL would stall it until the 60 s timeout.
		deadline = time.monotonic() + support.BOUND
		while ring.header_field("recv_waiters") == 0:
			if time.monotonic() > deadline:
				self.fail("the receiver never parked")
		self.assertEqual(sum(range(200000)), 199999 * 100000)
		self.assertTrue(receiver.is_alive())
		self.assertEqual(ring.header_field("recv_waiters"), 1)
		q.send(b"done")
		self.assertEqual(receiver.finish(self), (0, b"done"))

	def test_ring_view(self) -> None:
		q = self.queue("qv")
		ring = q.ring
		self.assertEqual(ring.capacity, CAP)
		q.send(b"abc")
		self.assertEqual(ring.tail, 16)
		self.assertEqual(ring.head, 0)


class ChannelTest(Named):
	def test_both_directions(self) -> None:
		name = support.unique_name("ch")
		a = goipc.Channel.create(name, CAP)
		self.addCleanup(a.close)
		self.addCleanup(a.unlink)
		b = goipc.Channel.open(name)
		self.addCleanup(b.close)
		self.assertEqual(a.max_message_size, 2040)
		self.assertEqual(a.tx.name, name)
		a.send(b"ping", type=1)
		self.assertEqual(b.recv(timeout=support.BOUND), (1, b"ping"))
		b.send(b"pong", type=2, timeout=support.BOUND)
		buf = bytearray(16)
		self.assertEqual(a.recv_into(buf, timeout=support.BOUND), (2, 4))
		b.try_send(b"t")
		self.assertEqual(a.try_recv(), (0, b"t"))
		with b.claim(3, 2) as c:
			c.buffer[:] = b"cl"
		got: List[Any] = []
		a.read_batch(None, lambda t, p: got.append((t, bytes(p))), timeout=support.BOUND)
		self.assertEqual(got, [(3, b"cl")])

	def test_close_releases_a_blocked_recv(self) -> None:
		name = support.unique_name("chc")
		a = goipc.Channel.create(name, CAP)
		self.addCleanup(a.unlink)
		receiver = support.start(lambda: a.recv(timeout=support.BOUND), "receiver")
		receiver.started_fn.wait(support.BOUND)
		a.close()
		self.assertIsInstance(receiver.finish_error(self), goipc.Closed)


class PeerTest(Named):
	"""Peer-gone, in-use, not-consumer and claim slots within one process."""

	def opened(self, name: str) -> goipc.Queue:
		q = goipc.Queue.open(name)
		self.addCleanup(q.close)
		return q

	def test_receiver_close_makes_senders_peer_gone(self) -> None:
		q = self.queue("pg")
		s = self.opened(q.name)
		s.send(b"before", timeout=support.BOUND)
		q.close()
		with self.assertRaises(goipc.PeerGone):
			s.send(b"x", timeout=support.BOUND)
		with self.assertRaises(goipc.PeerGone):
			s.try_send(b"x")
		with self.assertRaises(goipc.PeerGone):
			s.claim(1, 1, timeout=support.BOUND)
		with self.assertRaises(goipc.PeerGone):
			goipc.Queue.open(q.name)

	def test_receiver_close_wakes_a_parked_sender(self) -> None:
		q = self.queue("pgw")
		s = self.opened(q.name)
		self.fill(s)
		sender = support.start(lambda: s.send(b"x" * 100, timeout=support.BOUND), "sender")
		deadline = time.monotonic() + support.BOUND
		while q.ring.header_field("send_waiters") == 0:
			if time.monotonic() > deadline:
				self.fail("the sender never parked")
		q.close()
		self.assertIsInstance(sender.finish_error(self), goipc.PeerGone)

	def test_second_create_is_in_use(self) -> None:
		q = self.queue("iu")
		with self.assertRaises(goipc.InUse):
			goipc.Queue.create(q.name, CAP)
		name = support.unique_name("iuc")
		c = goipc.Channel.create(name, CAP)
		self.addCleanup(c.close)
		self.addCleanup(c.unlink)
		with self.assertRaises(goipc.InUse):
			goipc.Channel.create(name, CAP)

	def test_recv_on_an_opened_handle_is_not_consumer(self) -> None:
		q = self.queue("nc")
		s = self.opened(q.name)
		s.send(b"m")
		with self.assertRaises(goipc.NotConsumer):
			s.try_recv()
		with self.assertRaises(goipc.NotConsumer):
			s.recv(timeout=support.BOUND)
		with self.assertRaises(goipc.NotConsumer):
			s.recv_into(bytearray(16), timeout=support.BOUND)
		with self.assertRaises(goipc.NotConsumer):
			s.read_batch(None, lambda t, p: None, timeout=support.BOUND)
		self.assertEqual(q.try_recv(), (0, b"m"))

	def test_not_ready_name(self) -> None:
		name = support.unique_name("nr")
		path = goipc.wire.NAME_FILE_PATH.format(name=name)
		# The lock stands for a create in progress and keeps a sweep off the file.
		fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
		self.addCleanup(os.unlink, path)
		self.addCleanup(os.close, fd)
		fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
		with self.assertRaises(goipc.SystemCallError) as cm:
			goipc.Queue.open(name)
		self.assertEqual(cm.exception.errno, errno.EAGAIN)
		with self.assertRaises(goipc.InUse):
			goipc.Queue.create(name, CAP)

	def test_queue_claim_takes_a_slot(self) -> None:
		q = self.queue("cs")
		ring = q.ring
		me = ring.header_field("consumer")
		self.assertTrue(me >> goipc.wire.PROC_ALWAYS_SET_BIT & 1, hex(me))
		c = q.claim(1, 4, timeout=support.BOUND)
		self.assertGreaterEqual(c.slot, 0)
		self.assertEqual(ring.claim_slot(c.slot), (me, 0, 16))
		c.buffer[:] = b"abcd"
		c.commit()
		self.assertEqual(ring.claim_slot(c.slot), (me, goipc.wire.NO_INTENT, 16))
		with q.claim(2, 0) as again:
			self.assertEqual(again.slot, c.slot, "an idle slot of the handle is reused")
		self.assertEqual(goipc.Ring.init(bytearray(RING_BYTES)).try_claim(1, 1).slot, -1)

	def test_too_many_claims(self) -> None:
		q = self.queue("tm")
		claims = [q.claim(1, 0, timeout=0) for _ in range(goipc.wire.CLAIM_SLOTS)]
		self.assertEqual(sorted(c.slot for c in claims), list(range(goipc.wire.CLAIM_SLOTS)))
		with self.assertRaises(goipc.TooManyClaims):
			q.claim(1, 0, timeout=0)
		with self.assertRaises(goipc.TooManyClaims):
			q.try_send(b"x")
		for c in claims:
			c.abort()
		q.send(b"after", timeout=support.BOUND)
		self.assertEqual(q.recv(timeout=support.BOUND), (0, b"after"))

	def test_channel_has_one_peer(self) -> None:
		name = support.unique_name("c1")
		a = goipc.Channel.create(name, CAP)
		self.addCleanup(a.close)
		self.addCleanup(a.unlink)
		b = goipc.Channel.open(name)
		self.addCleanup(b.close)
		with self.assertRaises(goipc.InUse):
			goipc.Channel.open(name)

	def test_channel_peer_close_is_peer_gone_after_the_last_message(self) -> None:
		name = support.unique_name("cg")
		a = goipc.Channel.create(name, CAP)
		self.addCleanup(a.close)
		self.addCleanup(a.unlink)
		b = goipc.Channel.open(name)
		for i in range(3):
			b.send(b"0:%d" % i, type=i, timeout=support.BOUND)
		b.close()
		for i in range(3):
			self.assertEqual(a.recv(timeout=support.BOUND), (i, b"0:%d" % i))
		with self.assertRaises(goipc.PeerGone):
			a.recv(timeout=support.BOUND)
		with self.assertRaises(goipc.PeerGone):
			a.send(b"x", timeout=support.BOUND)

	def test_channel_creator_close_is_peer_gone_for_the_opener(self) -> None:
		name = support.unique_name("cg2")
		a = goipc.Channel.create(name, CAP)
		self.addCleanup(a.close)
		self.addCleanup(a.unlink)
		b = goipc.Channel.open(name)
		self.addCleanup(b.close)
		receiver = support.start(lambda: b.recv(timeout=support.BOUND), "receiver")
		receiver.started_fn.wait(support.BOUND)
		a.send(b"last", type=7, timeout=support.BOUND)
		self.assertEqual(receiver.finish(self), (7, b"last"))
		receiver = support.start(lambda: b.recv(timeout=support.BOUND), "receiver")
		receiver.started_fn.wait(support.BOUND)
		a.close()
		self.assertIsInstance(receiver.finish_error(self), goipc.PeerGone)
		with self.assertRaises(goipc.PeerGone):
			goipc.Channel.open(name)


class LifetimeTest(Named):
	def test_collected_handles_are_destroyed(self) -> None:
		q = goipc.Queue.create(support.unique_name("gc"), CAP)
		fin = q._fin
		q.unlink()
		del q
		self.assertFalse(fin.alive)

	def test_threads_share_a_handle(self) -> None:
		q = self.queue("qt")
		barrier = threading.Barrier(2, timeout=support.BOUND)

		def send() -> None:
			barrier.wait()
			q.send(b"x")

		w = support.start(send, "sender")
		barrier.wait()
		self.assertEqual(q.recv(timeout=support.BOUND), (0, b"x"))
		w.finish(self)


class ConnTest(Named):
	def pair(self) -> Tuple[goipc.Conn, goipc.Conn]:
		name = support.unique_name("conn")
		a = goipc.Conn.listen(name, CAP)
		self.addCleanup(a.close)
		self.addCleanup(a.unlink)
		b = goipc.Conn.dial(name)
		self.addCleanup(b.close)
		a.timeout = b.timeout = support.BOUND
		return a, b

	def read_all(self, conn: goipc.Conn) -> Tuple[bytes, int]:
		"""Reads to end-of-stream. Returns the bytes and the largest single read."""
		got = bytearray()
		largest = 0
		while True:
			chunk = conn.read(65536)
			if not chunk:
				return bytes(got), largest
			largest = max(largest, len(chunk))
			got += chunk

	def test_large_write_is_split_and_reassembled(self) -> None:
		a, b = self.pair()
		data = pattern(100000)

		def write() -> int:
			n = b.write(data)
			b.close_write()
			return n

		writer = support.start(write, "writer")
		got, largest = self.read_all(a)
		self.assertEqual(writer.finish(self), len(data))
		self.assertEqual(got, data)
		self.assertLessEqual(largest, 2040)
		self.assertEqual(a.read(10), b"")
		self.assertEqual(a.readinto(bytearray(10)), 0)

	def test_close_ends_the_stream(self) -> None:
		a, b = self.pair()
		b.write(b"hello")
		b.close()
		self.assertTrue(b.closed)
		self.assertEqual(self.read_all(a)[0], b"hello")
		with self.assertRaises(ValueError):
			b.write(b"after close")

	def test_close_write_keeps_reading(self) -> None:
		a, b = self.pair()
		b.close_write()
		with self.assertRaises(goipc.Closed):
			b.write(b"x")
		a.write(b"back")
		self.assertEqual(b.read(10), b"back")
		self.assertEqual(a.read(10), b"")
		a.close()
		self.assertEqual(b.read(10), b"")

	def test_buffered_io(self) -> None:
		a, b = self.pair()
		w = io.BufferedWriter(b)
		w.write(b"line one\nline two\n")
		w.flush()
		r = io.BufferedReader(a)
		self.assertEqual(r.readline(), b"line one\n")
		self.assertEqual(r.readline(), b"line two\n")
		w.close()
		self.assertEqual(r.read(), b"")
		self.assertTrue(b.closed)

	def test_read_timeout(self) -> None:
		a, b = self.pair()
		a.timeout = 0.02
		with self.assertRaises(goipc.Timeout):
			a.read(10)

	def test_close_releases_a_blocked_read(self) -> None:
		a, b = self.pair()
		reader = support.start(lambda: a.read(10), "reader")
		reader.started_fn.wait(support.BOUND)
		a.close()
		self.assertIsInstance(reader.finish_error(self), (goipc.Closed, ValueError))


class ProcessTest(Named):
	"""Runs the interop peer in child processes, against itself and against this process."""

	def test_recv_peer_with_send_peers(self) -> None:
		name = support.unique_name("pq")
		recv = support.Peer("recv", name, 2000, CAP)
		recv.wait_ready(self)
		senders = [support.Peer("send", name, "s%d" % i, 1000) for i in range(2)]
		for s in senders:
			s.finish(self)
		self.assertEqual(recv.finish(self), ["ok 2000"])

	def test_this_process_sends_to_recv_peer(self) -> None:
		name = support.unique_name("pq2")
		recv = support.Peer("recv", name, 500, CAP)
		recv.wait_ready(self)
		with goipc.Queue.open(name) as q:
			for i in range(500):
				q.send(b"py:%d" % i, type=i, timeout=support.BOUND)
		self.assertEqual(recv.finish(self), ["ok 500"])

	def test_send_peer_to_this_process(self) -> None:
		q = self.queue("pq3")
		sender = support.Peer("send", q.name, "peer", 300)
		for i in range(300):
			self.assertEqual(q.recv(timeout=support.BOUND), (i, b"peer:%d" % i))
		sender.finish(self)

	def test_typed_peers(self) -> None:
		count = len(support.load_json("vectors", "schema", "values.json"))
		name = support.unique_name("pt")
		recv = support.Peer("typed-recv", name, CAP)
		recv.wait_ready(self)
		support.Peer("typed-send", name).finish(self)
		self.assertEqual(recv.finish(self), ["ok %d" % count])

	def test_typed_recv_rejects_a_wrong_type(self) -> None:
		name = support.unique_name("pt2")
		recv = support.Peer("typed-recv", name, CAP)
		recv.wait_ready(self)
		with goipc.Queue.open(name) as q:
			q.send(b"not a message", type=0xFFFFFFF0, timeout=support.BOUND)
		recv.finish(self, want_code=1)
		self.assertIn("record type", recv.last_stderr)

	def test_echo_peers(self) -> None:
		name = support.unique_name("pc")
		echo = support.Peer("listen-echo", name, CAP)
		echo.wait_ready(self)
		support.Peer("dial-check", name, 200000).finish(self)
		echo.finish(self)

	def test_dial_check_peer_against_this_process(self) -> None:
		name = support.unique_name("pc2")
		conn = goipc.Conn.listen(name, CAP)
		self.addCleanup(conn.close)
		self.addCleanup(conn.unlink)
		conn.timeout = support.BOUND

		def echo() -> None:
			buf = bytearray(65536)
			while True:
				n = conn.readinto(buf)
				if n == 0:
					conn.close()
					return
				conn.write(memoryview(buf)[:n])

		echoer = support.start(echo, "echo")
		peer = support.Peer("dial-check", name, 50000)
		echoer.finish(self)
		peer.finish(self)

	def test_this_process_dials_echo_peer(self) -> None:
		name = support.unique_name("pc3")
		echo = support.Peer("listen-echo", name, CAP)
		echo.wait_ready(self)
		conn = goipc.Conn.dial(name)
		self.addCleanup(conn.close)
		conn.timeout = support.BOUND
		data = pattern(30000)

		def write() -> None:
			conn.write(data)
			conn.close_write()

		writer = support.start(write, "writer")
		got = bytearray()
		while True:
			chunk = conn.read(65536)
			if not chunk:
				break
			got += chunk
		writer.finish(self)
		self.assertEqual(bytes(got), data)
		conn.close()
		echo.finish(self)

	def test_recover_cell(self) -> None:
		k = 200
		name = support.unique_name("rc")
		recv = support.Peer("recv", name, 2 * k, CAP)
		recv.wait_ready(self)
		for args in (("claim-and-die", name, 64), ("send", name, "0", k), ("claim-and-die", name, 1500), ("send", name, "1", k)):
			self.assertEqual(support.Peer(*args).finish(self), [], args)
		self.assertEqual(recv.finish(self), ["ok %d" % (2 * k)])

	def test_this_process_recovers_dead_claims(self) -> None:
		q = self.queue("rc2")
		support.Peer("claim-and-die", q.name, 64).finish(self)
		ring = q.ring
		self.assertEqual(ring.tail, 72)
		q.send(b"live", type=3, timeout=support.BOUND)
		self.assertEqual(q.recv(timeout=support.BOUND), (3, b"live"))
		self.assertEqual(ring.head, ring.tail)

	def test_receiver_gone_cell(self) -> None:
		k = 300
		for mode in ("close", "exit", "release"):
			with self.subTest(mode=mode):
				name = support.unique_name("rg")
				recv = support.Peer("recv-then-stop", name, k, CAP, mode)
				recv.wait_ready(self)
				sender = support.Peer("send-until-gone", name, "0")
				self.assertEqual(recv.finish(self), ["ok %d" % k])
				lines = sender.finish(self)
				self.assertEqual(len(lines), 1, lines)
				word, n = lines[0].split()
				self.assertEqual(word, "gone")
				self.assertGreaterEqual(int(n), k)

	def test_this_process_sees_the_receiver_exit(self) -> None:
		k = 50
		name = support.unique_name("rg2")
		recv = support.Peer("recv-then-stop", name, k, CAP, "exit")
		recv.wait_ready(self)
		q = goipc.Queue.open(name)
		self.addCleanup(q.close)
		for i in range(k):
			q.send(b"py:%d" % i, type=i, timeout=support.BOUND)
		self.assertEqual(recv.finish(self), ["ok %d" % k])
		with self.assertRaises(goipc.PeerGone):
			while True:
				q.send(b"after", timeout=support.BOUND)

	def test_release_removes_the_life_socket_and_keeps_watches(self) -> None:
		name = support.unique_name("rel")
		child = subprocess.Popen(
			[sys.executable, "-c", _RELEASE_CHILD, name, str(CAP)],
			stdin=subprocess.PIPE,
			stdout=subprocess.PIPE,
			env=support.env(),
		)
		self.addCleanup(child.wait, support.BOUND)
		self.addCleanup(child.kill)
		assert child.stdin is not None and child.stdout is not None
		self.addCleanup(child.stdout.close)
		self.assertEqual(child.stdout.readline(), b"ready\n")
		life = "/dev/shm/go-ipc-life-%016x.sock" % _consumer(name)
		self.assertTrue(os.path.exists(life), life)

		# The first send starts a watch on the child before the release.
		q = goipc.Queue.open(name)
		self.addCleanup(q.close)
		q.send(b"before", timeout=support.BOUND)

		child.stdin.write(b"release\n")
		child.stdin.flush()
		self.assertEqual(child.stdout.readline(), b"released\n")
		self.assertFalse(os.path.exists(life), "the life socket is still there after release")
		# A new handle checks afresh and judges the child gone.
		with self.assertRaises(goipc.PeerGone):
			goipc.Queue.open(name)
		# The child still lives, and the earlier watch still says so.
		q.send(b"after", timeout=support.BOUND)

		child.stdin.close()
		self.assertEqual(child.wait(support.BOUND), 0)
		with self.assertRaises(goipc.PeerGone):
			while True:
				q.send(b"gone", timeout=support.BOUND)

	def test_second_creator_is_in_use_while_the_first_process_lives(self) -> None:
		name = support.unique_name("iu2")
		recv = support.Peer("recv", name, 1, CAP)
		recv.wait_ready(self)
		with self.assertRaises(goipc.InUse):
			goipc.Queue.create(name, CAP)
		with goipc.Queue.open(name) as q:
			with self.assertRaises(goipc.NotConsumer):
				q.try_recv()
			q.send(b"py:0", timeout=support.BOUND)
		self.assertEqual(recv.finish(self), ["ok 1"])
		after = goipc.Queue.create(name, CAP)
		after.unlink()
		after.close()

	def test_channel_peer_gone_cell(self) -> None:
		k = 300
		for mode in ("close", "exit", "release"):
			with self.subTest(mode=mode):
				name = support.unique_name("cpg")
				recv = support.Peer("chan-recv-until-gone", name, CAP, k)
				recv.wait_ready(self)
				support.Peer("chan-send-then-stop", name, k, mode).finish(self)
				self.assertEqual(recv.finish(self), ["ok %d" % k])

	def test_channel_peer_exit_is_peer_gone_after_the_last_message(self) -> None:
		k = 40
		for mode in ("close", "exit", "release"):
			with self.subTest(mode=mode):
				name = support.unique_name("cpg2")
				c = goipc.Channel.create(name, CAP)
				self.addCleanup(c.close)
				self.addCleanup(c.unlink)
				peer = support.Peer("chan-send-then-stop", name, k, mode)
				for i in range(k):
					self.assertEqual(c.recv(timeout=support.BOUND), (i, b"0:%d" % i))
				with self.assertRaises(goipc.PeerGone):
					c.recv(timeout=support.BOUND)
				with self.assertRaises(goipc.PeerGone):
					c.send(b"x", timeout=support.BOUND)
				peer.finish(self)

	def test_peer_failures_exit_1(self) -> None:
		closed = goipc.Queue.create(support.unique_name("closed"), CAP)
		self.addCleanup(closed.unlink)
		closed.close()
		for args in (
			("dial-check", support.unique_name("absent"), 10),
			("typed-send", "x"),
			("no-such-role",),
			("recv", "x", "many", 4096),
			("claim-and-die", support.unique_name("absent"), 8),
			("send-until-gone", support.unique_name("absent"), "s"),
			("send-until-gone", closed.name, "s"),
			("recv-then-stop", support.unique_name("mode"), 1, 4096, "linger"),
			("chan-send-then-stop", support.unique_name("absent"), 1, "close"),
			(),
		):
			with self.subTest(args=args):
				p = support.Peer(*args)
				p.finish(self, want_code=1)
				self.assertTrue(p.last_stderr.strip(), "a failing peer must say why")


PARK, PARKED = 6, 7


class ParkingService:
	"""Answers echo, fail and who, parks a park call until release is set, and records who goes."""

	def __init__(self) -> None:
		self.release = threading.Event()
		self.gone: List[int] = []
		self.gone_event = threading.Event()

	def handle(self, session: service.Session, type_: int, payload: bytes) -> Tuple[int, bytes]:
		if type_ == SERVICE_ECHO:
			return SERVICE_ECHOED, payload
		if type_ == SERVICE_FAIL:
			raise RuntimeError(payload.decode("utf-8"))
		if type_ == SERVICE_WHO:
			return SERVICE_WHOAMI, struct.pack("<Q", session.ordinal)
		if type_ == PARK:
			self.release.wait()
			return PARKED, b""
		raise ValueError("type %d is not a request" % type_)

	def on_gone(self, session: service.Session) -> None:
		self.gone.append(session.ordinal)
		self.gone_event.set()


def cpu_time() -> float:
	usage = resource.getrusage(resource.RUSAGE_SELF)
	return usage.ru_utime + usage.ru_stime


def echo(session: service.Session, type_: int, payload: bytes) -> Tuple[int, bytes]:
	return SERVICE_ECHOED, payload


class ServiceTest(unittest.TestCase):
	def serve(self, name: str) -> Tuple[service.Service, ParkingService]:
		h = ParkingService()
		svc = service.serve(name, h.handle, on_gone=h.on_gone, capacity=goipc.MIN_CAPACITY)
		self.addCleanup(svc.close)
		return svc, h

	def connect(self, name: str) -> service.Client:
		client = service.connect(name, timeout=support.BOUND, capacity=goipc.MIN_CAPACITY)
		self.addCleanup(client.close)
		return client

	def connect_early(self, name: str) -> support.Worker:
		"""Starts a connect to a service that does not exist and returns once its channel is in place."""
		worker = support.start(lambda: service.connect(name, timeout=support.BOUND, capacity=goipc.MIN_CAPACITY), "early connect")
		support.wait_until(self, lambda: bool(service.scan_clients(name)), "the client never created its channel")
		return worker

	def wait_parked(self, client: service.Client) -> None:
		support.wait_until(self, lambda: client._channel.rx.ring.header_field("recv_waiters") == 1, "the call never parked")

	def test_answers_calls(self) -> None:
		name = support.unique_name("svc")
		self.serve(name)
		client = self.connect(name)
		for i in range(100):
			self.assertEqual(client.call(SERVICE_ECHO, str(i).encode()), (SERVICE_ECHOED, str(i).encode()))
		with self.assertRaises(service.CallError) as cm:
			client.call(SERVICE_FAIL, b"boom")
		self.assertEqual(str(cm.exception), "boom")
		self.assertEqual(client.call(SERVICE_WHO), (SERVICE_WHOAMI, struct.pack("<Q", client.ordinal)))

	def test_each_client_gets_an_ordinal(self) -> None:
		name = support.unique_name("ordinal")
		self.serve(name)
		seen = set()
		for _ in range(4):
			client = self.connect(name)
			self.assertEqual(client.call(SERVICE_WHO), (SERVICE_WHOAMI, struct.pack("<Q", client.ordinal)))
			self.assertNotIn(client.ordinal, seen)
			seen.add(client.ordinal)

	def test_clients_run_independently(self) -> None:
		name = support.unique_name("independent")
		_, h = self.serve(name)
		parked = self.connect(name)
		worker = support.start(lambda: parked.call(PARK), "parked call")
		other = self.connect(name)
		self.assertEqual(other.call(SERVICE_ECHO, b"free"), (SERVICE_ECHOED, b"free"))
		h.release.set()
		self.assertEqual(worker.finish(self), (PARKED, b""))

	def test_client_waits_for_the_service(self) -> None:
		name = support.unique_name("early")
		worker = self.connect_early(name)
		self.serve(name)
		client = worker.finish(self)
		self.addCleanup(client.close)
		self.assertEqual(client.call(SERVICE_ECHO, b"early"), (SERVICE_ECHOED, b"early"))

	def test_reports_a_client_that_closes(self) -> None:
		name = support.unique_name("closes")
		_, h = self.serve(name)
		client = service.connect(name, timeout=support.BOUND, capacity=goipc.MIN_CAPACITY)
		ordinal = client.ordinal
		client.close()
		self.assertTrue(h.gone_event.wait(support.BOUND), "the handler was not told that the client went")
		self.assertEqual(h.gone, [ordinal])

	def test_reports_a_client_that_exits(self) -> None:
		name = support.unique_name("exits")
		_, h = self.serve(name)
		peer = support.Peer("service-call", name, 3, "exit")
		self.assertEqual(peer.finish(self), ["ok 3"])
		self.assertTrue(h.gone_event.wait(support.BOUND), "the handler was not told that the client exited")
		self.assertEqual(h.gone, [0])
		support.wait_until(self, lambda: not service.scan_clients(name), "the service must unlink a dead client's channel")

	def test_close_fails_every_call(self) -> None:
		name = support.unique_name("close")
		svc, _ = self.serve(name)
		client = self.connect(name)
		worker = support.start(lambda: client.call(PARK), "parked call")
		self.wait_parked(client)
		svc.close()
		self.assertIsInstance(worker.finish_error(self), goipc.PeerGone)
		with self.assertRaises(goipc.PeerGone):
			client.call(SERVICE_ECHO)
		svc.wait(support.BOUND)
		service.serve(name, echo).close()

	def test_name_is_held_while_it_runs(self) -> None:
		name = support.unique_name("inuse")
		self.serve(name)
		with self.assertRaises(goipc.InUse):
			service.serve(name, echo)

	def test_discards_a_stale_reply(self) -> None:
		name = support.unique_name("stale")
		_, h = self.serve(name)
		client = self.connect(name)
		with self.assertRaises(goipc.Timeout):
			client.call(PARK, timeout=0.05)
		h.release.set()
		self.assertEqual(client.call(SERVICE_ECHO, b"after"), (SERVICE_ECHOED, b"after"))

	def test_rejects_reserved_types(self) -> None:
		name = support.unique_name("reserved")
		self.serve(name)
		client = self.connect(name)
		with self.assertRaises(goipc.ReservedType):
			client.call(goipc.wire.SERVICE_TYPE_KNOCK)
		with self.assertRaises(goipc.ReservedType):
			client.call(goipc.TYPE_PADDING)

	def test_rejects_an_oversized_call(self) -> None:
		name = support.unique_name("oversized")
		self.serve(name)
		client = self.connect(name)
		with self.assertRaises(goipc.MessageTooLarge):
			client.call(SERVICE_ECHO, bytes(client.max_payload_size + 1))
		type_, reply = client.call(SERVICE_ECHO, bytes(client.max_payload_size))
		self.assertEqual(type_, SERVICE_ECHOED)
		self.assertEqual(len(reply), client.max_payload_size)

	def test_carries_a_whole_prompt(self) -> None:
		"""A prompt's token array travels in one call, so the channel is sized for one."""
		name = support.unique_name("prompt")
		svc = service.serve(name, echo)
		self.addCleanup(svc.close)
		client = service.connect(name, timeout=support.BOUND, capacity=1 << 21)
		self.addCleanup(client.close)
		tokens = array.array("i", range(1 << 17))
		type_, reply = client.call(SERVICE_ECHO, tokens.tobytes())
		self.assertEqual(type_, SERVICE_ECHOED)
		self.assertEqual(reply, tokens.tobytes())

	def test_the_default_capacity_holds_a_whole_prompt(self) -> None:
		"""The default a caller gets with no capacity named still carries a whole prompt."""
		payload = goipc.wire.max_message_size(service.SERVICE_CAPACITY)
		self.assertGreaterEqual(payload, 4 * (1 << 17))

	def test_rejects_a_bad_name(self) -> None:
		with self.assertRaises(goipc.InvalidName):
			service.serve("no/slashes", echo)
		with self.assertRaises(goipc.InvalidName):
			service.connect("no/slashes")

	def test_connect_gives_up_with_its_timeout(self) -> None:
		name = support.unique_name("giveup")
		with self.assertRaises(goipc.Timeout):
			service.connect(name, timeout=0.05, capacity=goipc.MIN_CAPACITY)
		self.assertEqual(service.scan_clients(name), [], "a client that gives up must unlink its channel")

	def test_typed_calls(self) -> None:
		import demo

		name = support.unique_name("typed")

		def handle(session: service.Session, type_: int, payload: bytes) -> Tuple[int, bytes]:
			req = demo.MESSAGES[type_].decode(payload)
			if req.x == 0:
				raise ValueError("zero")
			return demo.Vec2.TYPE_ID, demo.Vec2(x=req.x + 1, y=req.y).encode()

		svc = service.serve(name, handle, capacity=goipc.MIN_CAPACITY)
		self.addCleanup(svc.close)
		client = service.connect(name, timeout=support.BOUND, capacity=goipc.MIN_CAPACITY, messages=demo.MESSAGES)
		self.addCleanup(client.close)
		self.assertEqual(client.call_typed(demo.Vec2(x=1.5, y=2.0)), demo.Vec2(x=2.5, y=2.0))
		with self.assertRaises(service.CallError) as cm:
			client.call_typed(demo.Vec2(x=0.0, y=0.0))
		self.assertEqual(str(cm.exception), "zero")
		with self.assertRaises(TypeError):
			self.connect(name).call_typed(demo.Vec2())

	def test_parked_call_consumes_no_cpu(self) -> None:
		window = 0.3
		name = support.unique_name("nocpu")
		_, h = self.serve(name)
		parked = self.connect(name)
		worker = support.start(lambda: parked.call(PARK), "parked call")
		self.wait_parked(parked)
		early_name = support.unique_name("nocpu-early")
		early = self.connect_early(early_name)

		before = cpu_time()
		time.sleep(window)
		spent = cpu_time() - before
		self.assertLess(spent, window / 10, "parked calls burned %g s of CPU across a %g s window: something is spinning" % (spent, window))

		h.release.set()
		self.assertEqual(worker.finish(self), (PARKED, b""))
		svc = service.serve(early_name, h.handle, capacity=goipc.MIN_CAPACITY)
		self.addCleanup(svc.close)
		early.finish(self).close()
