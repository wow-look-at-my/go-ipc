"""The interop peer of spec/peer.md: python3 -m goipc.peer <role> <args...>."""

from __future__ import annotations

import dataclasses
import importlib
import json
import os
import struct
import sys
import threading
from typing import Any, Callable, Dict, List, NoReturn, Tuple

from . import service
from . import wire
from ._endpoints import Channel, Conn, Queue, release
from .errors import PeerGone

LIMIT_SECONDS = 60.0


class PeerError(Exception):
	"""A failed check. The message is the reason the peer prints."""


def _ready() -> None:
	sys.stdout.write("ready\n")
	sys.stdout.flush()


def _ints(args: List[str], names: List[str]) -> List[int]:
	out = []
	for value, name in zip(args, names):
		try:
			out.append(int(value, 10))
		except ValueError:
			raise PeerError("%s must be a decimal integer, not %r" % (name, value))
	return out


def _arity(args: List[str], usage: str) -> None:
	if len(args) != len(usage.split()):
		raise PeerError("usage: %s" % usage)


def pattern(n: int) -> bytes:
	"""Returns the dial-check stream: byte i is (i * 31 + 7) mod 256."""
	cycle = bytes((i * 31 + 7) % 256 for i in range(256))
	return (cycle * (n // 256 + 1))[:n]


def _print(line: str) -> None:
	sys.stdout.write(line + "\n")
	sys.stdout.flush()


def _exit_at_once() -> NoReturn:
	"""Exits with status 0 and runs no exit handler, finalizer or close, as _exit(0) does."""
	sys.stdout.flush()
	sys.stderr.flush()
	os._exit(0)


def _mode(value: str) -> str:
	if value not in ("close", "exit", "release"):
		raise PeerError("mode %r is not close, exit or release" % (value,))
	return value


def _stop_now(mode: str) -> NoReturn:
	"""Exits at once. Mode release removes the life socket first."""
	if mode == "release":
		release()
	_exit_at_once()


class _SeqCheck:
	"""Checks <sender>:<seq> payloads as the recv role does."""

	def __init__(self) -> None:
		self.next_seq: Dict[str, int] = {}

	def check(self, type_: int, payload: bytes) -> None:
		try:
			text = payload.decode("ascii")
		except UnicodeDecodeError:
			raise PeerError("payload %r is not ASCII" % (payload,))
		sender, sep, seq_text = text.rpartition(":")
		if not sep or not seq_text.isdigit():
			raise PeerError("payload %r is not <sender>:<seq>" % (text,))
		seq = int(seq_text)
		if type_ != seq:
			raise PeerError("payload %r has type %d, want %d" % (text, type_, seq))
		want = self.next_seq.get(sender, 0)
		if seq != want:
			raise PeerError("sender %r sent seq %d, want %d" % (sender, seq, want))
		self.next_seq[sender] = want + 1


def role_recv(args: List[str]) -> None:
	_arity(args, "<name> <total> <capacity>")
	name = args[0]
	total, capacity = _ints(args[1:], ["total", "capacity"])
	q = Queue.create(name, capacity)
	try:
		_recv_checked(q, total)
	finally:
		try:
			q.unlink()
		finally:
			q.close()


def _recv_checked(q: Queue, count: int) -> None:
	"""Prints ready, receives count checked messages, then prints ok <count>."""
	_ready()
	seq = _SeqCheck()
	for _ in range(count):
		seq.check(*q.recv())
	_print("ok %d" % count)


def role_send(args: List[str]) -> None:
	_arity(args, "<name> <sender> <count>")
	name, sender = args[0], args[1]
	(count,) = _ints(args[2:], ["count"])
	with Queue.open(name) as q:
		for i in range(count):
			q.send(("%s:%d" % (sender, i)).encode("ascii"), type=i)


def role_claim_and_die(args: List[str]) -> None:
	_arity(args, "<name> <length>")
	(length,) = _ints(args[1:], ["length"])
	q = Queue.open(args[0])
	c = q.claim(1, length)
	if length:
		c.buffer[:] = b"\xab" * length
	_exit_at_once()


def role_send_until_gone(args: List[str]) -> None:
	_arity(args, "<name> <sender>")
	name, sender = args[0], args[1]
	with Queue.open(name) as q:
		sent = 0
		while True:
			try:
				q.send(("%s:%d" % (sender, sent)).encode("ascii"), type=sent)
			except PeerGone:
				break
			sent += 1
		_print("gone %d" % sent)


def role_recv_then_stop(args: List[str]) -> None:
	_arity(args, "<name> <count> <capacity> <mode>")
	name = args[0]
	count, capacity = _ints(args[1:3], ["count", "capacity"])
	mode = _mode(args[3])
	q = Queue.create(name, capacity)
	if mode != "close":
		_recv_checked(q, count)
		q.unlink()
		_stop_now(mode)
	try:
		_recv_checked(q, count)
	finally:
		try:
			q.close()
		finally:
			q.unlink()


def role_chan_recv_until_gone(args: List[str]) -> None:
	_arity(args, "<name> <capacity> <count>")
	name = args[0]
	capacity, count = _ints(args[1:], ["capacity", "count"])
	c = Channel.create(name, capacity)
	try:
		_ready()
		got = 0
		while True:
			try:
				type_, payload = c.recv()
			except PeerGone:
				break
			want = ("0:%d" % got).encode("ascii")
			if type_ != got or payload != want:
				raise PeerError("message %d: type %d payload %r, want type %d payload %r" % (got, type_, payload, got, want))
			got += 1
		if got != count:
			raise PeerError("received %d messages before peer-gone, want %d" % (got, count))
		try:
			c.send(b"x")
		except PeerGone:
			pass
		else:
			raise PeerError("a send after the peer went succeeded, want peer-gone")
		_print("ok %d" % count)
	finally:
		try:
			c.close()
		finally:
			c.unlink()


def role_chan_send_then_stop(args: List[str]) -> None:
	_arity(args, "<name> <count> <mode>")
	(count,) = _ints(args[1:2], ["count"])
	mode = _mode(args[2])
	c = Channel.open(args[0])
	for i in range(count):
		c.send(("0:%d" % i).encode("ascii"), type=i)
	if mode != "close":
		_stop_now(mode)
	c.close()


def role_listen_echo(args: List[str]) -> None:
	_arity(args, "<name> <capacity>")
	name = args[0]
	(capacity,) = _ints(args[1:], ["capacity"])
	conn = Conn.listen(name, capacity)
	try:
		_ready()
		buf = bytearray(65536)
		view = memoryview(buf)
		while True:
			n = conn.readinto(buf)
			if n == 0:
				break
			conn.write(view[:n])
		# close sends end-of-stream only when the ring has room. close_write waits for room.
		conn.close_write()
	finally:
		try:
			conn.unlink()
		finally:
			conn.close()


def role_dial_check(args: List[str]) -> None:
	_arity(args, "<name> <bytes>")
	name = args[0]
	(total,) = _ints(args[1:], ["bytes"])
	want = pattern(total)
	conn = Conn.dial(name)
	try:
		failures: List[BaseException] = []

		def write() -> None:
			try:
				conn.write(want)
				conn.close_write()
			except BaseException as exc:
				failures.append(exc)

		writer = threading.Thread(target=write, name="dial-check writer", daemon=True)
		writer.start()
		got = bytearray()
		buf = bytearray(65536)
		while True:
			n = conn.readinto(buf)
			if n == 0:
				break
			got += buf[:n]
			if len(got) > total:
				raise PeerError("echo returned more than %d bytes" % total)
		writer.join()
		if failures:
			raise PeerError("write failed: %r" % (failures[0],))
		if len(got) != total:
			raise PeerError("echo returned %d bytes, want %d" % (len(got), total))
		if got != want:
			at = next(i for i in range(total) if got[i] != want[i])
			raise PeerError("byte %d is %d, want %d" % (at, got[at], want[at]))
	finally:
		conn.close()


def _schema_dir() -> str:
	spec = os.environ.get("GOIPC_SPEC_DIR")
	if not spec:
		raise PeerError("GOIPC_SPEC_DIR is not set; it must name the spec directory")
	return os.path.join(spec, "vectors", "schema")


def _demo() -> Any:
	try:
		return importlib.import_module("demo")
	except ImportError as exc:
		raise PeerError("the generated module demo is not on PYTHONPATH: %s" % (exc,))


def _from_json(template: Any, value: Any) -> Any:
	# A generated dataclass defaults every field to a value of its own type,
	# and every fixed array to its full length, so the default is the schema.
	if dataclasses.is_dataclass(template):
		return _build(type(template), value)
	if isinstance(template, list):
		return [_from_json(template[0], v) for v in value]
	if isinstance(template, bool):
		return value
	if isinstance(template, int):
		return int(value)
	if isinstance(template, float):
		return float(value)
	if isinstance(template, bytes):
		return bytes.fromhex(value)
	return value


def _build(cls: Any, value: Dict[str, Any]) -> Any:
	template = cls()
	names = [f.name for f in dataclasses.fields(cls)]
	if sorted(names) != sorted(value):
		raise PeerError("%s: values.json keys %s do not match fields %s" % (cls.__name__, sorted(value), sorted(names)))
	return cls(**{n: _from_json(getattr(template, n), value[n]) for n in names})


def _values() -> List[Dict[str, Any]]:
	with open(os.path.join(_schema_dir(), "values.json"), encoding="utf-8") as f:
		return json.load(f)


def role_typed_send(args: List[str]) -> None:
	_arity(args, "<name>")
	demo = _demo()
	with Queue.open(args[0]) as q:
		for entry in _values():
			cls = getattr(demo, entry["message"])
			q.send(_build(cls, entry["value"]).encode(), type=cls.TYPE_ID)


def role_typed_recv(args: List[str]) -> None:
	_arity(args, "<name> <capacity>")
	name = args[0]
	(capacity,) = _ints(args[1:], ["capacity"])
	demo = _demo()
	values = _values()
	q = Queue.create(name, capacity)
	try:
		_ready()
		for i, entry in enumerate(values):
			type_, payload = q.recv()
			want = getattr(demo, entry["message"])
			if type_ != want.TYPE_ID:
				raise PeerError("entry %d: record type %d, want %d (%s)" % (i, type_, want.TYPE_ID, entry["message"]))
			decoded = demo.MESSAGES[type_].decode(payload)
			with open(os.path.join(_schema_dir(), "%d.bin" % i), "rb") as f:
				expected = f.read()
			if decoded.encode() != expected:
				raise PeerError("entry %d: re-encoding differs from %d.bin" % (i, i))
		sys.stdout.write("ok %d\n" % len(values))
		sys.stdout.flush()
	finally:
		try:
			q.unlink()
		finally:
			q.close()


SERVICE_ECHO, SERVICE_ECHOED, SERVICE_FAIL, SERVICE_WHO, SERVICE_WHOAMI = 1, 2, 3, 4, 5


def _service_handler(session: service.Session, type_: int, payload: bytes) -> Tuple[int, bytes]:
	if type_ == SERVICE_ECHO:
		return SERVICE_ECHOED, payload
	if type_ == SERVICE_FAIL:
		raise PeerError(payload.decode("utf-8"))
	if type_ == SERVICE_WHO:
		return SERVICE_WHOAMI, struct.pack("<Q", session.ordinal)
	raise PeerError("type %d is not a request" % type_)


def role_service_serve(args: List[str]) -> None:
	_arity(args, "<name> <capacity> <clients>")
	name = args[0]
	capacity, clients = _ints(args[1:], ["capacity", "clients"])
	gone = threading.Semaphore(0)
	svc = service.serve(name, _service_handler, on_gone=lambda session: gone.release(), capacity=capacity)
	try:
		_ready()
		for _ in range(clients):
			gone.acquire()
		_print("ok %d" % clients)
	finally:
		svc.close()


def role_service_call(args: List[str]) -> None:
	_arity(args, "<name> <count> <mode>")
	name = args[0]
	(count,) = _ints(args[1:2], ["count"])
	mode = _mode(args[2])
	client = service.connect(name, timeout=LIMIT_SECONDS, capacity=wire.DEFAULT_CAPACITY)
	for i in range(count):
		want = str(i).encode("ascii")
		type_, reply = client.call(SERVICE_ECHO, want)
		if type_ != SERVICE_ECHOED or reply != want:
			raise PeerError("call %d: reply type %d payload %r, want type %d payload %r" % (i, type_, reply, SERVICE_ECHOED, want))
		boom = "boom %d" % i
		try:
			client.call(SERVICE_FAIL, boom.encode("utf-8"))
		except service.CallError as exc:
			if str(exc) != boom:
				raise PeerError("call %d: error %r, want %r" % (i, str(exc), boom))
		else:
			raise PeerError("call %d: a fail call succeeded, want a call error" % i)
	type_, reply = client.call(SERVICE_WHO)
	if type_ != SERVICE_WHOAMI or reply != struct.pack("<Q", client.ordinal):
		raise PeerError("who: reply type %d payload %r, want ordinal %d" % (type_, reply, client.ordinal))
	_print("ok %d" % count)
	if mode == "exit":
		_exit_at_once()
	client.close()


ROLES: Dict[str, Callable[[List[str]], None]] = {
	"service-serve": role_service_serve,
	"service-call": role_service_call,
	"recv": role_recv,
	"send": role_send,
	"listen-echo": role_listen_echo,
	"dial-check": role_dial_check,
	"typed-send": role_typed_send,
	"typed-recv": role_typed_recv,
	"claim-and-die": role_claim_and_die,
	"send-until-gone": role_send_until_gone,
	"recv-then-stop": role_recv_then_stop,
	"chan-recv-until-gone": role_chan_recv_until_gone,
	"chan-send-then-stop": role_chan_send_then_stop,
}


def _give_up(role: str) -> None:
	sys.stderr.write("peer %s: gave up after %g s\n" % (role, LIMIT_SECONDS))
	sys.stderr.flush()
	os._exit(1)


def main(argv: List[str]) -> int:
	"""Runs a role and returns the exit code."""
	if not argv:
		sys.stderr.write("usage: python3 -m goipc.peer <role> <args...>; roles: %s\n" % ", ".join(sorted(ROLES)))
		return 1
	role, args = argv[0], argv[1:]
	fn = ROLES.get(role)
	if fn is None:
		sys.stderr.write("peer: unknown role %r; roles: %s\n" % (role, ", ".join(sorted(ROLES))))
		return 1
	watchdog = threading.Timer(LIMIT_SECONDS, _give_up, args=(role,))
	watchdog.daemon = True
	watchdog.start()
	try:
		fn(args)
	except Exception as exc:
		sys.stderr.write("peer %s: %s: %s\n" % (role, type(exc).__name__, exc))
		sys.stderr.flush()
		return 1
	finally:
		watchdog.cancel()
	return 0


if __name__ == "__main__":
	sys.exit(main(sys.argv[1:]))
