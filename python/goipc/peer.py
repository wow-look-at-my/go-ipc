"""The interop peer of spec/peer.md: python3 -m goipc.peer <role> <args...>."""

from __future__ import annotations

import dataclasses
import importlib
import json
import os
import sys
import threading
from typing import Any, Callable, Dict, List

from ._endpoints import Conn, Queue

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


def role_recv(args: List[str]) -> None:
	_arity(args, "<name> <total> <capacity>")
	name = args[0]
	total, capacity = _ints(args[1:], ["total", "capacity"])
	q = Queue.create(name, capacity)
	try:
		_ready()
		next_seq: Dict[str, int] = {}
		for _ in range(total):
			type_, payload = q.recv()
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
			want = next_seq.get(sender, 0)
			if seq != want:
				raise PeerError("sender %r sent seq %d, want %d" % (sender, seq, want))
			next_seq[sender] = want + 1
		sys.stdout.write("ok %d\n" % total)
		sys.stdout.flush()
	finally:
		try:
			q.unlink()
		finally:
			q.close()


def role_send(args: List[str]) -> None:
	_arity(args, "<name> <sender> <count>")
	name, sender = args[0], args[1]
	(count,) = _ints(args[2:], ["count"])
	with Queue.open(name) as q:
		for i in range(count):
			q.send(("%s:%d" % (sender, i)).encode("ascii"), type=i)


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


ROLES: Dict[str, Callable[[List[str]], None]] = {
	"recv": role_recv,
	"send": role_send,
	"listen-echo": role_listen_echo,
	"dial-check": role_dial_check,
	"typed-send": role_typed_send,
	"typed-recv": role_typed_recv,
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
