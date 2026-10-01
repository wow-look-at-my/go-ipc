"""Helpers for the goipc suite: names, bounded thread joins and peer processes."""

from __future__ import annotations

import itertools
import os
import selectors
import subprocess
import sys
import threading
import time
import unittest
from typing import Any, Callable, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
PYTHON_DIR = os.path.dirname(HERE)

# The longest any single blocking step may take before a test fails.
BOUND = 20.0

_counter = itertools.count()


def spec_dir() -> str:
	return os.environ.get("GOIPC_SPEC_DIR") or os.path.join(os.path.dirname(PYTHON_DIR), "spec")


def unique_name(label: str) -> str:
	"""Returns a name no other test, process or interpreter version uses."""
	return "pytest-%d-%d-%s" % (os.getpid(), next(_counter), label)


class Worker(threading.Thread):
	"""Runs fn on a daemon thread and keeps its result or exception."""

	def __init__(self, fn: Callable[[], Any], name: str = "worker") -> None:
		super().__init__(name=name, daemon=True)
		self.fn = fn
		self.result: Any = None
		self.error: Optional[BaseException] = None
		self.started_fn = threading.Event()

	def run(self) -> None:
		self.started_fn.set()
		try:
			self.result = self.fn()
		except BaseException as exc:
			self.error = exc

	def finish(self, test: unittest.TestCase, timeout: float = BOUND) -> Any:
		"""Joins within timeout, re-raises the worker's exception, and returns its result."""
		self.join(timeout)
		if self.is_alive():
			test.fail("%s did not finish within %g s" % (self.name, timeout))
		if self.error is not None:
			raise self.error
		return self.result

	def finish_error(self, test: unittest.TestCase, timeout: float = BOUND) -> BaseException:
		"""Joins within timeout and returns the exception the worker raised."""
		self.join(timeout)
		if self.is_alive():
			test.fail("%s did not finish within %g s" % (self.name, timeout))
		if self.error is None:
			test.fail("%s returned %r, want an exception" % (self.name, self.result))
		return self.error


def start(fn: Callable[[], Any], name: str = "worker") -> Worker:
	w = Worker(fn, name)
	w.start()
	return w


def env() -> dict:
	"""The environment for a peer process: this one, with python/ on PYTHONPATH."""
	out = dict(os.environ)
	path = out.get("PYTHONPATH")
	out["PYTHONPATH"] = PYTHON_DIR + (os.pathsep + path if path else "")
	return out


class Peer:
	"""A `python -m goipc.peer` process."""

	def __init__(self, *args: Any) -> None:
		self.args = [str(a) for a in args]
		self.proc = subprocess.Popen(
			[sys.executable, "-m", "goipc.peer"] + self.args,
			stdout=subprocess.PIPE,
			stderr=subprocess.PIPE,
			env=env(),
			bufsize=0,
		)
		self._out = b""

	def read_line(self, test: unittest.TestCase, timeout: float = BOUND) -> str:
		"""Returns the next stdout line, waiting at most timeout."""
		deadline = time.monotonic() + timeout
		assert self.proc.stdout is not None
		with selectors.DefaultSelector() as sel:
			sel.register(self.proc.stdout, selectors.EVENT_READ)
			while b"\n" not in self._out:
				left = deadline - time.monotonic()
				if left <= 0 or not sel.select(left):
					self.kill()
					test.fail("peer %s printed no line within %g s; stderr: %s" % (self.args, timeout, self.stderr()))
				chunk = os.read(self.proc.stdout.fileno(), 4096)
				if not chunk:
					test.fail("peer %s closed stdout; exit %s; stderr: %s" % (self.args, self.proc.wait(BOUND), self.stderr()))
				self._out += chunk
		line, _, self._out = self._out.partition(b"\n")
		return line.decode("ascii", "replace")

	def wait_ready(self, test: unittest.TestCase) -> None:
		line = self.read_line(test)
		test.assertEqual(line, "ready", "peer %s: first line" % (self.args,))

	def stderr(self) -> str:
		if self.proc.poll() is None or self.proc.stderr is None:
			return "(still running)"
		return self.proc.stderr.read().decode("utf-8", "replace")

	def finish(self, test: unittest.TestCase, want_code: int = 0, timeout: float = 70.0) -> List[str]:
		"""Waits for exit, checks the code, and returns the remaining stdout lines."""
		try:
			out, err = self.proc.communicate(timeout=timeout)
		except subprocess.TimeoutExpired:
			self.kill()
			test.fail("peer %s did not exit within %g s" % (self.args, timeout))
		text = (self._out + out).decode("ascii", "replace")
		self._out = b""
		test.assertEqual(
			self.proc.returncode,
			want_code,
			"peer %s exit code; stderr: %s" % (self.args, err.decode("utf-8", "replace")),
		)
		self.last_stderr = err.decode("utf-8", "replace")
		return [ln for ln in text.splitlines() if ln]

	def kill(self) -> None:
		if self.proc.poll() is None:
			self.proc.kill()
			self.proc.wait()
