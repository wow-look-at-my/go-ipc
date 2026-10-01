"""Tests the generated Python module against spec/vectors/schema.

The Makefile sets IPCGEN_VECTORS to the vector directory, IPCGEN_SCHEMA_JSON to the ipcgen --lang json output
and PYTHONPATH to the directory of demo.py. The file runs on CPython 3.8 and later.
"""

import dataclasses
import json
import os
import unittest

import demo


def _env(name):
	value = os.environ.get(name)
	if not value:
		raise RuntimeError("%s is not set; run the tests through codegen/Makefile" % name)
	return value


VECTORS = _env("IPCGEN_VECTORS")
with open(_env("IPCGEN_SCHEMA_JSON")) as f:
	SCHEMA = {m["name"]: m for m in json.load(f)["messages"]}
with open(os.path.join(VECTORS, "values.json"), encoding="utf-8") as f:
	VALUES = json.load(f)
with open(os.path.join(VECTORS, "invalid.json"), encoding="utf-8") as f:
	INVALID = json.load(f)


def build(name, value):
	"""Returns the dataclass instance for a values.json value of message name."""
	fields = SCHEMA[name]["fields"]
	if sorted(value) != sorted(f["name"] for f in fields):
		raise AssertionError("%s: values.json keys %s do not match the schema" % (name, sorted(value)))
	return getattr(demo, name)(**{f["name"]: convert(f["type"], value[f["name"]]) for f in fields})


def convert(t, v):
	kind = t["kind"]
	if kind == "array":
		return [convert(t["elem"], x) for x in v]
	if kind == "message":
		return build(t["message"], v)
	if kind in ("u64", "i64"):
		return int(v)
	if kind in ("f32", "f64"):
		return float(v)
	if kind == "bytes":
		return bytes.fromhex(v)
	return v


def shape(v):
	"""Returns the value with every leaf replaced by its type, so a bool and an int compare unequal."""
	if dataclasses.is_dataclass(v):
		return (type(v), [shape(getattr(v, f.name)) for f in dataclasses.fields(v)])
	if isinstance(v, list):
		return [shape(x) for x in v]
	return type(v)


def read_vector(index):
	with open(os.path.join(VECTORS, "%d.bin" % index), "rb") as f:
		return f.read()


class ValuesTest(unittest.TestCase):
	def test_values(self):
		for i, entry in enumerate(VALUES):
			with self.subTest(entry=i, message=entry["message"]):
				cls = getattr(demo, entry["message"])
				want = read_vector(i)
				value = build(entry["message"], entry["value"])
				self.assertEqual(value.encode(), want)
				got = cls.decode(want)
				self.assertEqual(got, value)
				self.assertEqual(shape(got), shape(value))
				self.assertEqual(cls.decode(bytearray(want)), value)
				self.assertEqual(cls.decode(memoryview(want)), value)
				if want:
					with self.assertRaises(ValueError):
						cls.decode(want[:-1])
				with self.assertRaises(ValueError):
					cls.decode(want + b"\x00")

	def test_messages(self):
		self.assertEqual(sorted(demo.MESSAGES), sorted(m["id"] for m in SCHEMA.values()))
		for m in SCHEMA.values():
			cls = getattr(demo, m["name"])
			self.assertIs(demo.MESSAGES[m["id"]], cls)
			self.assertEqual(cls.TYPE_ID, m["id"])
			self.assertEqual(cls.FIXED_SIZE, m["fixed_size"])

	def test_invalid(self):
		for i, entry in enumerate(INVALID):
			with self.subTest(entry=i, message=entry["message"], error=entry["error"]):
				with self.assertRaises(ValueError):
					getattr(demo, entry["message"]).decode(bytes.fromhex(entry["hex"]))

	def test_encode_rejects_bad_values(self):
		with self.assertRaises(ValueError):
			demo.Scalars(f_u8=256).encode()
		with self.assertRaises(ValueError):
			demo.Record(tags=[1, 2, 3]).encode()
		with self.assertRaises(ValueError):
			demo.Text(title="\ud800").encode()


if __name__ == "__main__":
	unittest.main()
