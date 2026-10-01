# Schema language

`ipcgen` reads a schema and writes message types for Go, C, C++ and Python. All four encode a value to the same bytes. The queue carries those bytes as a payload, with the message's type ID as the record type.

## Syntax

```
# A comment runs to the end of the line.
package demo

message Vec2 = 10 {
	x f32
	y f32
}

message Point = 11 {
	id u64
	pos Vec2
	tags [4]u16
	ok bool
	label string
	blob bytes
}
```

- `package` comes first, once. It names the Go package, the C prefix (`demo_`), the C++ namespace and the Python module. It is `lower_snake`.
- A message has a name in `UpperCamel`, a type ID, and one field per line. A message may have no fields. A field type may name a message declared later.
- A field has a name in `lower_snake` and a type.
- The type ID is a `u32`, written in decimal or as `0x` hex. It is unique in the schema. It must not be `0xFFFFFFFF`.
- A name must not be a keyword of C, C++, Go or Python. It must not start with `ipcgen`.
- A name must not clash with a generated name. `ipcgen` rejects these field names: `size`, `encode`, `decode`, `type_id`, `fixed_size`, `self`, `cls`.
- `ipcgen` also rejects a field that spells a Go method, a message called `MESSAGES`, and a message `<M>Type` beside a message `<M>`.
- Messages must not share one C name, and fields of a message must not share one Go name.

## Types

| type | size | alignment |
| --- | --- | --- |
| `bool` | 1 | 1 |
| `u8` `i8` | 1 | 1 |
| `u16` `i16` | 2 | 2 |
| `u32` `i32` `f32` | 4 | 4 |
| `u64` `i64` `f64` | 8 | 8 |
| `[N]T` | `N * size(T)` | `align(T)` |
| a message `M` | `size(M)` | `align(M)` |
| `string` | variable | none |
| `bytes` | variable | none |

`T` in `[N]T` is a scalar or a fixed message. `N` is at least 1. A fixed message is one with no `string` or `bytes` field at any depth. Only a fixed message may be a field type or an array element. A message must not contain itself.

## Encoding

A message encodes as its fixed section and then its variable tail.

**Fixed section.** Take every field that is not `string` or `bytes`, in declaration order. Place each at the next offset that is a multiple of its alignment. Store it little-endian. A `bool` is `0` or `1`. A nested message is its own fixed section. Zero every padding byte. The section size is the end of the last field, rounded up to the largest alignment in the message. `align(M)` is that largest alignment, or 1 when the message has no fixed field.

**Variable tail.** For each `string` or `bytes` field, in declaration order: a `u32` length, then that many bytes. No padding. A `string` is UTF-8.

**Decode** fails when the input is shorter than the fixed section, when a length runs past the end, when bytes remain after the last field, when a `bool` is neither 0 nor 1, or when a `string` is not valid UTF-8. A length prefix cut short counts as a length that runs past the end. Valid UTF-8 has no overlong form, no surrogate and nothing above U+10FFFF. Decode checks in this order. Decode reports the first failure: the fixed section size, every `bool`, then each variable field in order (its length, then its UTF-8), then trailing bytes.

## Generated code

| language | output |
| --- | --- |
| Go | a struct per message, `<M>Type` constant, `Size()`, `MarshalTo([]byte) int`, `MarshalBinary`, `UnmarshalBinary` |
| C | a header with a struct per message, `DEMO_<M>_TYPE`, `demo_<m>_size`, `demo_<m>_encode`, `demo_<m>_decode`. Decode makes each variable field a pointer into the input plus a length, with no copy. |
| C++ | a header in `namespace demo` with a struct per message, `type_id`, `size()`, `encode(std::span<std::byte>)`, `static decode(std::span<const std::byte>)` |
| Python | a module with a dataclass per message, `TYPE_ID`, `encode() -> bytes`, `decode(buf)`, and `MESSAGES` mapping each type ID to its class |
| JSON | `--lang json`: the checked schema with every offset and size, for tools and tests |

In C, `<m>` is the message name in `lower_snake` and `<M>` is the same in upper case: `PointCloud` gives `demo_point_cloud` and `DEMO_POINT_CLOUD_TYPE`. A `string` field is a `demo_ipcgen_str` and a `bytes` field is a `demo_ipcgen_bytes`, each a `ptr` and a `len`. Encode returns the bytes written, or 0 when the buffer is too small or a field is longer than 4 GiB. Decode returns 0, or `DEMO_IPCGEN_ESHORT`, `_ELENGTH`, `_ETRAILING`, `_EBOOL` or `_EUTF8`. In C++, decode returns `std::nullopt` on malformed input and does not throw. Go's `UnmarshalBinary` and Python's `decode` return or raise an error (`ValueError` in Python). Generated Python runs on CPython 3.8 and later.

## Conformance vectors

Cross-language conformance lives in `vectors/schema/`. `example.ipc` is the schema. `values.json` holds values. `<index>.bin` holds the encoding of each value. Every language encodes each value to the matching `.bin` and decodes each `.bin` back to the value. `invalid.json` holds malformed encodings that every decoder must reject.

`values.json` is an array of `{"message": "<M>", "value": {...}}`. A value is an object with every field of the message. A `bool` is `true` or `false`, a `u64` or `i64` is a decimal string, every other number is a JSON number. A `string` is a JSON string, `bytes` is a lower-case hex string. An array is a JSON array and a nested message is an object. Every float is exact in its type.

`invalid.json` is an array of `{"message": "<M>", "error": "<kind>", "hex": "<input>"}`. The kind is the first failure in decode order: `short`, `length`, `trailing`, `bool` or `utf8`.

`ipcgen/internal/refcodec` is the reference encoder. Its tests check every `.bin` file and every `invalid.json` kind. `IPCGEN_UPDATE_VECTORS=1` makes them rewrite the `.bin` files.
