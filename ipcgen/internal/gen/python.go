package gen

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/wow-look-at-my/go-ipc/ipcgen/internal/schema"
)

type pyGen struct {
	w writer
}

// pyHelpers is the shared part of every Python module. PKG stands for the package name.
const pyHelpers = `
from __future__ import annotations

import dataclasses as _dc
import struct as _st
import typing as _t

_Error = ValueError


def _get_var(buf: memoryview, off: int) -> _t.Tuple[bytes, int]:
	if len(buf) - off < 4:
		raise _Error("PKG: a length runs past the end of the input")
	(n,) = _st.unpack_from("<I", buf, off)
	off += 4
	if n > len(buf) - off:
		raise _Error("PKG: a length runs past the end of the input")
	return bytes(buf[off:off + n]), off + n


def _put_var(out: bytearray, raw: bytes) -> None:
	if len(raw) > 0xFFFFFFFF:
		raise _Error("PKG: a string or bytes field is longer than 4 GiB")
	out += _st.pack("<I", len(raw))
	out += raw


def _bool(b: int) -> bool:
	if b > 1:
		raise _Error("PKG: a bool is neither 0 nor 1")
	return b == 1


def _str(raw: bytes) -> str:
	try:
		return raw.decode("utf-8")
	except UnicodeDecodeError:
		raise _Error("PKG: a string is not valid UTF-8") from None


def _arr(v: _t.Sequence[_t.Any], n: int) -> _t.Sequence[_t.Any]:
	if len(v) != n:
		raise _Error("PKG: an array holds %d items, want %d" % (len(v), n))
	return v


def _view(buf: _t.Any) -> memoryview:
	return memoryview(buf).cast("B")
`

var pyFormat = map[schema.Kind]string{
	schema.U8: "B", schema.I8: "b", schema.U16: "H", schema.I16: "h", schema.U32: "I", schema.I32: "i",
	schema.U64: "Q", schema.I64: "q", schema.F32: "f", schema.F64: "d",
}

func genPy(s *schema.Schema) ([]byte, error) {
	g := &pyGen{}
	w := &g.w
	w.line("# " + Header)
	w.line("\"\"\"Messages of package %s, from %s.", s.Package, filepath.Base(s.File))
	w.line("")
	w.line("encode() returns the encoding. decode(buf) raises ValueError when buf is malformed.")
	w.line("The module needs only the standard library of CPython 3.8 or later.")
	w.line("\"\"\"")
	w.WriteString(strings.ReplaceAll(pyHelpers, "PKG", s.Package))
	for _, m := range s.Sorted {
		g.message(s.Package, m)
	}
	w.line("")
	w.line("")
	w.line("MESSAGES: _t.Dict[int, _t.Any] = {")
	for _, m := range s.Messages {
		w.line("\t%s.TYPE_ID: %s,", m.Name, m.Name)
	}
	w.line("}")
	return w.Bytes(), nil
}

func pyType(t *schema.Type) string {
	switch t.Kind {
	case schema.Array:
		return "_t.List[" + pyType(t.Elem) + "]"
	case schema.Ref:
		return t.Name
	case schema.String:
		return "str"
	case schema.Bytes:
		return "bytes"
	case schema.Bool:
		return "bool"
	case schema.F32, schema.F64:
		return "float"
	}
	return "int"
}

func pyDefault(t *schema.Type) string {
	switch t.Kind {
	case schema.Array:
		if t.Elem.Kind == schema.Ref {
			return fmt.Sprintf("_dc.field(default_factory=lambda: [%s() for _ in range(%d)])", t.Elem.Name, t.Len)
		}
		return fmt.Sprintf("_dc.field(default_factory=lambda: [%s] * %d)", pyDefault(t.Elem), t.Len)
	case schema.Ref:
		return "_dc.field(default_factory=" + t.Name + ")"
	case schema.String:
		return `""`
	case schema.Bytes:
		return `b""`
	case schema.Bool:
		return "False"
	case schema.F32, schema.F64:
		return "0.0"
	}
	return "0"
}

func (g *pyGen) message(pkg string, m *schema.Message) {
	w := &g.w
	w.line("")
	w.line("")
	w.line("@_dc.dataclass")
	w.line("class %s:", m.Name)
	w.line("\tTYPE_ID = %d", m.ID)
	w.line("\tFIXED_SIZE = %d", m.FixedSize)
	if len(m.Fields) > 0 {
		w.line("")
	}
	for _, f := range m.Fields {
		w.line("\t%s: %s = %s", f.Name, pyType(f.Type), pyDefault(f.Type))
	}
	w.line("")
	w.line("\tdef encode(self) -> bytes:")
	w.line("\t\tbuf = bytearray(%d)", m.FixedSize)
	w.line("\t\ttry:")
	w.line("\t\t\tself._put(buf, 0)")
	w.line("\t\texcept (_st.error, OverflowError) as e:")
	w.line("\t\t\traise _Error(\"%s: %s: %%s\" %% e) from None", pkg, m.Name)
	for _, f := range m.VarFields() {
		if f.Type.Kind == schema.String {
			w.line("\t\t_put_var(buf, self.%s.encode(\"utf-8\"))", f.Name)
		} else {
			w.line("\t\t_put_var(buf, bytes(self.%s))", f.Name)
		}
	}
	w.line("\t\treturn bytes(buf)")
	w.line("")
	w.line("\t@classmethod")
	w.line("\tdef decode(cls, buf: _t.Any) -> %s:", m.Name)
	w.line("\t\tbuf = _view(buf)")
	w.line("\t\tif len(buf) < %d:", m.FixedSize)
	w.line("\t\t\traise _Error(\"%s: input is shorter than the fixed section\")", pkg)
	w.line("\t\tm = cls._get(buf, 0)")
	w.line("\t\toff = %d", m.FixedSize)
	for _, f := range m.VarFields() {
		w.line("\t\traw, off = _get_var(buf, off)")
		if f.Type.Kind == schema.String {
			w.line("\t\tm.%s = _str(raw)", f.Name)
		} else {
			w.line("\t\tm.%s = raw", f.Name)
		}
	}
	w.line("\t\tif off != len(buf):")
	w.line("\t\t\traise _Error(\"%s: bytes remain after the last field\")", pkg)
	w.line("\t\treturn m")
	w.line("")
	w.line("\tdef _put(self, buf: bytearray, off: int) -> None:")
	if len(m.FixedFields()) == 0 {
		w.line("\t\tpass")
	}
	for _, f := range m.FixedFields() {
		g.put(f.Type, "self."+f.Name, f.Offset)
	}
	w.line("")
	w.line("\t@classmethod")
	w.line("\tdef _get(cls, buf: memoryview, off: int) -> %s:", m.Name)
	var args []string
	for _, f := range m.FixedFields() {
		args = append(args, f.Name+"="+g.get(f.Type, f.Offset))
	}
	if len(args) == 0 {
		w.line("\t\treturn cls()")
		return
	}
	w.line("\t\treturn cls(")
	for _, a := range args {
		w.line("\t\t\t%s,", a)
	}
	w.line("\t\t)")
}

func (g *pyGen) put(t *schema.Type, val string, off int) {
	w := &g.w
	switch {
	case t.Kind == schema.Ref:
		w.line("\t\t%s._put(buf, off + %d)", val, off)
	case t.Kind == schema.Bool:
		w.line("\t\tbuf[off + %d] = 1 if %s else 0", off, val)
	case t.Kind == schema.Array && t.Elem.Kind == schema.Ref:
		w.line("\t\tfor i, v in enumerate(_arr(%s, %d)):", val, t.Len)
		w.line("\t\t\tv._put(buf, off + %d + i * %d)", off, t.Elem.Size())
	case t.Kind == schema.Array && t.Elem.Kind == schema.Bool:
		w.line("\t\t_st.pack_into(\"<%dB\", buf, off + %d, *[1 if v else 0 for v in _arr(%s, %d)])", t.Len, off, val, t.Len)
	case t.Kind == schema.Array:
		w.line("\t\t_st.pack_into(\"<%d%s\", buf, off + %d, *_arr(%s, %d))", t.Len, pyFormat[t.Elem.Kind], off, val, t.Len)
	default:
		w.line("\t\t_st.pack_into(\"<%s\", buf, off + %d, %s)", pyFormat[t.Kind], off, val)
	}
}

// get returns the expression that loads a fixed field.
func (g *pyGen) get(t *schema.Type, off int) string {
	switch {
	case t.Kind == schema.Ref:
		return fmt.Sprintf("%s._get(buf, off + %d)", t.Name, off)
	case t.Kind == schema.Bool:
		return fmt.Sprintf("_bool(buf[off + %d])", off)
	case t.Kind == schema.Array && t.Elem.Kind == schema.Ref:
		return fmt.Sprintf("[%s._get(buf, off + %d + i * %d) for i in range(%d)]", t.Elem.Name, off, t.Elem.Size(), t.Len)
	case t.Kind == schema.Array && t.Elem.Kind == schema.Bool:
		return fmt.Sprintf("[_bool(b) for b in buf[off + %d:off + %d]]", off, off+t.Len)
	case t.Kind == schema.Array:
		return fmt.Sprintf("list(_st.unpack_from(\"<%d%s\", buf, off + %d))", t.Len, pyFormat[t.Elem.Kind], off)
	}
	return fmt.Sprintf("_st.unpack_from(\"<%s\", buf, off + %d)[0]", pyFormat[t.Kind], off)
}
