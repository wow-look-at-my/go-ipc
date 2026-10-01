package gen

import (
	"fmt"
	"go/format"
	"path/filepath"
	"strings"

	"github.com/wow-look-at-my/go-ipc/ipcgen/internal/schema"
)

type goGen struct {
	w                           writer
	pkg                         string
	useBinary, useMath, useUTF8 bool
}

func genGo(s *schema.Schema) ([]byte, error) {
	g := &goGen{pkg: s.Package}
	g.helpers()
	for _, m := range s.Messages {
		g.message(m)
	}
	var w writer
	w.line("// " + Header)
	w.line("")
	w.line("// Package %s holds the messages of schema %s.", s.Package, filepath.Base(s.File))
	w.line("package %s", s.Package)
	w.line("")
	w.line("import (")
	if g.useBinary {
		w.line("\t\"encoding/binary\"")
	}
	w.line("\t\"errors\"")
	if g.useMath {
		w.line("\t\"math\"")
	}
	if g.useUTF8 {
		w.line("\t\"unicode/utf8\"")
	}
	w.line(")")
	w.Write(g.w.Bytes())
	out, err := format.Source(w.Bytes())
	if err != nil {
		return nil, fmt.Errorf("format generated Go: %w", err)
	}
	return out, nil
}

func (g *goGen) helpers() {
	w, p := &g.w, g.pkg
	w.line("")
	w.line("var (")
	w.line("\terrIpcgenShort    = errors.New(%q)", p+": input is shorter than the fixed section")
	w.line("\terrIpcgenLength   = errors.New(%q)", p+": a length runs past the end of the input")
	w.line("\terrIpcgenTrailing = errors.New(%q)", p+": bytes remain after the last field")
	w.line("\terrIpcgenBool     = errors.New(%q)", p+": a bool is neither 0 nor 1")
	w.line("\terrIpcgenUTF8     = errors.New(%q)", p+": a string is not valid UTF-8")
	w.line("\terrIpcgenTooLong  = errors.New(%q)", p+": a string or bytes field is longer than 4 GiB")
	w.line(")")
	w.line("")
	w.line("// ipcgenFits reports whether n fits in the u32 length prefix.")
	w.line("func ipcgenFits(n int) bool { return uint64(n) <= 0xFFFFFFFF }")
	w.line("")
	w.line("// ipcgenPutVar writes a length prefix and v at off and returns the offset after them.")
	w.line("func ipcgenPutVar[T string | []byte](b []byte, off int, v T) int {")
	w.line("\tbinary.LittleEndian.PutUint32(b[off:], uint32(len(v)))")
	w.line("\treturn off + 4 + copy(b[off+4:], v)")
	w.line("}")
	w.line("")
	w.line("// ipcgenGetVar reads a length prefix and the bytes after it.")
	w.line("func ipcgenGetVar(b []byte, off int) ([]byte, int, error) {")
	w.line("\tif len(b)-off < 4 {")
	w.line("\t\treturn nil, 0, errIpcgenLength")
	w.line("\t}")
	w.line("\tn := binary.LittleEndian.Uint32(b[off:])")
	w.line("\toff += 4")
	w.line("\tif uint64(n) > uint64(len(b)-off) {")
	w.line("\t\treturn nil, 0, errIpcgenLength")
	w.line("\t}")
	w.line("\treturn b[off : off+int(n)], off + int(n), nil")
	w.line("}")
	g.useBinary = true
}

func goType(t *schema.Type) string {
	switch t.Kind {
	case schema.Array:
		return fmt.Sprintf("[%d]%s", t.Len, goType(t.Elem))
	case schema.Ref:
		return t.Name
	case schema.String:
		return "string"
	case schema.Bytes:
		return "[]byte"
	case schema.Bool:
		return "bool"
	case schema.F32:
		return "float32"
	case schema.F64:
		return "float64"
	}
	name := t.Kind.String()
	if t.Kind.IsSigned() {
		return "int" + name[1:]
	}
	return "uint" + name[1:]
}

// goBits returns the unsigned Go type and the binary accessor suffix of a multi-byte scalar.
func goBits(k schema.Kind) (string, string) {
	n := fmt.Sprint(k.ScalarSize() * 8)
	return "uint" + n, "Uint" + n
}

func (g *goGen) message(m *schema.Message) {
	w := &g.w
	w.line("")
	w.line("// %s is message %d of package %s.", m.Name, m.ID, g.pkg)
	w.line("type %s struct {", m.Name)
	for _, f := range m.Fields {
		w.line("\t%s %s", schema.Camel(f.Name), goType(f.Type))
	}
	w.line("}")
	w.line("")
	w.line("// %sType is the type ID of %s.", m.Name, m.Name)
	w.line("const %sType uint32 = %d", m.Name, m.ID)
	g.size(m)
	g.marshal(m)
	g.unmarshal(m)
	w.line("")
	w.line("func (m *%s) ipcgenPut(b []byte) {", m.Name)
	for _, f := range m.FixedFields() {
		g.put(f.Type, "m."+schema.Camel(f.Name), f.Offset)
	}
	w.line("}")
	w.line("")
	w.line("func (m *%s) ipcgenGet(b []byte) error {", m.Name)
	for _, f := range m.FixedFields() {
		g.get(f.Type, "m."+schema.Camel(f.Name), f.Offset)
	}
	w.line("\treturn nil")
	w.line("}")
}

func (g *goGen) size(m *schema.Message) {
	w := &g.w
	terms := []string{fmt.Sprint(m.FixedSize)}
	for _, f := range m.VarFields() {
		terms = append(terms, "4", "len(m."+schema.Camel(f.Name)+")")
	}
	w.line("")
	w.line("// Size returns the encoded size of m.")
	w.line("func (m *%s) Size() int {", m.Name)
	w.line("\treturn %s", strings.Join(terms, " + "))
	w.line("}")
}

func (g *goGen) marshal(m *schema.Message) {
	w := &g.w
	w.line("")
	w.line("// MarshalTo encodes m into b and returns the bytes written.")
	w.line("// It returns 0 when b is shorter than Size or a string or bytes field is longer than 4 GiB.")
	w.line("func (m *%s) MarshalTo(b []byte) int {", m.Name)
	w.line("\tn, _ := m.ipcgenMarshal(b)")
	w.line("\treturn n")
	w.line("}")
	w.line("")
	w.line("// MarshalBinary returns the encoding of m.")
	var fits []string
	for _, f := range m.VarFields() {
		fits = append(fits, fmt.Sprintf("!ipcgenFits(len(m.%s))", schema.Camel(f.Name)))
	}
	tooLong := func(ret string) {
		if len(fits) > 0 {
			w.line("\tif %s {", strings.Join(fits, " || "))
			w.line("\t\treturn %s", ret)
			w.line("\t}")
		}
	}
	w.line("func (m *%s) MarshalBinary() ([]byte, error) {", m.Name)
	tooLong("nil, errIpcgenTooLong")
	w.line("\tb := make([]byte, m.Size())")
	w.line("\tm.ipcgenMarshal(b)")
	w.line("\treturn b, nil")
	w.line("}")
	w.line("")
	w.line("func (m *%s) ipcgenMarshal(b []byte) (int, bool) {", m.Name)
	tooLong("0, false")
	w.line("\tif len(b) < m.Size() {")
	w.line("\t\treturn 0, false")
	w.line("\t}")
	w.line("\tclear(b[:%d])", m.FixedSize)
	w.line("\tm.ipcgenPut(b)")
	if m.Fixed() {
		w.line("\treturn %d, true", m.FixedSize)
	} else {
		w.line("\toff := %d", m.FixedSize)
		for _, f := range m.VarFields() {
			w.line("\toff = ipcgenPutVar(b, off, m.%s)", schema.Camel(f.Name))
		}
		w.line("\treturn off, true")
	}
	w.line("}")
}

func (g *goGen) unmarshal(m *schema.Message) {
	w := &g.w
	w.line("")
	w.line("// UnmarshalBinary decodes b into m. It leaves m unchanged when b is malformed.")
	w.line("func (m *%s) UnmarshalBinary(b []byte) error {", m.Name)
	w.line("\tif len(b) < %d {", m.FixedSize)
	w.line("\t\treturn errIpcgenShort")
	w.line("\t}")
	w.line("\tvar v %s", m.Name)
	w.line("\tif err := v.ipcgenGet(b); err != nil {")
	w.line("\t\treturn err")
	w.line("\t}")
	w.line("\toff := %d", m.FixedSize)
	if !m.Fixed() {
		w.line("\tvar raw []byte")
		w.line("\tvar err error")
	}
	for _, f := range m.VarFields() {
		name := schema.Camel(f.Name)
		w.line("\tif raw, off, err = ipcgenGetVar(b, off); err != nil {")
		w.line("\t\treturn err")
		w.line("\t}")
		if f.Type.Kind == schema.String {
			g.useUTF8 = true
			w.line("\tif !utf8.Valid(raw) {")
			w.line("\t\treturn errIpcgenUTF8")
			w.line("\t}")
			w.line("\tv.%s = string(raw)", name)
		} else {
			w.line("\tv.%s = append([]byte{}, raw...)", name)
		}
	}
	w.line("\tif off != len(b) {")
	w.line("\t\treturn errIpcgenTrailing")
	w.line("\t}")
	w.line("\t*m = v")
	w.line("\treturn nil")
	w.line("}")
}

// put writes the statements that store val at offset off of b.
func (g *goGen) put(t *schema.Type, val string, off int) {
	g.access(t, val, fmt.Sprint(off), true)
}

// get writes the statements that load dst from offset off of b.
func (g *goGen) get(t *schema.Type, dst string, off int) {
	g.access(t, dst, fmt.Sprint(off), false)
}

func (g *goGen) access(t *schema.Type, val, off string, put bool) {
	w := &g.w
	k := t.Kind
	switch {
	case k == schema.Array:
		w.line("\tfor i := range %s {", val)
		g.access(t.Elem, val+"[i]", fmt.Sprintf("%s+%d*i", off, t.Elem.Size()), put)
		w.line("\t}")
	case k == schema.Ref && put:
		w.line("\t%s.ipcgenPut(b[%s:])", val, off)
	case k == schema.Ref:
		w.line("\tif err := %s.ipcgenGet(b[%s:]); err != nil {", val, off)
		w.line("\t\treturn err")
		w.line("\t}")
	case k == schema.Bool && put:
		w.line("\tif %s {", val)
		w.line("\t\tb[%s] = 1", off)
		w.line("\t}")
	case k == schema.Bool:
		w.line("\tif b[%s] > 1 {", off)
		w.line("\t\treturn errIpcgenBool")
		w.line("\t}")
		w.line("\t%s = b[%s] == 1", val, off)
	case k.ScalarSize() == 1 && put:
		w.line("\tb[%s] = byte(%s)", off, val)
	case k.ScalarSize() == 1:
		w.line("\t%s = %s(b[%s])", val, goType(t), off)
	default:
		g.useBinary = true
		bits, fn := goBits(k)
		if put {
			v := val
			if k.IsSigned() {
				v = bits + "(" + val + ")"
			}
			if k.IsFloat() {
				g.useMath = true
				v = fmt.Sprintf("math.Float%sbits(%s)", bits[4:], val)
			}
			w.line("\tbinary.LittleEndian.Put%s(b[%s:], %s)", fn, off, v)
			return
		}
		v := fmt.Sprintf("binary.LittleEndian.%s(b[%s:])", fn, off)
		if k.IsFloat() {
			g.useMath = true
			v = fmt.Sprintf("math.Float%sfrombits(%s)", bits[4:], v)
		} else if k.IsSigned() {
			v = goType(t) + "(" + v + ")"
		}
		w.line("\t%s = %s", val, v)
	}
}
