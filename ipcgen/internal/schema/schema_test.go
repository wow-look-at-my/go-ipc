package schema

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parseCheck(src string) (*Schema, error) {
	s, err := Parse("t.ipc", []byte(src))
	if err != nil {
		return nil, err
	}
	return s, Check(s)
}

func TestParseErrors(t *testing.T) {
	cases := []struct{ src, want string }{
		{"", "t.ipc:1:1: expected 'package', found end of file"},
		{"message A = 1 {\n}\n", "t.ipc:1:1: expected 'package', found \"message\""},
		{"package\n", "t.ipc:1:8: expected package name, found newline"},
		{"package demo extra\n", "t.ipc:1:14: expected newline after package name, found \"extra\""},
		{"package demo\npackage again\n", "t.ipc:2:1: package is already declared"},
		{"package demo\nfoo\n", "t.ipc:2:1: expected 'message', found \"foo\""},
		{"package demo\nmessage = 1 {\n}\n", "t.ipc:2:9: expected message name, found '='"},
		{"package demo\nmessage A 1 {\n}\n", "t.ipc:2:11: expected '=', found \"1\""},
		{"package demo\nmessage A = x {\n}\n", "t.ipc:2:13: expected type ID, found \"x\""},
		{"package demo\nmessage A = 0x100000000 {\n}\n", "t.ipc:2:13: type ID 0x100000000 is not a u32"},
		{"package demo\nmessage A = 12ab {\n}\n", "t.ipc:2:13: type ID 12ab is not a u32"},
		{"package demo\nmessage A = 1\n", "t.ipc:2:14: expected '{', found newline"},
		{"package demo\nmessage A = 1 {\n\t[\n}\n", "t.ipc:3:2: expected field name or '}', found '['"},
		{"package demo\nmessage A = 1 {\n\tx\n}\n", "t.ipc:3:3: expected type, found newline"},
		{"package demo\nmessage A = 1 {\n\tx u8 y\n}\n", "t.ipc:3:7: expected newline after field, found \"y\""},
		{"package demo\nmessage A = 1 {\n\tx [\n}\n", "t.ipc:3:5: expected array length, found newline"},
		{"package demo\nmessage A = 1 {\n\tx [3 u8\n}\n", "t.ipc:3:7: expected ']', found \"u8\""},
		{"package demo\nmessage A = 1 {\n\tx [99999999999]u8\n}\n", "t.ipc:3:5: array length 99999999999 is too large"},
		{"package demo\nmessage A = 1 {\n\tx [2]\n}\n", "t.ipc:3:7: expected type, found newline"},
		{"package demo\nmessage A = 1 {\n}x\n", "t.ipc:3:2: expected newline after '}', found \"x\""},
		{"package demo\nmessage A = 1 {\n\tx u8;\n}\n", "t.ipc:3:6: unexpected character ';'"},
		{"package demo # é\nmessage Ä = 1 {\n}\n", "t.ipc:2:9: unexpected character 'Ä'"},
	}
	for _, c := range cases {
		_, err := Parse("t.ipc", []byte(c.src))
		require.Error(t, err, c.src)
		assert.Equal(t, c.want, err.Error(), c.src)
	}
}

func TestCheckErrors(t *testing.T) {
	cases := []struct{ src, want string }{
		{"package Demo\n", "package name \"Demo\" must be lower_snake"},
		{"package int\n", "package name \"int\" must be lower_snake and not a reserved word"},
		{"package d\nmessage a = 1 {\n}\n", "message name \"a\" must be UpperCamel"},
		{"package d\nmessage MESSAGES = 1 {\n}\n", "message name \"MESSAGES\" is reserved"},
		{"package d\nmessage IpcgenX = 1 {\n}\n", "message name \"IpcgenX\" is reserved"},
		{"package d\nmessage A = 1 {\n}\nmessage A = 2 {\n}\n", "t.ipc:4:9: message A is already declared at line 2"},
		{"package d\nmessage AB = 1 {\n}\nmessage Ab = 2 {\n}\n", "message AB and message Ab both map to the C name ab"},
		{"package d\nmessage A = 1 {\n}\nmessage AType = 2 {\n}\n", "message name AType clashes with the Go constant for message A"},
		{"package d\nmessage A = 0xFFFFFFFF {\n}\n", "t.ipc:2:13: type ID 0xFFFFFFFF is reserved"},
		{"package d\nmessage A = 7 {\n}\nmessage B = 7 {\n}\n", "t.ipc:4:13: type ID 7 is already used by message A"},
		{"package d\nmessage A = 1 {\n\tBad u8\n}\n", "field name \"Bad\" must be lower_snake"},
		{"package d\nmessage A = 1 {\n\tx__y u8\n}\n", "field name \"x__y\" must be lower_snake"},
		{"package d\nmessage A = 1 {\n\tclass u8\n}\n", "field name \"class\" is reserved"},
		{"package d\nmessage A = 1 {\n\tsize u8\n}\n", "field name \"size\" is reserved"},
		{"package d\nmessage A = 1 {\n\tmarshal_to u8\n}\n", "field name \"marshal_to\" is reserved"},
		{"package d\nmessage A = 1 {\n\tipcgen_x u8\n}\n", "field name \"ipcgen_x\" is reserved"},
		{"package d\nmessage A = 1 {\n\tx u8\n\tx u16\n}\n", "t.ipc:4:2: field x is already declared in message A"},
		{"package d\nmessage A = 1 {\n\ta_1b u8\n\ta1b u8\n}\n", "fields a_1b and a1b both map to the Go name A1b"},
		{"package d\nmessage A = 1 {\n\tx Nope\n}\n", "t.ipc:3:4: unknown type Nope"},
		{"package d\nmessage A = 1 {\n\tx [0]u8\n}\n", "array length must be at least 1"},
		{"package d\nmessage A = 1 {\n\tx [2]string\n}\n", "array element string must be a scalar or a fixed message"},
		{"package d\nmessage A = 1 {\n\tx [2][2]u8\n}\n", "array element [2]u8 must be a scalar or a fixed message"},
		{"package d\nmessage A = 1 {\n\tx [2]Nope\n}\n", "unknown type Nope"},
		{"package d\nmessage A = 1 {\n\ts string\n}\nmessage B = 2 {\n\ta A\n}\n", "field a: message A has a string or bytes field, so it cannot be a field type"},
		{"package d\nmessage A = 1 {\n\ts bytes\n}\nmessage B = 2 {\n\ta [1]A\n}\n", "array element A has a string or bytes field, so it cannot be an array element"},
		{"package d\nmessage A = 1 {\n\ta A\n}\n", "message A contains itself: A -> A"},
		{"package d\nmessage A = 1 {\n\tb [2]B\n}\nmessage B = 2 {\n\ta A\n}\n", "message A contains itself: A -> B -> A"},
		{"package d\nmessage A = 1 {\n\tx [2147483647]u64\n}\n", "message A: the fixed section is larger than 2147483647 bytes"},
	}
	for _, c := range cases {
		_, err := parseCheck(c.src)
		require.Error(t, err, c.src)
		assert.Contains(t, err.Error(), c.want, c.src)
	}
}

func TestCheckReportsEveryError(t *testing.T) {
	_, err := parseCheck("package d\nmessage B = 1 {\n\tx Nope\n}\nmessage a = 1 {\n}\n")
	var list ErrorList
	require.ErrorAs(t, err, &list)
	require.Len(t, list, 3)
	assert.Equal(t, []Pos{{3, 4}, {5, 9}, {5, 13}}, []Pos{list[0].Pos, list[1].Pos, list[2].Pos}, "errors are sorted by position")
	assert.Contains(t, list[2].Msg, "type ID 1 is already used by message B")
}

func TestLayout(t *testing.T) {
	s, err := parseCheck(`package demo
message Outer = 2 {
	flag bool
	inner Inner
	pts [3]Inner
	tail string
	n u16
}
message Inner = 0x10 {
	a u8
	b u64
	c i16
}
message Empty = 3 {
}
message Holder = 4 {
	a u8
	e Empty
	b [2]Empty
	c u32
}
`)
	require.NoError(t, err)
	inner, outer, empty, holder := s.Lookup("Inner"), s.Lookup("Outer"), s.Lookup("Empty"), s.Lookup("Holder")
	assert.Equal(t, uint32(16), inner.ID)
	assert.Equal(t, 24, inner.FixedSize)
	assert.Equal(t, 8, inner.Align)
	assert.Equal(t, []int{0, 8, 16}, offsets(inner))
	assert.Equal(t, 112, outer.FixedSize)
	assert.Equal(t, []int{0, 8, 32, 0, 104}, offsets(outer))
	assert.Equal(t, 0, empty.FixedSize)
	assert.Equal(t, 1, empty.Align)
	assert.Equal(t, []int{0, 1, 1, 4}, offsets(holder))
	assert.Equal(t, 8, holder.FixedSize)
	assert.Equal(t, []*Message{inner, outer, empty, holder}, s.Sorted, "a contained message comes first")
	assert.False(t, outer.Fixed())
	assert.True(t, inner.Fixed())
	assert.Len(t, outer.VarFields(), 1)
	assert.Equal(t, "[3]Inner", outer.Fields[2].Type.String())
	assert.Nil(t, s.Lookup("Missing"))
}

func offsets(m *Message) []int {
	var out []int
	for _, f := range m.Fields {
		out = append(out, f.Offset)
	}
	return out
}

func TestKinds(t *testing.T) {
	assert.Equal(t, "u16", U16.String())
	assert.Equal(t, "string", String.String())
	assert.Equal(t, "bytes", Bytes.String())
	assert.Equal(t, "array", Array.String())
	assert.Equal(t, "message", Ref.String())
	assert.True(t, F64.IsScalar())
	assert.False(t, String.IsScalar())
	assert.True(t, I32.IsSigned())
	assert.False(t, U32.IsSigned())
	assert.True(t, F32.IsFloat())
	sizes := map[Kind]int{Bool: 1, U8: 1, I8: 1, U16: 2, I16: 2, U32: 4, I32: 4, F32: 4, U64: 8, I64: 8, F64: 8}
	for k, n := range sizes {
		assert.Equal(t, n, k.ScalarSize(), k.String())
	}
}

func TestNames(t *testing.T) {
	for in, want := range map[string]string{"Vec2": "vec2", "PointCloud": "point_cloud", "HTTPServer": "http_server", "A": "a", "Point3D": "point3_d"} {
		assert.Equal(t, want, Snake(in), in)
	}
	for in, want := range map[string]string{"label_text": "LabelText", "x": "X", "f_u64": "FU64"} {
		assert.Equal(t, want, Camel(in), in)
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	_, err := Load(filepath.Join(dir, "missing.ipc"))
	assert.Error(t, err)

	bad := filepath.Join(dir, "bad.ipc")
	require.NoError(t, os.WriteFile(bad, []byte("package d\nmessage A = 1 {\n\tx Nope\n}\n"), 0o644))
	_, err = Load(bad)
	require.Error(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), bad+":3:4: "), err.Error())

	syntax := filepath.Join(dir, "syntax.ipc")
	require.NoError(t, os.WriteFile(syntax, []byte("nope\n"), 0o644))
	_, err = Load(syntax)
	assert.Error(t, err)

	s, err := Load("../../../spec/vectors/schema/example.ipc")
	require.NoError(t, err)
	assert.Equal(t, "demo", s.Package)
}
