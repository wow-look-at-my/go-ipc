package gen

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-ipc/ipcgen/internal/schema"
)

const testSchema = `package demo
message Vec2 = 2 {
	x f32
	y f32
}
message Record = 5 {
	id u64
	pos Vec2
	pts [2]Vec2
	tags [4]u16
	flags [2]bool
	n i8
	ok bool
	label string
	blob bytes
}
message Text = 6 {
	title string
}
message Empty = 7 {
}
`

func load(t *testing.T) *schema.Schema {
	t.Helper()
	s, err := schema.Parse("dir/test.ipc", []byte(testSchema))
	require.NoError(t, err)
	require.NoError(t, schema.Check(s))
	return s
}

func generate(t *testing.T, lang string) string {
	t.Helper()
	out, err := Generate(lang, load(t))
	require.NoError(t, err)
	return string(out)
}

func TestLanguages(t *testing.T) {
	assert.Equal(t, []string{"c", "cpp", "go", "json", "py"}, Languages())
	_, err := Generate("rust", load(t))
	assert.ErrorContains(t, err, `unknown language "rust"`)
}

func TestHeaderLine(t *testing.T) {
	for _, lang := range []string{"go", "c", "cpp", "py"} {
		first, _, _ := strings.Cut(generate(t, lang), "\n")
		assert.Contains(t, first, Header, lang)
	}
}

func TestGo(t *testing.T) {
	src := generate(t, "go")
	_, err := parser.ParseFile(token.NewFileSet(), "demo.go", src, parser.AllErrors)
	require.NoError(t, err)
	for _, want := range []string{
		"package demo",
		"type Record struct {",
		"Pts   [2]Vec2",
		"Blob  []byte",
		"const RecordType uint32 = 5",
		"func (m *Record) Size() int {\n\treturn 48 + 4 + len(m.Label) + 4 + len(m.Blob)",
		"func (m *Record) MarshalTo(b []byte) int {",
		"func (m *Record) MarshalBinary() ([]byte, error) {",
		"func (m *Record) UnmarshalBinary(b []byte) error {",
		"binary.LittleEndian.PutUint32(b[0:], math.Float32bits(m.X))",
		"m.Pos.ipcgenPut(b[8:])",
		"\"unicode/utf8\"",
		"if b[43] > 1 {",
		"m.N = int8(b[42])",
		"const EmptyType uint32 = 7",
	} {
		assert.Contains(t, src, want)
	}
}

func TestGoImportsOnlyWhatItUses(t *testing.T) {
	s, err := schema.Parse("t.ipc", []byte("package p\nmessage A = 1 {\n\tb bool\n\tc u8\n}\n"))
	require.NoError(t, err)
	require.NoError(t, schema.Check(s))
	out, err := Generate("go", s)
	require.NoError(t, err)
	assert.NotContains(t, string(out), "\"math\"")
	assert.NotContains(t, string(out), "\"unicode/utf8\"")
}

func TestC(t *testing.T) {
	src := generate(t, "c")
	for _, want := range []string{
		"#ifndef DEMO_IPCGEN_H",
		"#define DEMO_RECORD_TYPE UINT32_C(5)",
		"#define DEMO_RECORD_FIXED_SIZE 48",
		"typedef struct demo_record {",
		"\tdemo_vec2 pts[2];",
		"\tuint16_t tags[4];",
		"\tdemo_ipcgen_str label;",
		"\tdemo_ipcgen_bytes blob;",
		"\tchar ipcgen_unused;",
		"static inline size_t demo_record_size(const demo_record *m) {",
		"static inline size_t demo_record_encode(const demo_record *m, uint8_t *buf, size_t cap) {",
		"static inline int demo_record_decode(demo_record *m, const uint8_t *buf, size_t len) {",
		"demo_ipcgen_vec2_put(&m->pts[i], p + 16 + i * 8);",
		"return DEMO_IPCGEN_EUTF8;",
	} {
		assert.Contains(t, src, want)
	}
	assert.Less(t, strings.Index(src, "typedef struct demo_vec2"), strings.Index(src, "typedef struct demo_record"),
		"a contained message is declared first")
}

func TestCpp(t *testing.T) {
	src := generate(t, "cpp")
	for _, want := range []string{
		"#pragma once",
		"namespace demo {",
		"struct Record {",
		"\tstatic constexpr std::uint32_t type_id = 5;",
		"\tstatic constexpr std::size_t fixed_size = 48;",
		"\tstd::array<Vec2, 2> pts{};",
		"\tstd::string label;",
		"\tstd::vector<std::byte> blob;",
		"\tstd::size_t encode(std::span<std::byte> out) const noexcept;",
		"\tstatic std::optional<Record> decode(std::span<const std::byte> in);",
		"bool operator==(const Record &) const = default;",
		"m.n = get<std::int8_t>(p + 42);",
		"decode returns std::nullopt",
	} {
		assert.Contains(t, src, want)
	}
}

func TestPython(t *testing.T) {
	src := generate(t, "py")
	for _, want := range []string{
		"from __future__ import annotations",
		"@_dc.dataclass\nclass Record:\n\tTYPE_ID = 5\n\tFIXED_SIZE = 48",
		"\tpts: _t.List[Vec2] = _dc.field(default_factory=lambda: [Vec2() for _ in range(2)])",
		"\ttags: _t.List[int] = _dc.field(default_factory=lambda: [0] * 4)",
		"\tflags: _t.List[bool] = _dc.field(default_factory=lambda: [False] * 2)",
		"\tpos: Vec2 = _dc.field(default_factory=Vec2)",
		"\tlabel: str = \"\"",
		"\tblob: bytes = b\"\"",
		"\tdef encode(self) -> bytes:",
		"\t@classmethod\n\tdef decode(cls, buf: _t.Any) -> Record:",
		"_st.pack_into(\"<4H\", buf, off + 32, *_arr(self.tags, 4))",
		"[_bool(b) for b in buf[off + 40:off + 42]]",
		"class Empty:\n\tTYPE_ID = 7\n\tFIXED_SIZE = 0\n\n\tdef encode",
		"\t\treturn cls()",
		"MESSAGES: _t.Dict[int, _t.Any] = {\n\tVec2.TYPE_ID: Vec2,",
	} {
		assert.Contains(t, src, want)
	}
	for _, banned := range []string{"match ", "slots=", "kw_only", "removeprefix", "| None"} {
		assert.NotContains(t, src, banned, "the module must run on CPython 3.8")
	}
}

func TestJSON(t *testing.T) {
	var out jsonSchema
	require.NoError(t, json.Unmarshal([]byte(generate(t, "json")), &out))
	assert.Equal(t, Header, out.Comment)
	assert.Equal(t, "demo", out.Package)
	require.Len(t, out.Messages, 4)
	rec := out.Messages[1]
	assert.Equal(t, "Record", rec.Name)
	assert.Equal(t, "record", rec.Snake)
	assert.Equal(t, 48, rec.FixedSize)
	assert.Equal(t, 8, rec.Align)
	pts := rec.Fields[2]
	assert.Equal(t, "array", pts.Type.Kind)
	assert.Equal(t, 2, pts.Type.Len)
	assert.Equal(t, "Vec2", pts.Type.Elem.Message)
	assert.Equal(t, 16, *pts.Offset)
	assert.Equal(t, 16, *pts.Size)
	label := rec.Fields[7]
	assert.Equal(t, "string", label.Type.Kind)
	assert.Nil(t, label.Offset)
}
