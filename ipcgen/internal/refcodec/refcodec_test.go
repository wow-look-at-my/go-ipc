package refcodec

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-ipc/ipcgen/internal/schema"
)

func value(t *testing.T, src string) any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(src))
	dec.UseNumber()
	var v any
	require.NoError(t, dec.Decode(&v))
	return v
}

func TestEncodeRejectsBadValues(t *testing.T) {
	s := loadSchema(t)
	cases := []struct{ msg, val, want string }{
		{"Vec2", `[1, 2]`, "want an object"},
		{"Vec2", `{"x": 1}`, "field y is missing"},
		{"Vec2", `{"x": 1, "y": 2, "z": 3}`, "has no field z"},
		{"Vec2", `{"x": 0.1, "y": 2}`, "0.1 is not exactly an f32"},
		{"Vec2", `{"x": "1", "y": 2}`, "found a string"},
		{"Vec2", `{"x": true, "y": 2}`, "want a f32"},
		{"Padded", `{"big": 1, "small": 2}`, "want a decimal string for u64"},
		{"Padded", `{"big": "18446744073709551616", "small": 2}`, "is not a u64"},
		{"Padded", `{"big": "1", "small": 256}`, "256 is not a u8"},
		{"Tagged", `{"level": 128, "name": ""}`, "128 is not an i8"},
		{"Tagged", `{"level": 0, "name": 5}`, "want a string"},
		{"Record", `{"id": "1", "pos": 3, "tags": [1,2,3,4], "ok": true, "label": "", "blob": ""}`, "Record.pos: want an object"},
		{"Record", `{"id": "1", "pos": {"x": 0, "y": 0}, "tags": [1,2,3], "ok": true, "label": "", "blob": ""}`, "want an array of 4 items"},
		{"Record", `{"id": "1", "pos": {"x": 0, "y": 0}, "tags": [1,2,3,4], "ok": 1, "label": "", "blob": ""}`, "want a bool"},
		{"Record", `{"id": "1", "pos": {"x": 0, "y": 0}, "tags": [1,2,3,4], "ok": true, "label": "", "blob": "ABCD"}`, "want lower-case hex"},
		{"Record", `{"id": "1", "pos": {"x": 0, "y": 0}, "tags": [1,2,3,70000], "ok": true, "label": "", "blob": ""}`, "Record.tags[3]: 70000 is not a u16"},
		{"Scalars", `{"f_bool": false, "f_u64": "0", "f_i8": 0, "f_i16": 0, "f_u8": 0, "f_i32": 0, "f_u16": 0, "f_f64": "x", "f_u32": 0, "f_f32": 0, "f_i64": "0"}`, "found a string"},
	}
	for _, c := range cases {
		_, err := Encode(s.Lookup(c.msg), value(t, c.val))
		require.Error(t, err, c.val)
		assert.Contains(t, err.Error(), c.want, c.val)
	}
}

func TestEncodeRejectsInvalidUTF8(t *testing.T) {
	s := loadSchema(t)
	_, err := Encode(s.Lookup("Tagged"), map[string]any{"level": json.Number("0"), "name": "\xff"})
	assert.ErrorContains(t, err, "not valid UTF-8")
}

func TestDecodeError(t *testing.T) {
	m := &schema.Message{Name: "M"}
	assert.EqualError(t, Decode(m, []byte{1}), "malformed input: trailing")
}
