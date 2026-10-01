// Tests the generated Go code against spec/vectors/schema. `make -C codegen prepare` copies demo.go next to this file.
package demo

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// vectorDir is spec/vectors/schema, seen from this package.
const vectorDir = "../../../spec/vectors/schema"

type message interface {
	Size() int
	MarshalTo([]byte) int
	MarshalBinary() ([]byte, error)
	UnmarshalBinary([]byte) error
}

// messages maps each message of example.ipc to a constructor and its type ID constant.
var messages = map[string]struct {
	make func() message
	id   uint32
}{
	"Scalars": {func() message { return &Scalars{} }, ScalarsType},
	"Vec2":    {func() message { return &Vec2{} }, Vec2Type},
	"Shape":   {func() message { return &Shape{} }, ShapeType},
	"Padded":  {func() message { return &Padded{} }, PaddedType},
	"Record":  {func() message { return &Record{} }, RecordType},
	"Text":    {func() message { return &Text{} }, TextType},
	"Empty":   {func() message { return &Empty{} }, EmptyType},
	"Holder":  {func() message { return &Holder{} }, HolderType},
	"Tagged":  {func() message { return &Tagged{} }, TaggedType},
}

func readJSON(t *testing.T, name string, out any) {
	t.Helper()
	f, err := os.Open(filepath.Join(vectorDir, name))
	require.NoError(t, err)
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.UseNumber()
	require.NoError(t, dec.Decode(out))
}

// camel converts a lower_snake schema name to the Go field name.
func camel(name string) string {
	var b strings.Builder
	for _, part := range strings.Split(name, "_") {
		b.WriteString(strings.ToUpper(part[:1]) + part[1:])
	}
	return b.String()
}

// fill sets v from a values.json value.
func fill(t *testing.T, v reflect.Value, j any, path string) {
	t.Helper()
	switch v.Kind() {
	case reflect.Struct:
		obj, ok := j.(map[string]any)
		require.True(t, ok, "%s: want an object", path)
		require.Len(t, obj, v.NumField(), "%s: field count", path)
		for k, x := range obj {
			f := v.FieldByName(camel(k))
			require.True(t, f.IsValid(), "%s: no Go field for %s", path, k)
			fill(t, f, x, path+"."+k)
		}
	case reflect.Array:
		arr, ok := j.([]any)
		require.True(t, ok && len(arr) == v.Len(), "%s: want %d items", path, v.Len())
		for i, x := range arr {
			fill(t, v.Index(i), x, fmt.Sprintf("%s[%d]", path, i))
		}
	case reflect.Bool:
		b, ok := j.(bool)
		require.True(t, ok, "%s: want a bool", path)
		v.SetBool(b)
	case reflect.String:
		s, ok := j.(string)
		require.True(t, ok, "%s: want a string", path)
		v.SetString(s)
	case reflect.Slice:
		b, err := hex.DecodeString(j.(string))
		require.NoError(t, err, path)
		v.SetBytes(b)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(string(j.(json.Number)), 64)
		require.NoError(t, err, path)
		v.SetFloat(f)
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(fmt.Sprint(j), 10, v.Type().Bits())
		require.NoError(t, err, path)
		v.SetInt(n)
	default:
		n, err := strconv.ParseUint(fmt.Sprint(j), 10, v.Type().Bits())
		require.NoError(t, err, path)
		v.SetUint(n)
	}
}

func TestValues(t *testing.T) {
	var values []struct {
		Message string `json:"message"`
		Value   any    `json:"value"`
	}
	readJSON(t, "values.json", &values)
	require.NotEmpty(t, values)
	for i, e := range values {
		t.Run(fmt.Sprintf("%d_%s", i, e.Message), func(t *testing.T) {
			entry, ok := messages[e.Message]
			require.True(t, ok, "unknown message %s", e.Message)
			want, err := os.ReadFile(filepath.Join(vectorDir, fmt.Sprintf("%d.bin", i)))
			require.NoError(t, err)
			v := entry.make()
			fill(t, reflect.ValueOf(v).Elem(), e.Value, e.Message)

			assert.Equal(t, len(want), v.Size())
			got, err := v.MarshalBinary()
			require.NoError(t, err)
			assert.Equal(t, want, got)
			buf := make([]byte, len(want)+3)
			assert.Equal(t, len(want), v.MarshalTo(buf))
			assert.Equal(t, want, buf[:len(want)])
			if len(want) > 0 {
				assert.Equal(t, 0, v.MarshalTo(buf[:len(want)-1]))
			}

			d := entry.make()
			require.NoError(t, d.UnmarshalBinary(want))
			assert.Equal(t, v, d)
			if len(want) > 0 {
				assert.Error(t, entry.make().UnmarshalBinary(want[:len(want)-1]))
			}
			assert.Error(t, d.UnmarshalBinary(append(append([]byte{}, want...), 0)))
			assert.Equal(t, v, d, "a failed decode must leave the message unchanged")
		})
	}
}

func TestInvalid(t *testing.T) {
	var entries []struct {
		Message string `json:"message"`
		Error   string `json:"error"`
		Hex     string `json:"hex"`
	}
	readJSON(t, "invalid.json", &entries)
	want := map[string]error{
		"short": errIpcgenShort, "length": errIpcgenLength, "trailing": errIpcgenTrailing,
		"bool": errIpcgenBool, "utf8": errIpcgenUTF8,
	}
	for i, e := range entries {
		b, err := hex.DecodeString(e.Hex)
		require.NoError(t, err)
		require.Contains(t, want, e.Error)
		assert.ErrorIs(t, messages[e.Message].make().UnmarshalBinary(b), want[e.Error], "invalid.json entry %d", i)
	}
}

func TestTypeIDs(t *testing.T) {
	assert.Equal(t, uint32(1), messages["Scalars"].id)
	assert.Equal(t, uint32(0xFFFFFFFE), messages["Tagged"].id)
	ids := map[uint32]bool{}
	for _, m := range messages {
		assert.False(t, ids[m.id], "type ID %d is used twice", m.id)
		ids[m.id] = true
	}
}

func TestTooLong(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("a 4 GiB field needs a 64-bit int")
	}
	long := make([]byte, 1<<32)
	_, err := (&Text{Data: long}).MarshalBinary()
	assert.ErrorIs(t, err, errIpcgenTooLong)
	assert.Equal(t, 0, (&Text{Data: long}).MarshalTo(make([]byte, 16)))
}
