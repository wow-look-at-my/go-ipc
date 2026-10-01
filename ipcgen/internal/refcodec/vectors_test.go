package refcodec

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
	"github.com/wow-look-at-my/go-ipc/ipcgen/internal/schema"
)

// vectorDir is spec/vectors/schema, seen from this package.
const vectorDir = "../../../spec/vectors/schema"

type valueEntry struct {
	Message string `json:"message"`
	Value   any    `json:"value"`
}

type invalidEntry struct {
	Message string `json:"message"`
	Error   string `json:"error"`
	Hex     string `json:"hex"`
}

func readJSON(t *testing.T, name string, out any) {
	t.Helper()
	f, err := os.Open(filepath.Join(vectorDir, name))
	require.NoError(t, err)
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.UseNumber()
	dec.DisallowUnknownFields()
	require.NoError(t, dec.Decode(out), name)
}

func loadSchema(t *testing.T) *schema.Schema {
	t.Helper()
	s, err := schema.Load(filepath.Join(vectorDir, "example.ipc"))
	require.NoError(t, err)
	return s
}

// TestVectors checks every <index>.bin against the reference encoder. IPCGEN_UPDATE_VECTORS=1 rewrites them.
func TestVectors(t *testing.T) {
	s := loadSchema(t)
	var values []valueEntry
	readJSON(t, "values.json", &values)
	update := os.Getenv("IPCGEN_UPDATE_VECTORS") == "1"
	if update {
		old, err := filepath.Glob(filepath.Join(vectorDir, "*.bin"))
		require.NoError(t, err)
		for _, p := range old {
			require.NoError(t, os.Remove(p))
		}
	}
	for i, e := range values {
		m := s.Lookup(e.Message)
		require.NotNil(t, m, "values.json entry %d names unknown message %s", i, e.Message)
		got, err := Encode(m, e.Value)
		require.NoError(t, err, "values.json entry %d", i)
		require.NoError(t, Decode(m, got), "entry %d does not decode", i)
		path := filepath.Join(vectorDir, fmt.Sprintf("%d.bin", i))
		if update {
			require.NoError(t, os.WriteFile(path, got, 0o644))
		}
		want, err := os.ReadFile(path)
		require.NoError(t, err, "run with IPCGEN_UPDATE_VECTORS=1 to write the vectors")
		assert.Equal(t, want, got, "%s does not match values.json entry %d", path, i)
	}
	bins, err := filepath.Glob(filepath.Join(vectorDir, "*.bin"))
	require.NoError(t, err)
	assert.Len(t, bins, len(values), "every .bin file must match a values.json entry")
}

// TestInvalid checks that the reference decoder rejects each invalid.json entry for the reason it names.
func TestInvalid(t *testing.T) {
	s := loadSchema(t)
	var entries []invalidEntry
	readJSON(t, "invalid.json", &entries)
	for i, e := range entries {
		m := s.Lookup(e.Message)
		require.NotNil(t, m, "invalid.json entry %d names unknown message %s", i, e.Message)
		b, err := hex.DecodeString(e.Hex)
		require.NoError(t, err, "invalid.json entry %d", i)
		var de *DecodeError
		require.True(t, errors.As(Decode(m, b), &de), "invalid.json entry %d decodes without error", i)
		assert.Equal(t, e.Error, de.Kind, "invalid.json entry %d", i)
	}
}

// TestVectorCoverage checks that the vectors exercise every feature that spec/schema.md defines.
func TestVectorCoverage(t *testing.T) {
	s := loadSchema(t)
	kinds := set.New[string]()
	var mark func(t *schema.Type)
	mark = func(t *schema.Type) {
		kinds.Add(t.Kind.String())
		if t.Kind == schema.Array {
			kinds.Add("array of " + t.Elem.Kind.String())
			mark(t.Elem)
		}
	}
	for _, m := range s.Messages {
		end := 0
		for _, f := range m.Fields {
			mark(f.Type)
			if !f.IsVar() {
				if f.Offset > end {
					kinds.Add("padding")
				}
				end = f.Offset + f.Type.Size()
			}
		}
		switch {
		case m.FixedSize > end:
			kinds.Add("tail padding")
		case len(m.Fields) == 0:
			kinds.Add("no fields")
		case m.FixedSize == 0:
			kinds.Add("only variable fields")
		case m.Fixed():
			kinds.Add("no variable fields")
		}
	}
	want := []string{"padding", "tail padding", "no fields", "only variable fields", "no variable fields",
		"string", "bytes", "array", "message", "array of message", "array of bool", "array of f64"}
	for k := schema.Bool; k <= schema.F64; k++ {
		want = append(want, k.String())
	}
	for _, k := range want {
		assert.True(t, kinds.Contains(k), "example.ipc does not cover %s", k)
	}

	var values []valueEntry
	readJSON(t, "values.json", &values)
	var invalid []invalidEntry
	readJSON(t, "invalid.json", &invalid)
	valued, broken, errs := set.New[string](), set.New[string](), set.New[string]()
	for _, e := range values {
		valued.Add(e.Message)
	}
	for _, e := range invalid {
		broken.Add(e.Message)
		errs.Add(e.Error)
	}
	for _, m := range s.Messages {
		assert.True(t, valued.Contains(m.Name), "values.json has no value for %s", m.Name)
		assert.True(t, broken.Contains(m.Name), "invalid.json has no entry for %s", m.Name)
	}
	for _, k := range []string{ErrShort, ErrLength, ErrTrailing, ErrBool, ErrUTF8} {
		assert.True(t, errs.Contains(k), "invalid.json has no %s entry", k)
	}
}
