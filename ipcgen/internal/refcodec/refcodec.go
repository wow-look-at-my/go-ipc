// Package refcodec is the reference encoder and decoder of spec/schema.md. It works from a checked schema and JSON values, and it writes and checks the conformance vectors.
package refcodec

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/wow-look-at-my/go-ipc/ipcgen/internal/schema"
)

// Encode returns the encoding of v as message m. v is decoded JSON with json.Number for numbers. A u64 or i64 is a decimal string, bytes are a lower-case hex string, and every field must be present.
func Encode(m *schema.Message, v any) ([]byte, error) {
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s: value is %T, want an object", m.Name, v)
	}
	b := make([]byte, m.FixedSize)
	if err := putFixed(b, m, obj, m.Name); err != nil {
		return nil, err
	}
	for _, f := range m.VarFields() {
		raw, err := varBytes(f, obj[f.Name], m.Name+"."+f.Name)
		if err != nil {
			return nil, err
		}
		b = binary.LittleEndian.AppendUint32(b, uint32(len(raw)))
		b = append(b, raw...)
	}
	return b, nil
}

func checkKeys(m *schema.Message, obj map[string]any, path string) error {
	for _, f := range m.Fields {
		if _, ok := obj[f.Name]; !ok {
			return fmt.Errorf("%s: field %s is missing", path, f.Name)
		}
	}
	if len(obj) != len(m.Fields) {
		for k := range obj {
			found := false
			for _, f := range m.Fields {
				found = found || f.Name == k
			}
			if !found {
				return fmt.Errorf("%s: message %s has no field %s", path, m.Name, k)
			}
		}
	}
	return nil
}

func putFixed(b []byte, m *schema.Message, obj map[string]any, path string) error {
	if err := checkKeys(m, obj, path); err != nil {
		return err
	}
	for _, f := range m.FixedFields() {
		if err := put(b[f.Offset:], f.Type, obj[f.Name], path+"."+f.Name); err != nil {
			return err
		}
	}
	return nil
}

func put(b []byte, t *schema.Type, v any, path string) error {
	switch t.Kind {
	case schema.Array:
		arr, ok := v.([]any)
		if !ok || len(arr) != t.Len {
			return fmt.Errorf("%s: want an array of %d items", path, t.Len)
		}
		for i, e := range arr {
			if err := put(b[i*t.Elem.Size():], t.Elem, e, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
		return nil
	case schema.Ref:
		obj, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: want an object", path)
		}
		return putFixed(b, t.Msg, obj, path)
	case schema.Bool:
		x, ok := v.(bool)
		if !ok {
			return fmt.Errorf("%s: want a bool", path)
		}
		if x {
			b[0] = 1
		}
		return nil
	}
	bits, err := scalarBits(t.Kind, v)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	for i := 0; i < t.Kind.ScalarSize(); i++ {
		b[i] = byte(bits >> (8 * i))
	}
	return nil
}

// scalarBits returns the little-endian bit pattern of a number scalar.
func scalarBits(k schema.Kind, v any) (uint64, error) {
	var text string
	switch x := v.(type) {
	case string:
		if k != schema.U64 && k != schema.I64 {
			return 0, fmt.Errorf("want a JSON number for %s, found a string", k)
		}
		text = x
	case json.Number:
		if k == schema.U64 || k == schema.I64 {
			return 0, fmt.Errorf("want a decimal string for %s, found a number", k)
		}
		text = string(x)
	default:
		return 0, fmt.Errorf("want a %s, found %T", k, v)
	}
	size := k.ScalarSize() * 8
	switch {
	case k == schema.F32:
		f, err := strconv.ParseFloat(text, 64)
		if err != nil || float64(float32(f)) != f {
			return 0, fmt.Errorf("%s is not exactly an f32", text)
		}
		return uint64(math.Float32bits(float32(f))), nil
	case k == schema.F64:
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return 0, fmt.Errorf("%s is not an f64", text)
		}
		return math.Float64bits(f), nil
	case k.IsSigned():
		n, err := strconv.ParseInt(text, 10, size)
		if err != nil {
			return 0, fmt.Errorf("%s is not an %s", text, k)
		}
		return uint64(n), nil
	}
	n, err := strconv.ParseUint(text, 10, size)
	if err != nil {
		return 0, fmt.Errorf("%s is not a %s", text, k)
	}
	return n, nil
}

func varBytes(f *schema.Field, v any, path string) ([]byte, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("%s: want a string", path)
	}
	if f.Type.Kind == schema.String {
		if !utf8.ValidString(s) {
			return nil, fmt.Errorf("%s: not valid UTF-8", path)
		}
		return []byte(s), nil
	}
	raw, err := hex.DecodeString(s)
	if err != nil || hex.EncodeToString(raw) != s {
		return nil, fmt.Errorf("%s: want lower-case hex", path)
	}
	return raw, nil
}

// Error kinds that Decode reports. invalid.json names them in its "error" key.
const (
	ErrShort    = "short"
	ErrLength   = "length"
	ErrTrailing = "trailing"
	ErrBool     = "bool"
	ErrUTF8     = "utf8"
)

// DecodeError is a malformed input. Kind is one of the Err constants.
type DecodeError struct {
	Kind string
}

func (e *DecodeError) Error() string { return "malformed input: " + e.Kind }

// Decode checks that b is a well-formed encoding of m. It returns a *DecodeError when b is malformed.
func Decode(m *schema.Message, b []byte) error {
	if len(b) < m.FixedSize {
		return &DecodeError{ErrShort}
	}
	if err := checkFixed(b, m); err != nil {
		return err
	}
	off := m.FixedSize
	for _, f := range m.VarFields() {
		if len(b)-off < 4 {
			return &DecodeError{ErrLength}
		}
		n := uint64(binary.LittleEndian.Uint32(b[off:]))
		off += 4
		if n > uint64(len(b)-off) {
			return &DecodeError{ErrLength}
		}
		if f.Type.Kind == schema.String && !utf8.Valid(b[off:off+int(n)]) {
			return &DecodeError{ErrUTF8}
		}
		off += int(n)
	}
	if off != len(b) {
		return &DecodeError{ErrTrailing}
	}
	return nil
}

func checkFixed(b []byte, m *schema.Message) error {
	for _, f := range m.FixedFields() {
		if err := checkType(b[f.Offset:], f.Type); err != nil {
			return err
		}
	}
	return nil
}

func checkType(b []byte, t *schema.Type) error {
	switch t.Kind {
	case schema.Bool:
		if b[0] > 1 {
			return &DecodeError{ErrBool}
		}
	case schema.Ref:
		return checkFixed(b, t.Msg)
	case schema.Array:
		for i := 0; i < t.Len; i++ {
			if err := checkType(b[i*t.Elem.Size():], t.Elem); err != nil {
				return err
			}
		}
	}
	return nil
}
