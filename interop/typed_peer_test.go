package interop

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	ipc "github.com/wow-look-at-my/go-ipc"
	"github.com/wow-look-at-my/go-ipc/interop/internal/demo"
)

// specDirEnv names the spec directory. The typed roles read values.json and the .bin files under it.
const specDirEnv = "GOIPC_SPEC_DIR"

type message interface {
	Size() int
	MarshalBinary() ([]byte, error)
	UnmarshalBinary([]byte) error
}

type messageKind struct {
	make func() message
	id   uint32
}

// messages maps each message of example.ipc to a constructor and its type ID.
var messages = map[string]messageKind{
	"Scalars": {func() message { return &demo.Scalars{} }, demo.ScalarsType},
	"Vec2":    {func() message { return &demo.Vec2{} }, demo.Vec2Type},
	"Shape":   {func() message { return &demo.Shape{} }, demo.ShapeType},
	"Padded":  {func() message { return &demo.Padded{} }, demo.PaddedType},
	"Record":  {func() message { return &demo.Record{} }, demo.RecordType},
	"Text":    {func() message { return &demo.Text{} }, demo.TextType},
	"Empty":   {func() message { return &demo.Empty{} }, demo.EmptyType},
	"Holder":  {func() message { return &demo.Holder{} }, demo.HolderType},
	"Tagged":  {func() message { return &demo.Tagged{} }, demo.TaggedType},
}

// messageByID returns the constructor for a type ID.
func messageByID(id uint32) (func() message, bool) {
	for _, k := range messages {
		if k.id == id {
			return k.make, true
		}
	}
	return nil, false
}

type typedEntry struct {
	Message string `json:"message"`
	Value   any    `json:"value"`
}

func schemaDir() (string, error) {
	dir := os.Getenv(specDirEnv)
	if dir == "" {
		return "", fmt.Errorf("%s is not set; it must name the spec directory", specDirEnv)
	}
	return filepath.Join(dir, "vectors", "schema"), nil
}

func readValues() ([]typedEntry, error) {
	dir, err := schemaDir()
	if err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(dir, "values.json"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.UseNumber()
	var values []typedEntry
	if err := dec.Decode(&values); err != nil {
		return nil, fmt.Errorf("values.json: %w", err)
	}
	if len(values) == 0 {
		return nil, errors.New("values.json has no entries")
	}
	return values, nil
}

// build returns entry e as its generated type, with its type ID.
func (e typedEntry) build() (message, uint32, error) {
	k, ok := messages[e.Message]
	if !ok {
		return nil, 0, fmt.Errorf("unknown message %s", e.Message)
	}
	m := k.make()
	if err := fill(reflect.ValueOf(m).Elem(), e.Value, e.Message); err != nil {
		return nil, 0, err
	}
	return m, k.id, nil
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
func fill(v reflect.Value, j any, path string) error {
	switch v.Kind() {
	case reflect.Struct:
		obj, ok := j.(map[string]any)
		if !ok || len(obj) != v.NumField() {
			return fmt.Errorf("%s: want an object with %d fields", path, v.NumField())
		}
		for k, x := range obj {
			f := v.FieldByName(camel(k))
			if !f.IsValid() {
				return fmt.Errorf("%s: no Go field for %s", path, k)
			}
			if err := fill(f, x, path+"."+k); err != nil {
				return err
			}
		}
	case reflect.Array:
		arr, ok := j.([]any)
		if !ok || len(arr) != v.Len() {
			return fmt.Errorf("%s: want %d items", path, v.Len())
		}
		for i, x := range arr {
			if err := fill(v.Index(i), x, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	case reflect.Bool:
		b, ok := j.(bool)
		if !ok {
			return fmt.Errorf("%s: want a bool", path)
		}
		v.SetBool(b)
	case reflect.String:
		s, ok := j.(string)
		if !ok {
			return fmt.Errorf("%s: want a string", path)
		}
		v.SetString(s)
	case reflect.Slice:
		s, ok := j.(string)
		if !ok {
			return fmt.Errorf("%s: want a hex string", path)
		}
		b, err := hex.DecodeString(s)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		v.SetBytes(b)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(fmt.Sprint(j), 64)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		v.SetFloat(f)
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(fmt.Sprint(j), 10, v.Type().Bits())
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		v.SetInt(n)
	default:
		n, err := strconv.ParseUint(fmt.Sprint(j), 10, v.Type().Bits())
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		v.SetUint(n)
	}
	return nil
}

func peerTypedSend(ctx context.Context, name string) error {
	values, err := readValues()
	if err != nil {
		return err
	}
	q, err := ipc.OpenQueue(name)
	if err != nil {
		return err
	}
	defer q.Close()
	for i, e := range values {
		m, id, err := e.build()
		if err != nil {
			return fmt.Errorf("entry %d: %w", i, err)
		}
		b, err := m.MarshalBinary()
		if err != nil {
			return fmt.Errorf("entry %d: %w", i, err)
		}
		if err := q.SendTyped(ctx, id, b); err != nil {
			return fmt.Errorf("send entry %d: %w", i, err)
		}
	}
	return nil
}

func peerTypedRecv(ctx context.Context, name string, capacity int) error {
	values, err := readValues()
	if err != nil {
		return err
	}
	dir, err := schemaDir()
	if err != nil {
		return err
	}
	q, err := ipc.CreateQueue(name, ipc.WithCapacity(capacity))
	if err != nil {
		return err
	}
	defer q.Unlink()
	defer q.Close()
	if err := ready(); err != nil {
		return err
	}

	buf := make([]byte, q.MaxMessageSize())
	for i, e := range values {
		k, ok := messages[e.Message]
		if !ok {
			return fmt.Errorf("entry %d: unknown message %s", i, e.Message)
		}
		typ, msg, err := q.RecvInto(ctx, buf)
		if err != nil {
			return fmt.Errorf("entry %d: %w", i, err)
		}
		if typ != k.id {
			return fmt.Errorf("entry %d: type %d, want %d", i, typ, k.id)
		}
		mk, ok := messageByID(typ)
		if !ok {
			return fmt.Errorf("entry %d: no message has type ID %d", i, typ)
		}
		m := mk()
		if err := m.UnmarshalBinary(msg); err != nil {
			return fmt.Errorf("entry %d: decode: %w", i, err)
		}
		got, err := m.MarshalBinary()
		if err != nil {
			return fmt.Errorf("entry %d: encode: %w", i, err)
		}
		path := filepath.Join(dir, strconv.Itoa(i)+".bin")
		want, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf("entry %d: re-encoding differs from %s", i, path)
		}
	}
	_, err = fmt.Fprintf(os.Stdout, "ok %d\n", len(values))
	return err
}
