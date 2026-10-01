// Package schema parses, checks and lays out an ipcgen schema. spec/schema.md is the contract.
package schema

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

// Kind is the kind of a type.
type Kind int

// The kinds. Scalars come first, in the order of scalarNames.
const (
	Bool Kind = iota
	U8
	I8
	U16
	I16
	U32
	I32
	F32
	U64
	I64
	F64
	String
	Bytes
	Array
	Ref
)

var scalarNames = []string{"bool", "u8", "i8", "u16", "i16", "u32", "i32", "f32", "u64", "i64", "f64"}

// String returns the schema spelling of a scalar, string or bytes kind.
func (k Kind) String() string {
	switch {
	case k <= F64:
		return scalarNames[k]
	case k == String:
		return "string"
	case k == Bytes:
		return "bytes"
	case k == Array:
		return "array"
	}
	return "message"
}

// IsScalar reports whether k is a fixed-size scalar.
func (k Kind) IsScalar() bool { return k <= F64 }

// IsVar reports whether k is string or bytes.
func (k Kind) IsVar() bool { return k == String || k == Bytes }

// IsSigned reports whether k is a signed integer.
func (k Kind) IsSigned() bool { return k == I8 || k == I16 || k == I32 || k == I64 }

// IsFloat reports whether k is f32 or f64.
func (k Kind) IsFloat() bool { return k == F32 || k == F64 }

// ScalarSize returns the size of a scalar kind in bytes. The size is also its alignment.
func (k Kind) ScalarSize() int {
	switch k {
	case U16, I16:
		return 2
	case U32, I32, F32:
		return 4
	case U64, I64, F64:
		return 8
	}
	return 1
}

// Pos is a 1-based line and column in a schema file.
type Pos struct {
	Line, Col int
}

// Type is a field type or an array element type.
type Type struct {
	Kind Kind
	Len  int      // Array only.
	Elem *Type    // Array only.
	Name string   // Ref only: the message name as written.
	Msg  *Message // Ref only: set by Check.
	Pos  Pos
}

// Size returns the size of a fixed type. Check must have succeeded.
func (t *Type) Size() int {
	switch t.Kind {
	case Array:
		return t.Len * t.Elem.Size()
	case Ref:
		return t.Msg.FixedSize
	}
	return t.Kind.ScalarSize()
}

// Align returns the alignment of a fixed type. Check must have succeeded.
func (t *Type) Align() int {
	switch t.Kind {
	case Array:
		return t.Elem.Align()
	case Ref:
		return t.Msg.Align
	}
	return t.Kind.ScalarSize()
}

// String returns the type in schema syntax.
func (t *Type) String() string {
	switch t.Kind {
	case Array:
		return fmt.Sprintf("[%d]%s", t.Len, t.Elem)
	case Ref:
		return t.Name
	}
	return t.Kind.String()
}

// Field is one field of a message.
type Field struct {
	Name   string
	Type   *Type
	Pos    Pos
	Offset int // Fixed fields only: the offset in the fixed section.
}

// IsVar reports whether f is a string or bytes field.
func (f *Field) IsVar() bool { return f.Type.Kind.IsVar() }

// Message is one message declaration.
type Message struct {
	Name      string
	ID        uint32
	Fields    []*Field
	Pos       Pos
	IDPos     Pos
	FixedSize int // Set by Check.
	Align     int // Set by Check.
}

// Fixed reports whether m has no string or bytes field.
func (m *Message) Fixed() bool { return len(m.VarFields()) == 0 }

// FixedFields returns the fields that are not string or bytes, in order.
func (m *Message) FixedFields() []*Field {
	var out []*Field
	for _, f := range m.Fields {
		if !f.IsVar() {
			out = append(out, f)
		}
	}
	return out
}

// VarFields returns the string and bytes fields, in order.
func (m *Message) VarFields() []*Field {
	var out []*Field
	for _, f := range m.Fields {
		if f.IsVar() {
			out = append(out, f)
		}
	}
	return out
}

// Snake returns the lower_snake form of the message name, as C uses it.
func (m *Message) Snake() string { return Snake(m.Name) }

// Schema is a parsed schema.
type Schema struct {
	File     string
	Package  string
	PkgPos   Pos
	Messages []*Message // Declaration order.
	Sorted   []*Message // Every message after the messages it contains. Set by Check.
	byName   map[string]*Message
}

// Lookup returns the message with the given name, or nil.
func (s *Schema) Lookup(name string) *Message { return s.byName[name] }

// Error is one problem at a position in a schema file.
type Error struct {
	File string
	Pos  Pos
	Msg  string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s:%d:%d: %s", e.File, e.Pos.Line, e.Pos.Col, e.Msg)
}

// ErrorList holds every problem Check finds, sorted by position.
type ErrorList []*Error

func (l ErrorList) Error() string {
	lines := make([]string, len(l))
	for i, e := range l {
		lines[i] = e.Error()
	}
	return strings.Join(lines, "\n")
}

func (l ErrorList) sort() {
	sort.SliceStable(l, func(i, j int) bool {
		a, b := l[i].Pos, l[j].Pos
		if a.Line != b.Line {
			return a.Line < b.Line
		}
		return a.Col < b.Col
	})
}

// Load reads, parses and checks the schema file at path.
func Load(path string) (*Schema, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	s, err := Parse(path, src)
	if err != nil {
		return nil, err
	}
	if err := Check(s); err != nil {
		return nil, err
	}
	return s, nil
}
