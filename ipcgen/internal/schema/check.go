package schema

import (
	"fmt"
	"strings"
)

// maxFixedSize bounds a fixed section so every size fits in an int32.
const maxFixedSize = 1<<31 - 1

type checker struct {
	s    *Schema
	errs ErrorList
}

func (c *checker) errorf(pos Pos, format string, args ...any) {
	c.errs = append(c.errs, &Error{c.s.File, pos, fmt.Sprintf(format, args...)})
}

// Check validates s, resolves message references and computes the layout. It returns an ErrorList that holds every problem.
func Check(s *Schema) error {
	c := &checker{s: s}
	c.names()
	c.types()
	if len(c.errs) == 0 {
		c.cycles()
	}
	if len(c.errs) == 0 {
		c.layout()
	}
	if len(c.errs) > 0 {
		c.errs.sort()
		return c.errs
	}
	return nil
}

func (c *checker) names() {
	s := c.s
	if !snakeName.MatchString(s.Package) || reservedWords[s.Package] || strings.HasPrefix(s.Package, "ipcgen") {
		c.errorf(s.PkgPos, "package name %q must be lower_snake and not a reserved word", s.Package)
	}
	s.byName = map[string]*Message{}
	snakes := map[string]*Message{}
	ids := map[uint32]*Message{}
	for _, m := range s.Messages {
		snake := m.Snake()
		switch {
		case !messageName.MatchString(m.Name):
			c.errorf(m.Pos, "message name %q must be UpperCamel", m.Name)
		case pyNames[m.Name] || strings.HasPrefix(snake, "ipcgen"):
			c.errorf(m.Pos, "message name %q is reserved", m.Name)
		case s.byName[m.Name] != nil:
			c.errorf(m.Pos, "message %s is already declared at line %d", m.Name, s.byName[m.Name].Pos.Line)
		case snakes[snake] != nil:
			c.errorf(m.Pos, "message %s and message %s both map to the C name %s", snakes[snake].Name, m.Name, snake)
		}
		if s.byName[m.Name] == nil {
			s.byName[m.Name] = m
		}
		snakes[snake] = m
		switch {
		case m.ID == 0xFFFFFFFF:
			c.errorf(m.IDPos, "type ID 0xFFFFFFFF is reserved")
		case ids[m.ID] != nil:
			c.errorf(m.IDPos, "type ID %d is already used by message %s", m.ID, ids[m.ID].Name)
		default:
			ids[m.ID] = m
		}
		c.fieldNames(m)
	}
	for _, m := range s.Messages {
		if other := s.byName[m.Name+"Type"]; other != nil {
			c.errorf(other.Pos, "message name %s clashes with the Go constant for message %s", other.Name, m.Name)
		}
	}
}

func (c *checker) fieldNames(m *Message) {
	seen := map[string]*Field{}
	goNames := map[string]*Field{}
	for _, f := range m.Fields {
		goName := Camel(f.Name)
		switch {
		case !snakeName.MatchString(f.Name):
			c.errorf(f.Pos, "field name %q must be lower_snake", f.Name)
		case reservedWords[f.Name] || strings.HasPrefix(f.Name, "ipcgen") || goMethods[goName]:
			c.errorf(f.Pos, "field name %q is reserved", f.Name)
		case seen[f.Name] != nil:
			c.errorf(f.Pos, "field %s is already declared in message %s", f.Name, m.Name)
		case goNames[goName] != nil:
			c.errorf(f.Pos, "fields %s and %s both map to the Go name %s", goNames[goName].Name, f.Name, goName)
		}
		seen[f.Name] = f
		goNames[goName] = f
	}
}

// types resolves every reference and checks where each type may appear.
func (c *checker) types() {
	for _, m := range c.s.Messages {
		for _, f := range m.Fields {
			c.resolve(f.Type)
			t := f.Type
			if t.Kind == Ref && t.Msg != nil && !t.Msg.Fixed() {
				c.errorf(t.Pos, "field %s: message %s has a string or bytes field, so it cannot be a field type", f.Name, t.Name)
			}
		}
	}
}

func (c *checker) resolve(t *Type) {
	switch t.Kind {
	case Ref:
		t.Msg = c.s.byName[t.Name]
		if t.Msg == nil {
			c.errorf(t.Pos, "unknown type %s", t.Name)
		}
	case Array:
		if t.Len < 1 {
			c.errorf(t.Pos, "array length must be at least 1")
		}
		e := t.Elem
		switch {
		case e.Kind.IsVar() || e.Kind == Array:
			c.errorf(e.Pos, "array element %s must be a scalar or a fixed message", e)
		case e.Kind == Ref:
			c.resolve(e)
			if e.Msg != nil && !e.Msg.Fixed() {
				c.errorf(e.Pos, "array element %s has a string or bytes field, so it cannot be an array element", e.Name)
			}
		}
	}
}

// refOf returns the message that a fixed field type contains, or nil.
func refOf(t *Type) *Message {
	if t.Kind == Array {
		t = t.Elem
	}
	if t.Kind == Ref {
		return t.Msg
	}
	return nil
}

// cycles reports each message that contains itself and sets s.Sorted.
func (c *checker) cycles() {
	const (
		unseen = iota
		active
		done
	)
	state := map[*Message]int{}
	var stack []*Message
	var visit func(m *Message)
	visit = func(m *Message) {
		switch state[m] {
		case active:
			var path []string
			for i := len(stack) - 1; i >= 0; i-- {
				path = append([]string{stack[i].Name}, path...)
				if stack[i] == m {
					break
				}
			}
			c.errorf(m.Pos, "message %s contains itself: %s -> %s", m.Name, strings.Join(path, " -> "), m.Name)
			return
		case done:
			return
		}
		state[m] = active
		stack = append(stack, m)
		for _, f := range m.Fields {
			if r := refOf(f.Type); r != nil {
				visit(r)
			}
		}
		stack = stack[:len(stack)-1]
		state[m] = done
		c.s.Sorted = append(c.s.Sorted, m)
	}
	for _, m := range c.s.Messages {
		visit(m)
	}
}

// layout places every fixed field. Sorted puts each contained message first.
func (c *checker) layout() {
	for _, m := range c.s.Sorted {
		off, align := int64(0), 1
		for _, f := range m.FixedFields() {
			a := f.Type.Align()
			off = roundUp(off, int64(a))
			f.Offset = int(off)
			off += fieldSize(f.Type)
			if a > align {
				align = a
			}
			if off > maxFixedSize {
				c.errorf(f.Pos, "message %s: the fixed section is larger than %d bytes", m.Name, maxFixedSize)
				return
			}
		}
		m.FixedSize, m.Align = int(roundUp(off, int64(align))), align
	}
}

// fieldSize is Type.Size in int64, so an oversized array cannot overflow.
func fieldSize(t *Type) int64 {
	if t.Kind == Array {
		return int64(t.Len) * fieldSize(t.Elem)
	}
	return int64(t.Size())
}

func roundUp(n, a int64) int64 { return (n + a - 1) / a * a }
