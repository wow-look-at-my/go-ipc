package schema

import (
	"fmt"
	"strconv"
	"unicode/utf8"
)

type tokKind int

const (
	tEOF tokKind = iota
	tNewline
	tIdent
	tNumber
	tLBrace
	tRBrace
	tLBrack
	tRBrack
	tEq
)

var tokNames = map[tokKind]string{
	tEOF: "end of file", tNewline: "newline", tIdent: "name", tNumber: "number",
	tLBrace: "'{'", tRBrace: "'}'", tLBrack: "'['", tRBrack: "']'", tEq: "'='",
}

type token struct {
	kind tokKind
	text string
	pos  Pos
}

func (t token) describe() string {
	if t.kind == tIdent || t.kind == tNumber {
		return fmt.Sprintf("%q", t.text)
	}
	return tokNames[t.kind]
}

// lex splits src into tokens. A comment runs from '#' to the end of the line.
func lex(file string, src []byte) ([]token, error) {
	var toks []token
	line, col := 1, 1
	for i := 0; i < len(src); {
		r, w := utf8.DecodeRune(src[i:])
		pos := Pos{line, col}
		switch {
		case r == '\n':
			toks = append(toks, token{tNewline, "\n", pos})
			i++
			line, col = line+1, 1
			continue
		case r == ' ' || r == '\t' || r == '\r':
			i++
			col++
			continue
		case r == '#':
			for i < len(src) && src[i] != '\n' {
				_, w := utf8.DecodeRune(src[i:])
				i += w
			}
			continue
		case isIdentStart(r) || isDigit(r):
			j := i
			for j < len(src) && (isIdentStart(rune(src[j])) || isDigit(rune(src[j]))) {
				j++
			}
			kind := tIdent
			if isDigit(r) {
				kind = tNumber
			}
			toks = append(toks, token{kind, string(src[i:j]), pos})
			col += j - i
			i = j
			continue
		}
		kind, ok := map[rune]tokKind{'{': tLBrace, '}': tRBrace, '[': tLBrack, ']': tRBrack, '=': tEq}[r]
		if !ok {
			return nil, &Error{file, pos, fmt.Sprintf("unexpected character %q", r)}
		}
		toks = append(toks, token{kind, string(r), pos})
		i += w
		col++
	}
	return append(toks, token{tEOF, "", Pos{line, col}}), nil
}

func isIdentStart(r rune) bool {
	return r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
}

func isDigit(r rune) bool { return r >= '0' && r <= '9' }

type parser struct {
	file string
	toks []token
	i    int
}

func (p *parser) peek() token { return p.toks[p.i] }

func (p *parser) next() token {
	t := p.toks[p.i]
	if t.kind != tEOF {
		p.i++
	}
	return t
}

func (p *parser) errorf(pos Pos, format string, args ...any) error {
	return &Error{p.file, pos, fmt.Sprintf(format, args...)}
}

func (p *parser) expect(kind tokKind, what string) (token, error) {
	t := p.next()
	if t.kind != kind {
		return t, p.errorf(t.pos, "expected %s, found %s", what, t.describe())
	}
	return t, nil
}

func (p *parser) skipNewlines() {
	for p.peek().kind == tNewline {
		p.next()
	}
}

// endLine requires a newline or the end of the file.
func (p *parser) endLine(after string) error {
	t := p.peek()
	if t.kind == tNewline || t.kind == tEOF {
		p.next()
		return nil
	}
	return p.errorf(t.pos, "expected newline after %s, found %s", after, t.describe())
}

// Parse parses a schema. It checks syntax only; Check does the rest.
func Parse(file string, src []byte) (*Schema, error) {
	toks, err := lex(file, src)
	if err != nil {
		return nil, err
	}
	p := &parser{file: file, toks: toks}
	s := &Schema{File: file}
	p.skipNewlines()
	if t := p.next(); t.kind != tIdent || t.text != "package" {
		return nil, p.errorf(t.pos, "expected 'package', found %s", t.describe())
	}
	name, err := p.expect(tIdent, "package name")
	if err != nil {
		return nil, err
	}
	s.Package, s.PkgPos = name.text, name.pos
	if err := p.endLine("package name"); err != nil {
		return nil, err
	}
	for {
		p.skipNewlines()
		t := p.next()
		switch {
		case t.kind == tEOF:
			return s, nil
		case t.kind == tIdent && t.text == "package":
			return nil, p.errorf(t.pos, "package is already declared")
		case t.kind != tIdent || t.text != "message":
			return nil, p.errorf(t.pos, "expected 'message', found %s", t.describe())
		}
		m, err := p.message()
		if err != nil {
			return nil, err
		}
		s.Messages = append(s.Messages, m)
	}
}

func (p *parser) message() (*Message, error) {
	name, err := p.expect(tIdent, "message name")
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tEq, "'='"); err != nil {
		return nil, err
	}
	idTok, err := p.expect(tNumber, "type ID")
	if err != nil {
		return nil, err
	}
	id, err := parseNum(idTok.text, 32)
	if err != nil {
		return nil, p.errorf(idTok.pos, "type ID %s is not a u32", idTok.text)
	}
	if _, err := p.expect(tLBrace, "'{'"); err != nil {
		return nil, err
	}
	m := &Message{Name: name.text, ID: uint32(id), Pos: name.pos, IDPos: idTok.pos}
	for {
		p.skipNewlines()
		t := p.next()
		if t.kind == tRBrace {
			return m, p.endLine("'}'")
		}
		if t.kind != tIdent {
			return nil, p.errorf(t.pos, "expected field name or '}', found %s", t.describe())
		}
		typ, err := p.typ()
		if err != nil {
			return nil, err
		}
		m.Fields = append(m.Fields, &Field{Name: t.text, Type: typ, Pos: t.pos})
		if err := p.endLine("field"); err != nil {
			return nil, err
		}
	}
}

func (p *parser) typ() (*Type, error) {
	t := p.next()
	switch t.kind {
	case tIdent:
		for k, n := range scalarNames {
			if n == t.text {
				return &Type{Kind: Kind(k), Pos: t.pos}, nil
			}
		}
		switch t.text {
		case "string":
			return &Type{Kind: String, Pos: t.pos}, nil
		case "bytes":
			return &Type{Kind: Bytes, Pos: t.pos}, nil
		}
		return &Type{Kind: Ref, Name: t.text, Pos: t.pos}, nil
	case tLBrack:
		n, err := p.expect(tNumber, "array length")
		if err != nil {
			return nil, err
		}
		length, err := parseNum(n.text, 31)
		if err != nil {
			return nil, p.errorf(n.pos, "array length %s is too large", n.text)
		}
		if _, err := p.expect(tRBrack, "']'"); err != nil {
			return nil, err
		}
		elem, err := p.typ()
		if err != nil {
			return nil, err
		}
		return &Type{Kind: Array, Len: int(length), Elem: elem, Pos: t.pos}, nil
	}
	return nil, p.errorf(t.pos, "expected type, found %s", t.describe())
}

// parseNum parses a decimal or 0x-prefixed hex number that fits in bits.
func parseNum(text string, bits int) (uint64, error) {
	if len(text) > 2 && (text[:2] == "0x" || text[:2] == "0X") {
		return strconv.ParseUint(text[2:], 16, bits)
	}
	return strconv.ParseUint(text, 10, bits)
}
