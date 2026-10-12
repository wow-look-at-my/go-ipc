package schema

import (
	"regexp"
	"strings"
	"unicode"
)

var (
	messageName = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)
	snakeName   = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)
)

// Snake converts an UpperCamel name to lower_snake. An acronym stays one word.
func Snake(name string) string {
	r := []rune(name)
	var b strings.Builder
	for i, c := range r {
		if i > 0 && unicode.IsUpper(c) {
			prev := r[i-1]
			nextLower := i+1 < len(r) && unicode.IsLower(r[i+1])
			if unicode.IsLower(prev) || unicode.IsDigit(prev) || unicode.IsUpper(prev) && nextLower {
				b.WriteByte('_')
			}
		}
		b.WriteRune(unicode.ToLower(c))
	}
	return b.String()
}

// Camel converts a lower_snake name to UpperCamel.
func Camel(name string) string {
	var b strings.Builder
	for _, part := range strings.Split(name, "_") {
		if part != "" {
			b.WriteString(strings.ToUpper(part[:1]) + part[1:])
		}
	}
	return b.String()
}

// reservedWords holds keywords and predefined names of C, C++, Go and Python that a lower_snake name can spell.
var reservedWords = setOf(
	// C99 and C11.
	"auto", "break", "case", "char", "const", "continue", "default", "do", "double", "else", "enum", "extern",
	"float", "for", "goto", "if", "inline", "int", "long", "register", "restrict", "return", "short", "signed",
	"sizeof", "static", "struct", "switch", "typedef", "union", "unsigned", "void", "volatile", "while",
	"alignas", "alignof", "bool", "true", "false", "static_assert", "thread_local", "noreturn", "complex",
	"imaginary", "assert", "errno", "offsetof", "linux", "unix", "i386",
	"and", "and_eq", "asm", "bitand", "bitor", "catch", "char8_t", "char16_t", "char32_t", "class", "compl",
	"concept", "consteval", "constexpr", "constinit", "const_cast", "co_await", "co_return", "co_yield",
	"decltype", "delete", "dynamic_cast", "explicit", "export", "friend", "mutable", "namespace", "new",
	"noexcept", "not", "not_eq", "nullptr", "operator", "or", "or_eq", "private", "protected", "public",
	"reinterpret_cast", "requires", "static_cast", "template", "this", "throw", "try", "typeid", "typename",
	"using", "virtual", "wchar_t", "xor", "xor_eq", "std",
	// Go.
	"chan", "defer", "fallthrough", "func", "go", "import", "interface", "map", "package", "range", "select",
	"type", "var",
	// Python.
	"as", "async", "await", "def", "del", "elif", "except", "finally", "from", "global", "in", "is", "lambda",
	"nonlocal", "pass", "raise", "with", "yield", "none", "self", "cls",
	// Members that generated code declares on every message.
	"size", "encode", "decode", "type_id", "fixed_size",
)

func setOf(words ...string) map[string]bool {
	m := make(map[string]bool, len(words))
	for _, w := range words {
		m[w] = true
	}
	return m
}

// goMethods holds the Go methods that generated code declares on every message.
var goMethods = setOf("Size", "MarshalTo", "MarshalBinary", "UnmarshalBinary", "TypeID")

// pyNames holds module names that generated Python declares and a message must not shadow.
var pyNames = setOf("MESSAGES")

// goNames holds package names that generated Go declares and a message must not shadow.
var goNames = setOf("Message", "NewMessage")
