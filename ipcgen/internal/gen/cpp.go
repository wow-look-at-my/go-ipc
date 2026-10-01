package gen

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/wow-look-at-my/go-ipc/ipcgen/internal/schema"
)

type cppGen struct {
	w writer
}

func genCpp(s *schema.Schema) ([]byte, error) {
	g := &cppGen{}
	w := &g.w
	w.line("// " + Header)
	w.line("// Messages of package %s, from %s. Header-only C++20.", s.Package, filepath.Base(s.File))
	w.line("// encode returns the bytes written, or 0 when out is too small or a field is longer than 4 GiB.")
	w.line("// decode returns std::nullopt when the input is malformed. It throws only std::bad_alloc.")
	w.WriteString(cppHelpers)
	w.line("")
	w.line("namespace %s {", s.Package)
	w.WriteString(cppDetail)
	for _, m := range s.Sorted {
		g.decl(m)
	}
	w.line("")
	w.line("namespace ipcgen {")
	for _, m := range s.Sorted {
		g.fixed(m)
	}
	w.line("")
	w.line("} // namespace ipcgen")
	for _, m := range s.Sorted {
		g.methods(m)
	}
	w.line("")
	w.line("} // namespace %s", s.Package)
	return w.Bytes(), nil
}

const cppHelpers = `
#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <vector>
`

// cppDetail holds the shared helpers. They live in the package namespace.
const cppDetail = `
namespace ipcgen {

static_assert(std::numeric_limits<float>::is_iec559 && sizeof(float) == 4);
static_assert(std::numeric_limits<double>::is_iec559 && sizeof(double) == 8);

template <class T>
inline void put(std::byte *p, T v) noexcept {
	if constexpr (std::is_same_v<T, bool>) {
		p[0] = std::byte{static_cast<unsigned char>(v ? 1 : 0)};
	} else if constexpr (std::is_floating_point_v<T>) {
		using U = std::conditional_t<sizeof(T) == 4, std::uint32_t, std::uint64_t>;
		put(p, std::bit_cast<U>(v));
	} else {
		using U = std::make_unsigned_t<T>;
		U u = static_cast<U>(v);
		for (std::size_t i = 0; i < sizeof(T); i++) {
			p[i] = static_cast<std::byte>(u >> (8 * i));
		}
	}
}

template <class T>
inline T get(const std::byte *p) noexcept {
	if constexpr (std::is_floating_point_v<T>) {
		using U = std::conditional_t<sizeof(T) == 4, std::uint32_t, std::uint64_t>;
		return std::bit_cast<T>(get<U>(p));
	} else {
		using U = std::make_unsigned_t<T>;
		U u = 0;
		for (std::size_t i = 0; i < sizeof(T); i++) {
			u = static_cast<U>(u | static_cast<U>(std::to_integer<U>(p[i]) << (8 * i)));
		}
		return static_cast<T>(u);
	}
}

inline bool fits(std::size_t n) noexcept { return static_cast<std::uint64_t>(n) <= 0xFFFFFFFFu; }

inline std::size_t put_var(std::byte *out, std::size_t off, const void *p, std::size_t n) noexcept {
	put<std::uint32_t>(out + off, static_cast<std::uint32_t>(n));
	if (n > 0) {
		std::memcpy(out + off + 4, p, n);
	}
	return off + 4 + n;
}

inline bool get_var(std::span<const std::byte> in, std::size_t &off, std::span<const std::byte> &out) noexcept {
	if (in.size() - off < 4) {
		return false;
	}
	std::uint32_t n = get<std::uint32_t>(in.data() + off);
	off += 4;
	if (n > in.size() - off) {
		return false;
	}
	out = in.subspan(off, n);
	off += n;
	return true;
}

// utf8 reports whether s holds valid UTF-8, with no overlong form and no surrogate.
inline bool utf8(std::span<const std::byte> s) noexcept {
	std::size_t i = 0, n = s.size();
	while (i < n) {
		auto c = std::to_integer<std::uint32_t>(s[i]);
		std::uint32_t cp = 0, min = 0;
		std::size_t k = 0;
		if (c < 0x80) {
			i++;
			continue;
		}
		if ((c & 0xE0) == 0xC0) {
			k = 1, cp = c & 0x1F, min = 0x80;
		} else if ((c & 0xF0) == 0xE0) {
			k = 2, cp = c & 0x0F, min = 0x800;
		} else if ((c & 0xF8) == 0xF0) {
			k = 3, cp = c & 0x07, min = 0x10000;
		} else {
			return false;
		}
		if (n - i - 1 < k) {
			return false;
		}
		for (std::size_t j = 1; j <= k; j++) {
			auto d = std::to_integer<std::uint32_t>(s[i + j]);
			if ((d & 0xC0) != 0x80) {
				return false;
			}
			cp = cp << 6 | (d & 0x3F);
		}
		if (cp < min || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) {
			return false;
		}
		i += k + 1;
	}
	return true;
}

} // namespace ipcgen
`

func cppType(t *schema.Type) string {
	switch t.Kind {
	case schema.Array:
		return fmt.Sprintf("std::array<%s, %d>", cppType(t.Elem), t.Len)
	case schema.Ref:
		return t.Name
	case schema.String:
		return "std::string"
	case schema.Bytes:
		return "std::vector<std::byte>"
	case schema.Bool:
		return "bool"
	case schema.F32:
		return "float"
	case schema.F64:
		return "double"
	}
	name := t.Kind.String()
	if t.Kind.IsSigned() {
		return "std::int" + name[1:] + "_t"
	}
	return "std::uint" + name[1:] + "_t"
}

func (g *cppGen) decl(m *schema.Message) {
	w := &g.w
	w.line("")
	w.line("struct %s {", m.Name)
	w.line("\tstatic constexpr std::uint32_t type_id = %d;", m.ID)
	w.line("\tstatic constexpr std::size_t fixed_size = %d;", m.FixedSize)
	if len(m.Fields) > 0 {
		w.line("")
	}
	for _, f := range m.Fields {
		init := "{}"
		if f.IsVar() {
			init = ""
		}
		w.line("\t%s %s%s;", cppType(f.Type), f.Name, init)
	}
	w.line("")
	w.line("\tbool operator==(const %s &) const = default;", m.Name)
	w.line("\tstd::size_t size() const noexcept;")
	w.line("\tstd::size_t encode(std::span<std::byte> out) const noexcept;")
	w.line("\tstatic std::optional<%s> decode(std::span<const std::byte> in);", m.Name)
	w.line("};")
}

func (g *cppGen) fixed(m *schema.Message) {
	w := &g.w
	w.line("")
	w.line("inline void put_fixed([[maybe_unused]] const %s &m, [[maybe_unused]] std::byte *p) noexcept {", m.Name)
	for _, f := range m.FixedFields() {
		g.put(f.Type, "m."+f.Name, fmt.Sprint(f.Offset), "\t")
	}
	w.line("}")
	w.line("")
	w.line("inline bool get_fixed([[maybe_unused]] %s &m, [[maybe_unused]] const std::byte *p) noexcept {", m.Name)
	for _, f := range m.FixedFields() {
		g.get(f.Type, "m."+f.Name, fmt.Sprint(f.Offset), "\t")
	}
	w.line("\treturn true;")
	w.line("}")
}

func (g *cppGen) put(t *schema.Type, val, off, ind string) {
	w := &g.w
	switch t.Kind {
	case schema.Array:
		w.line("%sfor (std::size_t i = 0; i < %d; i++) {", ind, t.Len)
		g.put(t.Elem, val+"[i]", fmt.Sprintf("%s + i * %d", off, t.Elem.Size()), ind+"\t")
		w.line("%s}", ind)
	case schema.Ref:
		w.line("%sput_fixed(%s, p + %s);", ind, val, off)
	default:
		w.line("%sput(p + %s, %s);", ind, off, val)
	}
}

func (g *cppGen) get(t *schema.Type, val, off, ind string) {
	w := &g.w
	switch t.Kind {
	case schema.Array:
		w.line("%sfor (std::size_t i = 0; i < %d; i++) {", ind, t.Len)
		g.get(t.Elem, val+"[i]", fmt.Sprintf("%s + i * %d", off, t.Elem.Size()), ind+"\t")
		w.line("%s}", ind)
	case schema.Ref:
		w.line("%sif (!get_fixed(%s, p + %s)) {", ind, val, off)
		w.line("%s\treturn false;", ind)
		w.line("%s}", ind)
	case schema.Bool:
		w.line("%sif (std::to_integer<unsigned>(p[%s]) > 1) {", ind, off)
		w.line("%s\treturn false;", ind)
		w.line("%s}", ind)
		w.line("%s%s = std::to_integer<unsigned>(p[%s]) == 1;", ind, val, off)
	default:
		w.line("%s%s = get<%s>(p + %s);", ind, val, cppType(t), off)
	}
}

func (g *cppGen) methods(m *schema.Message) {
	w := &g.w
	terms := []string{"fixed_size"}
	var fits []string
	for _, f := range m.VarFields() {
		terms = append(terms, "4", "this->"+f.Name+".size()")
		fits = append(fits, fmt.Sprintf("!ipcgen::fits(this->%s.size())", f.Name))
	}
	w.line("")
	w.line("inline std::size_t %s::size() const noexcept { return %s; }", m.Name, strings.Join(terms, " + "))
	w.line("")
	w.line("inline std::size_t %s::encode(std::span<std::byte> out) const noexcept {", m.Name)
	if len(fits) > 0 {
		w.line("\tif (%s) {", strings.Join(fits, " || "))
		w.line("\t\treturn 0;")
		w.line("\t}")
	}
	w.line("\tstd::size_t n = size();")
	w.line("\tif (out.size() < n) {")
	w.line("\t\treturn 0;")
	w.line("\t}")
	w.line("\tstd::fill_n(out.data(), fixed_size, std::byte{0});")
	w.line("\tipcgen::put_fixed(*this, out.data());")
	if !m.Fixed() {
		w.line("\tstd::size_t off = fixed_size;")
		for _, f := range m.VarFields() {
			w.line("\toff = ipcgen::put_var(out.data(), off, this->%s.data(), this->%s.size());", f.Name, f.Name)
		}
	}
	w.line("\treturn n;")
	w.line("}")
	w.line("")
	w.line("inline std::optional<%s> %s::decode(std::span<const std::byte> in) {", m.Name, m.Name)
	if m.FixedSize > 0 {
		w.line("\tif (in.size() < fixed_size) {")
		w.line("\t\treturn std::nullopt;")
		w.line("\t}")
	}
	w.line("\t%s m;", m.Name)
	w.line("\tif (!ipcgen::get_fixed(m, in.data())) {")
	w.line("\t\treturn std::nullopt;")
	w.line("\t}")
	w.line("\tstd::size_t off = fixed_size;")
	if !m.Fixed() {
		w.line("\tstd::span<const std::byte> raw;")
	}
	for _, f := range m.VarFields() {
		if f.Type.Kind == schema.String {
			w.line("\tif (!ipcgen::get_var(in, off, raw) || !ipcgen::utf8(raw)) {")
			w.line("\t\treturn std::nullopt;")
			w.line("\t}")
			w.line("\tm.%s.assign(reinterpret_cast<const char *>(raw.data()), raw.size());", f.Name)
		} else {
			w.line("\tif (!ipcgen::get_var(in, off, raw)) {")
			w.line("\t\treturn std::nullopt;")
			w.line("\t}")
			w.line("\tm.%s.assign(raw.begin(), raw.end());", f.Name)
		}
	}
	w.line("\tif (off != in.size()) {")
	w.line("\t\treturn std::nullopt;")
	w.line("\t}")
	w.line("\treturn m;")
	w.line("}")
}
