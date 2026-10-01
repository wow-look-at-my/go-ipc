package gen

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/wow-look-at-my/go-ipc/ipcgen/internal/schema"
)

type cGen struct {
	w writer
	p string // Lower prefix, such as demo.
	P string // Upper prefix, such as DEMO.
}

func genC(s *schema.Schema) ([]byte, error) {
	g := &cGen{p: s.Package, P: strings.ToUpper(s.Package)}
	w := &g.w
	w.line("/* " + Header + " */")
	w.line("/*")
	w.line(" * Messages of package %s, from %s. Header-only C99.", g.p, filepath.Base(s.File))
	w.line(" * <m>_encode returns the bytes written, or 0 when cap is too small or a field is longer than 4 GiB.")
	w.line(" * <m>_decode returns 0, or a negative %s_IPCGEN_E* code when the input is malformed.", g.P)
	w.line(" * Decode does not copy: string and bytes fields point into the input. On failure *m is unspecified.")
	w.line(" */")
	w.line("#ifndef %s_IPCGEN_H", g.P)
	w.line("#define %s_IPCGEN_H", g.P)
	w.line("")
	w.line("#include <stdbool.h>")
	w.line("#include <stddef.h>")
	w.line("#include <stdint.h>")
	w.line("#include <string.h>")
	w.line("")
	w.line("#ifdef __cplusplus")
	w.line("extern \"C\" {")
	w.line("#endif")
	g.helpers()
	for _, m := range s.Sorted {
		g.message(m)
	}
	w.line("")
	w.line("#ifdef __cplusplus")
	w.line("}")
	w.line("#endif")
	w.line("")
	w.line("#endif")
	return w.Bytes(), nil
}

// cHelpers is the shared part of every C header. PFX stands for the lower prefix and UPFX for the upper prefix.
const cHelpers = `
#define UPFX_IPCGEN_ESHORT (-1)    /* The input is shorter than the fixed section. */
#define UPFX_IPCGEN_ELENGTH (-2)   /* A length runs past the end of the input. */
#define UPFX_IPCGEN_ETRAILING (-3) /* Bytes remain after the last field. */
#define UPFX_IPCGEN_EBOOL (-4)     /* A bool is neither 0 nor 1. */
#define UPFX_IPCGEN_EUTF8 (-5)     /* A string is not valid UTF-8. */

typedef struct PFX_ipcgen_str {
	const char *ptr;
	size_t len;
} PFX_ipcgen_str;

typedef struct PFX_ipcgen_bytes {
	const uint8_t *ptr;
	size_t len;
} PFX_ipcgen_bytes;

typedef char PFX_ipcgen_float_is_32_bits[sizeof(float) == 4 ? 1 : -1];
typedef char PFX_ipcgen_double_is_64_bits[sizeof(double) == 8 ? 1 : -1];

static inline void PFX_ipcgen_put_bool(uint8_t *p, bool v) { p[0] = v ? 1 : 0; }
static inline void PFX_ipcgen_put_u8(uint8_t *p, uint8_t v) { p[0] = v; }
static inline void PFX_ipcgen_put_i8(uint8_t *p, int8_t v) { p[0] = (uint8_t)v; }
static inline void PFX_ipcgen_put_u16(uint8_t *p, uint16_t v) {
	p[0] = (uint8_t)v;
	p[1] = (uint8_t)(v >> 8);
}
static inline void PFX_ipcgen_put_i16(uint8_t *p, int16_t v) { PFX_ipcgen_put_u16(p, (uint16_t)v); }
static inline void PFX_ipcgen_put_u32(uint8_t *p, uint32_t v) {
	PFX_ipcgen_put_u16(p, (uint16_t)v);
	PFX_ipcgen_put_u16(p + 2, (uint16_t)(v >> 16));
}
static inline void PFX_ipcgen_put_i32(uint8_t *p, int32_t v) { PFX_ipcgen_put_u32(p, (uint32_t)v); }
static inline void PFX_ipcgen_put_u64(uint8_t *p, uint64_t v) {
	PFX_ipcgen_put_u32(p, (uint32_t)v);
	PFX_ipcgen_put_u32(p + 4, (uint32_t)(v >> 32));
}
static inline void PFX_ipcgen_put_i64(uint8_t *p, int64_t v) { PFX_ipcgen_put_u64(p, (uint64_t)v); }
static inline void PFX_ipcgen_put_f32(uint8_t *p, float v) {
	uint32_t u;
	memcpy(&u, &v, sizeof u);
	PFX_ipcgen_put_u32(p, u);
}
static inline void PFX_ipcgen_put_f64(uint8_t *p, double v) {
	uint64_t u;
	memcpy(&u, &v, sizeof u);
	PFX_ipcgen_put_u64(p, u);
}

static inline uint8_t PFX_ipcgen_get_u8(const uint8_t *p) { return p[0]; }
static inline int8_t PFX_ipcgen_get_i8(const uint8_t *p) {
	int8_t v;
	memcpy(&v, p, sizeof v);
	return v;
}
static inline uint16_t PFX_ipcgen_get_u16(const uint8_t *p) { return (uint16_t)(p[0] | p[1] << 8); }
static inline int16_t PFX_ipcgen_get_i16(const uint8_t *p) {
	uint16_t u = PFX_ipcgen_get_u16(p);
	int16_t v;
	memcpy(&v, &u, sizeof v);
	return v;
}
static inline uint32_t PFX_ipcgen_get_u32(const uint8_t *p) {
	return (uint32_t)PFX_ipcgen_get_u16(p) | (uint32_t)PFX_ipcgen_get_u16(p + 2) << 16;
}
static inline int32_t PFX_ipcgen_get_i32(const uint8_t *p) {
	uint32_t u = PFX_ipcgen_get_u32(p);
	int32_t v;
	memcpy(&v, &u, sizeof v);
	return v;
}
static inline uint64_t PFX_ipcgen_get_u64(const uint8_t *p) {
	return (uint64_t)PFX_ipcgen_get_u32(p) | (uint64_t)PFX_ipcgen_get_u32(p + 4) << 32;
}
static inline int64_t PFX_ipcgen_get_i64(const uint8_t *p) {
	uint64_t u = PFX_ipcgen_get_u64(p);
	int64_t v;
	memcpy(&v, &u, sizeof v);
	return v;
}
static inline float PFX_ipcgen_get_f32(const uint8_t *p) {
	uint32_t u = PFX_ipcgen_get_u32(p);
	float v;
	memcpy(&v, &u, sizeof v);
	return v;
}
static inline double PFX_ipcgen_get_f64(const uint8_t *p) {
	uint64_t u = PFX_ipcgen_get_u64(p);
	double v;
	memcpy(&v, &u, sizeof v);
	return v;
}

/* PFX_ipcgen_addvar adds a length prefix and n bytes to *total. It returns 0 when n or the total does not fit. */
static inline int PFX_ipcgen_addvar(size_t *total, size_t n) {
	if ((uint64_t)n > UINT32_MAX || *total > SIZE_MAX - 4 || *total + 4 > SIZE_MAX - n) {
		return 0;
	}
	*total += 4 + n;
	return 1;
}

static inline size_t PFX_ipcgen_putvar(uint8_t *buf, size_t off, const void *ptr, size_t n) {
	PFX_ipcgen_put_u32(buf + off, (uint32_t)n);
	if (n > 0) {
		memcpy(buf + off + 4, ptr, n);
	}
	return off + 4 + n;
}

static inline int PFX_ipcgen_getvar(const uint8_t *buf, size_t len, size_t *off, const uint8_t **ptr, size_t *n) {
	uint32_t k;
	if (len - *off < 4) {
		return UPFX_IPCGEN_ELENGTH;
	}
	k = PFX_ipcgen_get_u32(buf + *off);
	*off += 4;
	if ((uint64_t)k > (uint64_t)(len - *off)) {
		return UPFX_IPCGEN_ELENGTH;
	}
	*ptr = buf + *off;
	*n = k;
	*off += k;
	return 0;
}

/* PFX_ipcgen_utf8 reports whether s holds valid UTF-8, with no overlong form and no surrogate. */
static inline int PFX_ipcgen_utf8(const uint8_t *s, size_t n) {
	size_t i = 0;
	while (i < n) {
		uint8_t c = s[i];
		uint32_t cp, min;
		size_t k, j;
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
			return 0;
		}
		if (n - i - 1 < k) {
			return 0;
		}
		for (j = 1; j <= k; j++) {
			if ((s[i + j] & 0xC0) != 0x80) {
				return 0;
			}
			cp = cp << 6 | (s[i + j] & 0x3F);
		}
		if (cp < min || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) {
			return 0;
		}
		i += k + 1;
	}
	return 1;
}
`

func (g *cGen) helpers() {
	r := strings.NewReplacer("UPFX", g.P, "PFX", g.p)
	g.w.WriteString(r.Replace(cHelpers))
}

func (g *cGen) cType(t *schema.Type) string {
	switch t.Kind {
	case schema.Ref:
		return g.p + "_" + t.Msg.Snake()
	case schema.String:
		return g.p + "_ipcgen_str"
	case schema.Bytes:
		return g.p + "_ipcgen_bytes"
	case schema.Bool:
		return "bool"
	case schema.F32:
		return "float"
	case schema.F64:
		return "double"
	}
	name := t.Kind.String()
	if t.Kind.IsSigned() {
		return "int" + name[1:] + "_t"
	}
	return "uint" + name[1:] + "_t"
}

func (g *cGen) message(m *schema.Message) {
	w := &g.w
	sn := m.Snake()
	name := g.p + "_" + sn
	upper := g.P + "_" + strings.ToUpper(sn)
	internal := g.p + "_ipcgen_" + sn
	w.line("")
	w.line("/* %s */", m.Name)
	w.line("#define %s_TYPE UINT32_C(%d)", upper, m.ID)
	w.line("#define %s_FIXED_SIZE %d", upper, m.FixedSize)
	w.line("")
	w.line("typedef struct %s {", name)
	for _, f := range m.Fields {
		if f.Type.Kind == schema.Array {
			w.line("\t%s %s[%d];", g.cType(f.Type.Elem), f.Name, f.Type.Len)
		} else {
			w.line("\t%s %s;", g.cType(f.Type), f.Name)
		}
	}
	if len(m.Fields) == 0 {
		w.line("\tchar ipcgen_unused; /* C has no empty struct. */")
	}
	w.line("} %s;", name)

	w.line("")
	w.line("static inline void %s_put(const %s *m, uint8_t *p) {", internal, name)
	if len(m.FixedFields()) == 0 {
		w.line("\t(void)m;")
		w.line("\t(void)p;")
	}
	for _, f := range m.FixedFields() {
		g.put(f.Type, "m->"+f.Name, fmt.Sprint(f.Offset), "\t")
	}
	w.line("}")
	w.line("")
	w.line("static inline int %s_get(%s *m, const uint8_t *p) {", internal, name)
	if len(m.FixedFields()) == 0 {
		w.line("\t(void)m;")
		w.line("\t(void)p;")
	}
	for _, f := range m.FixedFields() {
		g.get(f.Type, "m->"+f.Name, fmt.Sprint(f.Offset), "\t")
	}
	w.line("\treturn 0;")
	w.line("}")
	g.size(m, name)
	g.encode(m, name, internal)
	g.decode(m, name, internal)
}

func (g *cGen) size(m *schema.Message, name string) {
	w := &g.w
	w.line("")
	w.line("/* %s_size returns the encoded size of m, or SIZE_MAX when a field is longer than 4 GiB. */", name)
	w.line("static inline size_t %s_size(const %s *m) {", name, name)
	if m.Fixed() {
		w.line("\t(void)m;")
		w.line("\treturn %d;", m.FixedSize)
		w.line("}")
		return
	}
	w.line("\tsize_t n = %d;", m.FixedSize)
	var checks []string
	for _, f := range m.VarFields() {
		checks = append(checks, fmt.Sprintf("!%s_ipcgen_addvar(&n, m->%s.len)", g.p, f.Name))
	}
	w.line("\tif (%s) {", strings.Join(checks, " || "))
	w.line("\t\treturn SIZE_MAX;")
	w.line("\t}")
	w.line("\treturn n;")
	w.line("}")
}

func (g *cGen) encode(m *schema.Message, name, internal string) {
	w := &g.w
	w.line("")
	w.line("static inline size_t %s_encode(const %s *m, uint8_t *buf, size_t cap) {", name, name)
	w.line("\tsize_t n = %s_size(m);", name)
	switch {
	case !m.Fixed():
		w.line("\tsize_t off = %d;", m.FixedSize)
		w.line("\tif (n == SIZE_MAX || cap < n) {")
		w.line("\t\treturn 0;")
		w.line("\t}")
	case m.FixedSize > 0:
		w.line("\tif (cap < n) {")
		w.line("\t\treturn 0;")
		w.line("\t}")
	default:
		w.line("\t(void)buf;")
		w.line("\t(void)cap;")
	}
	if m.FixedSize > 0 {
		w.line("\tmemset(buf, 0, %d);", m.FixedSize)
		w.line("\t%s_put(m, buf);", internal)
	}
	for _, f := range m.VarFields() {
		w.line("\toff = %s_ipcgen_putvar(buf, off, m->%s.ptr, m->%s.len);", g.p, f.Name, f.Name)
	}
	w.line("\treturn n;")
	w.line("}")
}

func (g *cGen) decode(m *schema.Message, name, internal string) {
	w := &g.w
	w.line("")
	w.line("static inline int %s_decode(%s *m, const uint8_t *buf, size_t len) {", name, name)
	w.line("\tsize_t off = %d;", m.FixedSize)
	if !m.Fixed() {
		w.line("\tconst uint8_t *ptr;")
		w.line("\tsize_t n;")
	}
	w.line("\tint rc;")
	if m.FixedSize > 0 {
		w.line("\tif (len < %d) {", m.FixedSize)
		w.line("\t\treturn %s_IPCGEN_ESHORT;", g.P)
		w.line("\t}")
	}
	w.line("\trc = %s_get(m, buf);", internal)
	w.line("\tif (rc != 0) {")
	w.line("\t\treturn rc;")
	w.line("\t}")
	for _, f := range m.VarFields() {
		w.line("\trc = %s_ipcgen_getvar(buf, len, &off, &ptr, &n);", g.p)
		w.line("\tif (rc != 0) {")
		w.line("\t\treturn rc;")
		w.line("\t}")
		if f.Type.Kind == schema.String {
			w.line("\tif (!%s_ipcgen_utf8(ptr, n)) {", g.p)
			w.line("\t\treturn %s_IPCGEN_EUTF8;", g.P)
			w.line("\t}")
			w.line("\tm->%s.ptr = (const char *)ptr;", f.Name)
		} else {
			w.line("\tm->%s.ptr = ptr;", f.Name)
		}
		w.line("\tm->%s.len = n;", f.Name)
	}
	w.line("\tif (off != len) {")
	w.line("\t\treturn %s_IPCGEN_ETRAILING;", g.P)
	w.line("\t}")
	w.line("\treturn 0;")
	w.line("}")
}

func (g *cGen) put(t *schema.Type, val, off, ind string) {
	w := &g.w
	switch t.Kind {
	case schema.Array:
		w.line("%sfor (size_t i = 0; i < %d; i++) {", ind, t.Len)
		g.put(t.Elem, val+"[i]", fmt.Sprintf("%s + i * %d", off, t.Elem.Size()), ind+"\t")
		w.line("%s}", ind)
	case schema.Ref:
		w.line("%s%s_ipcgen_%s_put(&%s, p + %s);", ind, g.p, t.Msg.Snake(), val, off)
	default:
		w.line("%s%s_ipcgen_put_%s(p + %s, %s);", ind, g.p, t.Kind, off, val)
	}
}

func (g *cGen) get(t *schema.Type, val, off, ind string) {
	w := &g.w
	switch t.Kind {
	case schema.Array:
		w.line("%sfor (size_t i = 0; i < %d; i++) {", ind, t.Len)
		g.get(t.Elem, val+"[i]", fmt.Sprintf("%s + i * %d", off, t.Elem.Size()), ind+"\t")
		w.line("%s}", ind)
	case schema.Ref:
		w.line("%s{", ind)
		w.line("%s\tint rc = %s_ipcgen_%s_get(&%s, p + %s);", ind, g.p, t.Msg.Snake(), val, off)
		w.line("%s\tif (rc != 0) {", ind)
		w.line("%s\t\treturn rc;", ind)
		w.line("%s\t}", ind)
		w.line("%s}", ind)
	case schema.Bool:
		w.line("%sif (p[%s] > 1) {", ind, off)
		w.line("%s\treturn %s_IPCGEN_EBOOL;", ind, g.P)
		w.line("%s}", ind)
		w.line("%s%s = p[%s] == 1;", ind, val, off)
	default:
		w.line("%s%s = %s_ipcgen_get_%s(p + %s);", ind, val, g.p, t.Kind, off)
	}
}
