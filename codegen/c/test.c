/* Tests the generated C header against spec/vectors/schema. The Makefile builds fixture.h with jq and defines VECTOR_DIR. */
#include <stdio.h>
#include <stdlib.h>

#include "demo.h"

static int failures;

#define FAIL(...)                                                                                                     \
	do {                                                                                                          \
		fprintf(stderr, __VA_ARGS__);                                                                         \
		fputc('\n', stderr);                                                                                  \
		failures++;                                                                                           \
	} while (0)

static int eq_mem(const void *a, size_t an, const void *b, size_t bn) {
	return an == bn && (an == 0 || memcmp(a, b, an) == 0);
}

/* read_vector returns the bytes of <index>.bin in a buffer of exactly that size, so ASan sees any overread. */
static uint8_t *read_vector(int index, size_t *len) {
	char path[4096];
	FILE *f;
	long n;
	uint8_t *buf;
	snprintf(path, sizeof path, "%s/%d.bin", VECTOR_DIR, index);
	f = fopen(path, "rb");
	if (f == NULL || fseek(f, 0, SEEK_END) != 0 || (n = ftell(f)) < 0 || fseek(f, 0, SEEK_SET) != 0) {
		fprintf(stderr, "cannot read %s\n", path);
		exit(2);
	}
	buf = malloc(n > 0 ? (size_t)n : 1);
	if (buf == NULL || fread(buf, 1, (size_t)n, f) != (size_t)n) {
		fprintf(stderr, "cannot read %s\n", path);
		exit(2);
	}
	fclose(f);
	*len = (size_t)n;
	return buf;
}

/* copy returns n bytes of src in a buffer of exactly n bytes. */
static uint8_t *copy(const void *src, size_t n) {
	uint8_t *buf = malloc(n > 0 ? n : 1);
	if (buf == NULL) {
		exit(2);
	}
	if (n > 0) {
		memcpy(buf, src, n);
	}
	return buf;
}

#include "fixture.h"

/* FX_CASE checks one values.json entry: encode, decode, a short buffer, a truncated input and a trailing byte. */
#define FX_CASE(idx, T)                                                                                               \
	do {                                                                                                          \
		T v, d;                                                                                               \
		size_t want_len, n;                                                                                   \
		uint8_t *want = read_vector(idx, &want_len);                                                          \
		uint8_t *got = malloc(want_len > 0 ? want_len : 1);                                                   \
		uint8_t *longer = malloc(want_len + 1);                                                               \
		fx_build_##idx(&v);                                                                                   \
		if (T##_size(&v) != want_len) {                                                                       \
			FAIL("entry %d: " #T "_size is %zu, want %zu", idx, T##_size(&v), want_len);                 \
		}                                                                                                     \
		n = T##_encode(&v, got, want_len);                                                                    \
		if (n != want_len || !eq_mem(got, n, want, want_len)) {                                               \
			FAIL("entry %d: " #T "_encode does not match %d.bin", idx, idx);                              \
		}                                                                                                     \
		if (want_len > 0 && T##_encode(&v, got, want_len - 1) != 0) {                                         \
			FAIL("entry %d: " #T "_encode into a short buffer does not return 0", idx);                  \
		}                                                                                                     \
		memset(&d, 0xAB, sizeof d);                                                                           \
		if (T##_decode(&d, want, want_len) != 0) {                                                            \
			FAIL("entry %d: " #T "_decode rejects %d.bin", idx, idx);                                     \
		} else if (!fx_check_##idx(&d)) {                                                                     \
			FAIL("entry %d: " #T "_decode of %d.bin does not equal the value", idx, idx);                 \
		}                                                                                                     \
		if (want_len > 0) {                                                                                   \
			uint8_t *cut = copy(want, want_len - 1);                                                      \
			if (T##_decode(&d, cut, want_len - 1) >= 0) {                                                 \
				FAIL("entry %d: " #T "_decode accepts a truncated input", idx);                      \
			}                                                                                             \
			free(cut);                                                                                    \
		}                                                                                                     \
		if (want_len > 0) {                                                                                   \
			memcpy(longer, want, want_len);                                                               \
		}                                                                                                     \
		longer[want_len] = 0;                                                                                 \
		if (T##_decode(&d, longer, want_len + 1) != DEMO_IPCGEN_ETRAILING) {                                  \
			FAIL("entry %d: " #T "_decode does not report a trailing byte", idx);                        \
		}                                                                                                     \
		free(longer);                                                                                         \
		free(got);                                                                                            \
		free(want);                                                                                           \
		cases++;                                                                                              \
	} while (0);

/* FX_BAD checks that one invalid.json entry is rejected with the error code it names. */
#define FX_BAD(T, why, bytes, len)                                                                                    \
	do {                                                                                                          \
		T d;                                                                                                  \
		uint8_t *in = copy(bytes, len);                                                                       \
		int rc = T##_decode(&d, in, len);                                                                     \
		if (rc != code_of(why)) {                                                                             \
			FAIL("invalid entry %d: " #T "_decode returns %d, want %d (%s)", bad, rc, code_of(why), why); \
		}                                                                                                     \
		free(in);                                                                                             \
		bad++;                                                                                                \
	} while (0);

static int code_of(const char *why) {
	static const struct {
		const char *name;
		int code;
	} codes[] = {
		{"short", DEMO_IPCGEN_ESHORT},       {"length", DEMO_IPCGEN_ELENGTH}, {"trailing", DEMO_IPCGEN_ETRAILING},
		{"bool", DEMO_IPCGEN_EBOOL},         {"utf8", DEMO_IPCGEN_EUTF8},
	};
	size_t i;
	for (i = 0; i < sizeof codes / sizeof codes[0]; i++) {
		if (strcmp(codes[i].name, why) == 0) {
			return codes[i].code;
		}
	}
	fprintf(stderr, "invalid.json names unknown error %s\n", why);
	exit(2);
}

int main(void) {
	int cases = 0, bad = 0;
	FX_CASES
	FX_INVALID
	if (DEMO_SCALARS_TYPE != 1 || DEMO_TAGGED_TYPE != UINT32_C(0xFFFFFFFE) || DEMO_SHAPE_FIXED_SIZE != 88) {
		FAIL("type or size macros do not match example.ipc");
	}
	if (failures > 0) {
		fprintf(stderr, "C: %d failures\n", failures);
		return 1;
	}
	printf("C: %d values and %d invalid inputs pass\n", cases, bad);
	return 0;
}
