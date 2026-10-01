/* Conformance against spec/wire.json and spec/vectors/ring. */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "harness.h"
#include "internal.h"
#include "spec_wire.h"

struct field {
	const char *name;
	size_t offset;
	size_t size;
};

#define HDR_FIELD(f) {#f, offsetof(struct goipc_ring_hdr, f), sizeof(((struct goipc_ring_hdr *)0)->f)}

TEST(wire_constants_match_spec, 0)
{
	REQUIRE(GOIPC_RING_MAGIC == SPEC_RING_MAGIC);
	REQUIRE(GOIPC_RING_VERSION == SPEC_RING_VERSION);
	REQUIRE(GOIPC_HEADER_SIZE == SPEC_HEADER_SIZE);
	REQUIRE(sizeof(struct goipc_ring_hdr) == SPEC_HEADER_SIZE);
	REQUIRE(GOIPC_CACHE_LINE == SPEC_CACHE_LINE);
	REQUIRE(GOIPC_MIN_CAPACITY == SPEC_MIN_CAPACITY);
	REQUIRE(GOIPC_RECORD_HEADER_SIZE == SPEC_RECORD_HEADER_SIZE);
	REQUIRE(GOIPC_RECORD_ALIGNMENT == SPEC_RECORD_ALIGNMENT);
	REQUIRE(GOIPC_TYPE_PADDING == SPEC_TYPE_PADDING);
	REQUIRE(GOIPC_DEFAULT_CAPACITY == SPEC_DEFAULT_CAPACITY);
	REQUIRE(GOIPC_SIGNAL_MAX_TOKENS == SPEC_SIGNAL_MAX_TOKENS);
	REQUIRE(strcmp(GOIPC_NOT_EMPTY_SUFFIX, SPEC_NOT_EMPTY_SUFFIX) == 0);
	REQUIRE(strcmp(GOIPC_NOT_FULL_SUFFIX, SPEC_NOT_FULL_SUFFIX) == 0);
	REQUIRE(strcmp(GOIPC_C2O_SUFFIX, SPEC_C2O_SUFFIX) == 0);
	REQUIRE(strcmp(GOIPC_O2C_SUFFIX, SPEC_O2C_SUFFIX) == 0);
	REQUIRE(GOIPC_CONN_TYPE_DATA == SPEC_CONN_TYPE_DATA);
	REQUIRE(GOIPC_CONN_TYPE_EOF == SPEC_CONN_TYPE_EOF);
}

TEST(ring_header_offsets_match_spec, 0)
{
	const struct field spec[] = {SPEC_FIELDS};
	const struct field ours[] = {
		HDR_FIELD(magic), HDR_FIELD(version), HDR_FIELD(flags),
		HDR_FIELD(capacity), HDR_FIELD(tail), HDR_FIELD(head),
		HDR_FIELD(head_cache), HDR_FIELD(recv_waiters), HDR_FIELD(send_waiters),
	};
	REQUIRE(SPEC_FIELD_COUNT == sizeof ours / sizeof ours[0], "spec lists %d fields", SPEC_FIELD_COUNT);
	for (size_t i = 0; i < SPEC_FIELD_COUNT; i++) {
		const struct field *f = NULL;
		for (size_t k = 0; k < sizeof ours / sizeof ours[0]; k++)
			if (strcmp(ours[k].name, spec[i].name) == 0)
				f = &ours[k];
		REQUIRE(f != NULL, "spec names unknown field %s", spec[i].name);
		CHECK(f->offset == spec[i].offset, "%s offset %zu, spec %zu", f->name, f->offset, spec[i].offset);
		CHECK(f->size == spec[i].size, "%s size %zu, spec %zu", f->name, f->size, spec[i].size);
	}
}

static void path_from_pattern(char *out, size_t len, const char *pattern, const char *name)
{
	const char *at = strstr(pattern, "{name}");
	snprintf(out, len, "%.*s%s%s", (int)(at - pattern), pattern, name, at + strlen("{name}"));
}

TEST(paths_match_spec, 0)
{
	char want[256];
	char *got = goipc__event_path("x");
	path_from_pattern(want, sizeof want, SPEC_EVENT_PATH, "x");
	CHECK(strcmp(got, want) == 0, "event path %s, spec %s", got, want);
	free(got);
	got = goipc__segment_path("x");
	path_from_pattern(want, sizeof want, SPEC_SEGMENT_PATH, "x");
	CHECK(strcmp(got, want) == 0, "segment path %s, spec %s", got, want);
	free(got);
}

/* ---- ring vectors ---- */

struct payload {
	uint8_t *bytes;
	size_t len;
};

static struct payload decode(const char *hex, int repeat)
{
	struct payload p = {NULL, 0};
	size_t n = strcmp(hex, "-") == 0 ? 0 : strlen(hex) / 2;
	p.len = n * (size_t)repeat;
	p.bytes = malloc(p.len + 1);
	REQUIRE(p.bytes != NULL);
	for (size_t i = 0; i < n; i++) {
		unsigned v;
		REQUIRE(sscanf(hex + 2 * i, "%2x", &v) == 1, "bad hex %s", hex);
		p.bytes[i] = (uint8_t)v;
	}
	for (int r = 1; r < repeat; r++)
		memcpy(p.bytes + n * (size_t)r, p.bytes, n);
	return p;
}

static int vector_error(const char *name)
{
	if (strcmp(name, "full") == 0)
		return GOIPC_EFULL;
	if (strcmp(name, "too_large") == 0)
		return GOIPC_ETOOLARGE;
	if (strcmp(name, "reserved_type") == 0)
		return GOIPC_ERESERVED;
	harness_fail(__FILE__, __LINE__, "unknown vector error %s", name);
	harness_abort_test();
}

static void *read_file(const char *path, size_t *len)
{
	FILE *f = fopen(path, "rb");
	REQUIRE(f != NULL, "open %s", path);
	fseek(f, 0, SEEK_END);
	long n = ftell(f);
	fseek(f, 0, SEEK_SET);
	void *buf = aligned_buffer((size_t)n);
	REQUIRE(buf != NULL);
	REQUIRE(fread(buf, 1, (size_t)n, f) == (size_t)n, "read %s", path);
	fclose(f);
	*len = (size_t)n;
	return buf;
}

struct record {
	uint32_t type;
	struct payload p;
};

struct collected {
	struct record recs[16];
	int n;
};

static void collect(void *ctx, uint32_t type, const uint8_t *payload, size_t len)
{
	struct collected *c = ctx;
	if (c->n == 16) {
		harness_fail(__FILE__, __LINE__, "more than 16 records");
		return;
	}
	struct record *r = &c->recs[c->n++];
	r->type = type;
	r->p.len = len;
	r->p.bytes = malloc(len + 1);
	memcpy(r->p.bytes, payload, len);
}

static void discard(void *ctx, uint32_t type, const uint8_t *payload, size_t len)
{
	(void)ctx, (void)type, (void)payload, (void)len;
}

struct vcase {
	char name[64];
	size_t size;
	uint64_t head, tail;
	uint8_t *buf;
	goipc_ring ring;
	struct record want[16];
	int nwant;
};

static void run_op(struct vcase *c, char *line)
{
	char op[16], hex[16384], then[16], err[32];
	unsigned long type;
	int repeat, limit;
	REQUIRE(sscanf(line, "op %15s %lu %16383s %d %15s %d %31s", op, &type, hex, &repeat, then, &limit, err) == 7, "bad line %s", line);
	struct payload p = decode(hex, repeat);
	int rc;
	if (strcmp(op, "write") == 0) {
		rc = goipc_ring_try_write(&c->ring, (uint32_t)type, p.bytes, p.len);
	} else if (strcmp(op, "claim") == 0) {
		goipc_claim cl;
		rc = goipc_ring_try_claim(&c->ring, (uint32_t)type, p.len, &cl);
		if (rc == GOIPC_OK) {
			REQUIRE(cl.len == p.len);
			memcpy(cl.bytes, p.bytes, p.len);
			if (strcmp(then, "commit") == 0)
				goipc_claim_commit(&cl);
			else if (strcmp(then, "abort") == 0)
				goipc_claim_abort(&cl);
			else
				REQUIRE(strcmp(then, "none") == 0, "unknown then %s", then);
		}
	} else {
		REQUIRE(strcmp(op, "read") == 0, "unknown op %s", op);
		rc = goipc_ring_read(&c->ring, limit, discard, NULL);
		if (rc > 0)
			rc = GOIPC_OK;
	}
	free(p.bytes);
	if (strcmp(err, "-") == 0)
		REQUIRE_RC(rc, GOIPC_OK);
	else
		REQUIRE_RC(rc, vector_error(err));
}

static void finish_case(struct vcase *c)
{
	char path[512];
	snprintf(path, sizeof path, "%s/vectors/ring/%s.bin", GOIPC_SPEC_DIR, c->name);
	size_t len;
	uint8_t *want = read_file(path, &len);
	REQUIRE(len == c->size, "%s: %s holds %zu bytes, case says %zu", c->name, path, len, c->size);
	for (size_t i = 0; i < len; i++)
		REQUIRE(want[i] == c->buf[i], "%s: byte %zu is %02x, %s holds %02x", c->name, i, c->buf[i], path, want[i]);

	goipc_ring r;
	REQUIRE_RC(goipc_ring_attach(&r, want, len), GOIPC_OK);
	struct goipc_ring_hdr *h = goipc__hdr(&r);
	CHECK(atomic_load(&h->head) == c->head, "%s: head %llu, want %llu", c->name, (unsigned long long)atomic_load(&h->head), (unsigned long long)c->head);
	CHECK(atomic_load(&h->tail) == c->tail, "%s: tail %llu, want %llu", c->name, (unsigned long long)atomic_load(&h->tail), (unsigned long long)c->tail);

	struct collected got = {.n = 0};
	int n = goipc_ring_read(&r, 1 << 30, collect, &got);
	REQUIRE(n == c->nwant, "%s: read %d records, want %d", c->name, n, c->nwant);
	for (int i = 0; i < n; i++) {
		CHECK(got.recs[i].type == c->want[i].type, "%s: record %d type %u, want %u", c->name, i, got.recs[i].type, c->want[i].type);
		CHECK(got.recs[i].p.len == c->want[i].p.len && memcmp(got.recs[i].p.bytes, c->want[i].p.bytes, c->want[i].p.len) == 0,
		      "%s: record %d payload differs", c->name, i);
		free(got.recs[i].p.bytes);
		free(c->want[i].p.bytes);
	}
	free(want);
	free(c->buf);
}

TEST(ring_vectors, 0)
{
	FILE *f = fopen(GOIPC_VECTORS_FILE, "r");
	REQUIRE(f != NULL, "open %s", GOIPC_VECTORS_FILE);
	static char line[20000];
	struct vcase c;
	int cases = 0;
	while (fgets(line, sizeof line, f) != NULL) {
		line[strcspn(line, "\n")] = '\0';
		if (strncmp(line, "case ", 5) == 0) {
			memset(&c, 0, sizeof c);
			unsigned long long head, tail;
			REQUIRE(sscanf(line, "case %63s %zu %llu %llu", c.name, &c.size, &head, &tail) == 4, "bad line %s", line);
			c.head = head;
			c.tail = tail;
			c.buf = aligned_buffer(c.size);
			REQUIRE(c.buf != NULL);
			REQUIRE_RC(goipc_ring_init(&c.ring, c.buf, c.size), GOIPC_OK);
		} else if (strncmp(line, "op ", 3) == 0) {
			run_op(&c, line);
		} else if (strncmp(line, "rec ", 4) == 0) {
			static char hex[16384];
			unsigned long type;
			int repeat;
			REQUIRE(sscanf(line, "rec %lu %16383s %d", &type, hex, &repeat) == 3, "bad line %s", line);
			REQUIRE(c.nwant < 16);
			c.want[c.nwant].type = (uint32_t)type;
			c.want[c.nwant].p = decode(hex, repeat);
			c.nwant++;
		} else {
			REQUIRE(strcmp(line, "end") == 0, "bad line %s", line);
			finish_case(&c);
			cases++;
		}
	}
	fclose(f);
	REQUIRE(cases >= 8, "only %d cases", cases);
}
