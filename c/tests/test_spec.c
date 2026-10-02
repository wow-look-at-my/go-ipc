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
	REQUIRE(strcmp(GOIPC_SERVICE_REGISTRY_SUFFIX, SPEC_SERVICE_REGISTRY_SUFFIX) == 0);
	REQUIRE(strcmp(GOIPC_SERVICE_CLIENT_PREFIX, SPEC_SERVICE_CLIENT_PREFIX) == 0);
	REQUIRE(GOIPC_INC_LEN == SPEC_SERVICE_CLIENT_ID_HEX_DIGITS);
	REQUIRE(GOIPC_SERVICE_RESERVED_TYPE_MIN == SPEC_SERVICE_RESERVED_TYPE_MIN);
	REQUIRE(GOIPC_SERVICE_TYPE_KNOCK == SPEC_SERVICE_TYPE_KNOCK);
	REQUIRE(GOIPC_SERVICE_TYPE_HELLO == SPEC_SERVICE_TYPE_HELLO);
	REQUIRE(GOIPC_SERVICE_TYPE_ERROR == SPEC_SERVICE_TYPE_ERROR);
	REQUIRE(GOIPC_SERVICE_SEQUENCE_SIZE == SPEC_SERVICE_SEQUENCE_SIZE);
	REQUIRE(SPEC_SERVICE_FIRST_SEQUENCE == 1u);
	REQUIRE(GOIPC_CONTROL_SIZE == SPEC_CONTROL_SIZE);
	REQUIRE(GOIPC_CLAIM_SLOTS == SPEC_SLOTS_COUNT);
	REQUIRE(GOIPC_CLAIM_SLOT_SIZE == SPEC_SLOT_SIZE);
	REQUIRE(offsetof(struct goipc_ring_hdr, slots) == SPEC_SLOTS_OFFSET);
	REQUIRE(GOIPC_NO_INTENT == SPEC_NO_INTENT);
	REQUIRE(GOIPC_PROC_NONE == SPEC_PROC_NONE);
	REQUIRE(GOIPC_PROC_PENDING == SPEC_PROC_PENDING);
	REQUIRE(GOIPC_PROC_WATCHABLE == UINT64_C(1) << SPEC_PROC_WATCHABLE_BIT);
	REQUIRE(GOIPC_PROC_ANY == UINT64_C(1) << SPEC_PROC_ALWAYS_SET_BIT);
	REQUIRE(GOIPC_INC_LEN == SPEC_INSTANCE_ID_HEX_DIGITS);
	REQUIRE(strcmp(GOIPC_SHM_DIR, SPEC_RUNTIME_DIR) == 0);
}

TEST(claim_slot_offsets_match_spec, 0)
{
	const struct field spec[] = {SPEC_SLOT_FIELDS};
	const struct field ours[] = {
		{"owner", offsetof(struct goipc_claim_slot, owner), 8},
		{"at", offsetof(struct goipc_claim_slot, at), 8},
		{"size", offsetof(struct goipc_claim_slot, size), 8},
	};
	REQUIRE(SPEC_SLOT_FIELD_COUNT == sizeof ours / sizeof ours[0], "spec lists %d slot fields", SPEC_SLOT_FIELD_COUNT);
	for (size_t i = 0; i < SPEC_SLOT_FIELD_COUNT; i++) {
		CHECK(strcmp(ours[i].name, spec[i].name) == 0, "slot field %zu is %s, spec %s", i, ours[i].name, spec[i].name);
		CHECK(ours[i].offset == spec[i].offset && ours[i].size == spec[i].size, "slot field %s", spec[i].name);
	}
}

TEST(ring_header_offsets_match_spec, 0)
{
	const struct field spec[] = {SPEC_FIELDS};
	const struct field ours[] = {
		HDR_FIELD(magic), HDR_FIELD(version), HDR_FIELD(flags),
		HDR_FIELD(capacity), HDR_FIELD(consumer), HDR_FIELD(tail), HDR_FIELD(head),
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

/* expand replaces each {key} of pattern with its value. */
static void expand(char *out, size_t len, const char *pattern, const char *const kv[][2], size_t n)
{
	size_t o = 0;
	while (*pattern != '\0' && o + 1 < len) {
		bool hit = false;
		for (size_t i = 0; i < n && !hit; i++) {
			size_t kl = strlen(kv[i][0]);
			if (strncmp(pattern, kv[i][0], kl) == 0) {
				o += (size_t)snprintf(out + o, len - o, "%s", kv[i][1]);
				pattern += kl;
				hit = true;
			}
		}
		if (!hit)
			out[o++] = *pattern++;
	}
	out[o < len ? o : len - 1] = '\0';
}

static void check_path(char *got, const char *pattern, const char *const kv[][2], size_t n, const char *what)
{
	char want[256];
	expand(want, sizeof want, pattern, kv, n);
	CHECK(got != NULL && strcmp(got, want) == 0, "%s path %s, spec %s", what, got, want);
	free(got);
}

TEST(paths_match_spec, 0)
{
	const char *id = "0123456789abcdef";
	const char *const kv[][2] = {{"{name}", "x"}, {"{id}", "0123456789abcdef"}, {"{event}", "x"}, {"{procid}", "c0ffee0000000042"}};
	char *inst = goipc__instance_name("x", id);
	char *ne = goipc__join(inst, GOIPC_NOT_EMPTY_SUFFIX, "");
	char *nf = goipc__join(inst, GOIPC_NOT_FULL_SUFFIX, "");
	check_path(goipc__event_path("x"), SPEC_EVENT_PATH, kv, 4, "event");
	check_path(goipc__name_path("x"), SPEC_NAME_FILE, kv, 4, "name file");
	check_path(goipc__inc_path("x"), SPEC_INSTANCE_FILE, kv, 4, "instance file");
	check_path(goipc__segment_path(inst), SPEC_SEGMENT_PATH, kv, 4, "segment");
	check_path(goipc__event_path(ne), SPEC_NOT_EMPTY_EVENT, kv, 4, "not-empty event");
	check_path(goipc__event_path(nf), SPEC_NOT_FULL_EVENT, kv, 4, "not-full event");
	check_path(goipc__life_path(UINT64_C(0xc0ffee0000000042)), SPEC_LIFE_SOCKET, kv, 4, "life socket");
	CHECK(SPEC_LIFE_SOCKET_HEX_DIGITS == 16);
	CHECK(strcmp(SPEC_LIFE_SOCKET_TEMP_SUFFIX, GOIPC_LIFE_TEMP_SUFFIX) == 0);
	CHECK(SPEC_FILE_MODE == 0600);
	free(inst);
	free(ne);
	free(nf);
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
	uint64_t head, tail, consumer;
	uint8_t *buf;
	goipc_ring ring;
	struct record want[16];
	int nwant;
};

static bool never_dead(void *ctx, uint64_t id)
{
	(void)ctx, (void)id;
	return false;
}

static void run_op(struct vcase *c, char *line)
{
	char op[16], hex[16384], then[16], err[32];
	unsigned long type;
	unsigned long long owner;
	int repeat, limit, slot;
	REQUIRE(sscanf(line, "op %15s %lu %16383s %d %15s %d %31s %d %llx", op, &type, hex, &repeat, then, &limit, err, &slot, &owner) == 9,
		"bad line %s", line);
	struct payload p = decode(hex, repeat);
	int rc;
	if (strcmp(op, "write") == 0) {
		goipc_claim cl;
		rc = goipc__ring_try_claim(&c->ring, slot, (uint32_t)type, p.len, &cl);
		if (rc == GOIPC_OK) {
			memcpy(cl.bytes, p.bytes, p.len);
			goipc_claim_commit(&cl);
		}
	} else if (strcmp(op, "acquire") == 0) {
		int got = -1;
		rc = goipc__ring_acquire_slot(&c->ring, owner, never_dead, NULL, &got);
		REQUIRE(rc != GOIPC_OK || got == slot, "%s: acquire took slot %d, want %d", c->name, got, slot);
	} else if (strcmp(op, "drop") == 0) {
		goipc__ring_drop_slot(&c->ring, owner, slot);
		rc = GOIPC_OK;
	} else if (strcmp(op, "claim") == 0) {
		goipc_claim cl;
		rc = goipc__ring_try_claim(&c->ring, slot, (uint32_t)type, p.len, &cl);
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
	CHECK(atomic_load(&h->consumer) == c->consumer, "%s: consumer %llx, want %llx", c->name, (unsigned long long)atomic_load(&h->consumer),
	      (unsigned long long)c->consumer);

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
			unsigned long long head, tail, consumer;
			REQUIRE(sscanf(line, "case %63s %zu %llu %llu %llx", c.name, &c.size, &head, &tail, &consumer) == 5, "bad line %s", line);
			c.head = head;
			c.tail = tail;
			c.consumer = consumer;
			c.buf = aligned_buffer(c.size);
			REQUIRE(c.buf != NULL);
			REQUIRE_RC(goipc__ring_init(&c.ring, c.buf, c.size, c.consumer), GOIPC_OK);
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
