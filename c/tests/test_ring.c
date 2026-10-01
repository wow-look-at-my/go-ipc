#define _GNU_SOURCE
#include <pthread.h>
#include <sched.h>
#include <stdlib.h>
#include <string.h>

#include "harness.h"
#include "internal.h"

static goipc_ring new_ring(size_t capacity, void **buf)
{
	size_t len = goipc_ring_size(capacity);
	*buf = aligned_buffer(len);
	REQUIRE(*buf != NULL);
	goipc_ring r;
	REQUIRE_RC(goipc_ring_init(&r, *buf, len), GOIPC_OK);
	return r;
}

TEST(ring_init_and_attach_errors, 0)
{
	uint8_t *buf = aligned_buffer(8 + goipc_ring_size(8192));
	goipc_ring r;
	REQUIRE_RC(goipc_ring_init(&r, buf, GOIPC_HEADER_SIZE + GOIPC_MIN_CAPACITY - 1), GOIPC_ETOOSMALL);
	REQUIRE_RC(goipc_ring_attach(&r, buf, GOIPC_HEADER_SIZE + GOIPC_MIN_CAPACITY - 1), GOIPC_ETOOSMALL);
	REQUIRE_RC(goipc_ring_init(&r, buf + 1, goipc_ring_size(4096)), GOIPC_EUNALIGNED);
	REQUIRE_RC(goipc_ring_attach(&r, buf + 4, goipc_ring_size(4096)), GOIPC_EUNALIGNED);
	REQUIRE_RC(goipc_ring_init(&r, NULL, goipc_ring_size(4096)), GOIPC_EINVAL);

	/* A zeroed buffer has no magic. */
	REQUIRE_RC(goipc_ring_attach(&r, buf, goipc_ring_size(4096)), GOIPC_EBADLAYOUT);

	REQUIRE_RC(goipc_ring_init(&r, buf, goipc_ring_size(8192)), GOIPC_OK);
	struct goipc_ring_hdr *h = (struct goipc_ring_hdr *)buf;
	REQUIRE(h->capacity == 8192);
	REQUIRE_RC(goipc_ring_attach(&r, buf, goipc_ring_size(8192)), GOIPC_OK);
	REQUIRE_RC(goipc_ring_attach(&r, buf, goipc_ring_size(8192) - 1), GOIPC_EBADLAYOUT);

	h->version = 2;
	REQUIRE_RC(goipc_ring_attach(&r, buf, goipc_ring_size(8192)), GOIPC_EBADLAYOUT);
	h->version = GOIPC_RING_VERSION;
	h->capacity = 6000;
	REQUIRE_RC(goipc_ring_attach(&r, buf, goipc_ring_size(8192)), GOIPC_EBADLAYOUT);
	h->capacity = 2048;
	REQUIRE_RC(goipc_ring_attach(&r, buf, goipc_ring_size(8192)), GOIPC_EBADLAYOUT);
	h->capacity = 8192;
	atomic_store(&h->magic, GOIPC_RING_MAGIC + 1);
	REQUIRE_RC(goipc_ring_attach(&r, buf, goipc_ring_size(8192)), GOIPC_EBADLAYOUT);
	free(buf);
}

TEST(ring_capacity_rounds_down_to_power_of_two, 0)
{
	uint8_t *buf = aligned_buffer(GOIPC_HEADER_SIZE + 9000);
	goipc_ring r;
	REQUIRE_RC(goipc_ring_init(&r, buf, GOIPC_HEADER_SIZE + 9000), GOIPC_OK);
	REQUIRE(goipc_ring_capacity(&r) == 8192);
	REQUIRE(goipc_ring_max_message_size(&r) == 4096 - 8);
	REQUIRE(goipc_ring_empty(&r));
	REQUIRE(goipc_ring_buffered(&r) == 0);
	free(buf);
}

TEST(ring_max_message_size, 0)
{
	void *buf;
	goipc_ring r = new_ring(4096, &buf);
	size_t max = goipc_ring_max_message_size(&r);
	REQUIRE(max == 2040);
	uint8_t *p = calloc(1, max + 1);
	REQUIRE_RC(goipc_ring_try_write(&r, 1, p, max + 1), GOIPC_ETOOLARGE);
	REQUIRE_RC(goipc_ring_try_write(&r, GOIPC_TYPE_PADDING, p, 1), GOIPC_ERESERVED);
	REQUIRE(goipc_ring_empty(&r));
	REQUIRE_RC(goipc_ring_try_write(&r, 1, p, max), GOIPC_OK);
	REQUIRE(goipc_ring_buffered(&r) == 2048);
	free(p);
	free(buf);
}

struct seen {
	uint32_t types[64];
	size_t lens[64];
	int n;
};

static void record(void *ctx, uint32_t type, const uint8_t *payload, size_t len)
{
	struct seen *s = ctx;
	(void)payload;
	if (s->n < 64) {
		s->types[s->n] = type;
		s->lens[s->n] = len;
	}
	s->n++;
}

TEST(ring_wrap_writes_padding, 0)
{
	void *buf;
	goipc_ring r = new_ring(4096, &buf);
	struct goipc_ring_hdr *h = goipc__hdr(&r);
	static uint8_t p[2000];
	struct seen s = {.n = 0};

	REQUIRE_RC(goipc_ring_try_write(&r, 10, p, 2000), GOIPC_OK);
	REQUIRE(goipc_ring_read(&r, 1, record, &s) == 1);
	REQUIRE_RC(goipc_ring_try_write(&r, 11, p, 1500), GOIPC_OK);
	REQUIRE(atomic_load(&h->tail) == 3520);
	/* */
	REQUIRE_RC(goipc_ring_try_write(&r, 12, p, 1000), GOIPC_OK);
	REQUIRE(atomic_load(&h->tail) == 3520 + 576 + 1008);
	int32_t len;
	uint32_t type;
	memcpy(&len, r.data + 3520, 4);
	memcpy(&type, r.data + 3524, 4);
	REQUIRE(len == 576 && type == GOIPC_TYPE_PADDING, "padding len %d type %x", len, type);
	memcpy(&len, r.data, 4);
	memcpy(&type, r.data + 4, 4);
	REQUIRE(len == 1008 && type == 12, "wrapped record len %d type %u", len, type);

	s.n = 0;
	REQUIRE(goipc_ring_read(&r, 10, record, &s) == 2);
	REQUIRE(s.types[0] == 11 && s.lens[0] == 1500);
	REQUIRE(s.types[1] == 12 && s.lens[1] == 1000);
	REQUIRE(goipc_ring_empty(&r));
	REQUIRE(atomic_load(&h->head) == 3520 + 576 + 1008);
	free(buf);
}

TEST(ring_full_reports_efull_and_frees_on_read, 0)
{
	void *buf;
	goipc_ring r = new_ring(4096, &buf);
	static uint8_t p[1016];
	int n = 0;
	while (goipc_ring_try_write(&r, 1, p, sizeof p) == GOIPC_OK)
		n++;
	REQUIRE(n == 4, "wrote %d", n);
	REQUIRE_RC(goipc_ring_try_write(&r, 1, p, 0), GOIPC_EFULL);
	struct seen s = {.n = 0};
	REQUIRE(goipc_ring_read(&r, 1, record, &s) == 1);
	REQUIRE_RC(goipc_ring_try_write(&r, 1, p, sizeof p), GOIPC_OK);
	free(buf);
}

/* A preempted producer can store a head value that is more than a lap old.
 * The claim must still see a full ring as full. */
TEST(ring_stale_head_cache_does_not_overrun, 0)
{
	void *buf;
	goipc_ring r = new_ring(4096, &buf);
	struct goipc_ring_hdr *h = goipc__hdr(&r);
	static uint8_t p[1016];
	struct seen s = {.n = 0};
	for (int lap = 0; lap < 2; lap++) {
		REQUIRE(goipc_ring_read(&r, 10, record, &s) >= 0);
		while (goipc_ring_try_write(&r, 1, p, sizeof p) == GOIPC_OK)
			;
	}
	uint64_t head = atomic_load(&h->head);
	REQUIRE(atomic_load(&h->tail) - head == 4096);
	atomic_store(&h->head_cache, head - 4096);
	REQUIRE_RC(goipc_ring_try_write(&r, 1, p, 0), GOIPC_EFULL);
	REQUIRE(atomic_load(&h->tail) - head == 4096);
	REQUIRE(goipc_ring_read(&r, 1, record, &s) == 1);
	REQUIRE_RC(goipc_ring_try_write(&r, 1, p, sizeof p), GOIPC_OK);
	free(buf);
}

/* A producer moves tail before it stores -rec. Until it does, the reader must
 * see zero there, not bytes left from an earlier payload. */
TEST(ring_unpublished_claim_over_old_payload_reads_nothing, 0)
{
	void *buf;
	goipc_ring r = new_ring(4096, &buf);
	struct goipc_ring_hdr *h = goipc__hdr(&r);
	static uint8_t p[2040];
	for (size_t i = 0; i < sizeof p; i += 8) {
		int32_t len = 16;
		uint32_t type = 7;
		memcpy(p + i, &len, 4);
		memcpy(p + i + 4, &type, 4);
	}
	struct seen s = {.n = 0};
	REQUIRE_RC(goipc_ring_try_write(&r, 1, p, sizeof p), GOIPC_OK);
	REQUIRE_RC(goipc_ring_try_write(&r, 1, p, sizeof p), GOIPC_OK);
	REQUIRE(goipc_ring_read(&r, 10, record, &s) == 2);
	for (size_t i = 0; i < 4096; i++)
		REQUIRE(r.data[i] == 0, "byte %zu of a consumed record is %02x", i, r.data[i]);

	/* The next lap walks through the old payload one empty record at a time. */
	for (uint64_t idx = 0; idx < 2048; idx += 8) {
		atomic_fetch_add(&h->tail, 8);
		int n = goipc_ring_read(&r, 10, record, &s);
		REQUIRE(n == 0, "unpublished claim at %llu: read returned %d", (unsigned long long)idx, n);
		uint32_t type = 9;
		memcpy(r.data + idx + 4, &type, 4);
		atomic_store((_Atomic int32_t *)(void *)(r.data + idx), 8);
		REQUIRE(goipc_ring_read(&r, 10, record, &s) == 1);
	}
	free(buf);
}

TEST(ring_abort_becomes_padding, 0)
{
	void *buf;
	goipc_ring r = new_ring(4096, &buf);
	struct goipc_ring_hdr *h = goipc__hdr(&r);
	goipc_claim c;
	REQUIRE_RC(goipc_ring_try_claim(&r, 5, 10, &c), GOIPC_OK);
	REQUIRE(c.len == 10 && c.bytes == r.data + 8);
	goipc_claim_abort(&c);
	struct seen s = {.n = 0};
	REQUIRE(goipc_ring_read(&r, 10, record, &s) == 0);
	REQUIRE(atomic_load(&h->head) == 24, "head %llu", (unsigned long long)atomic_load(&h->head));
	REQUIRE(goipc_ring_empty(&r));
	free(buf);
}

TEST(ring_open_claim_stops_reader, 0)
{
	void *buf;
	goipc_ring r = new_ring(4096, &buf);
	goipc_claim c;
	REQUIRE_RC(goipc_ring_try_claim(&r, 5, 3, &c), GOIPC_OK);
	memcpy(c.bytes, "abc", 3);
	REQUIRE_RC(goipc_ring_try_write(&r, 6, "d", 1), GOIPC_OK);
	struct seen s = {.n = 0};
	REQUIRE(goipc_ring_read(&r, 10, record, &s) == 0);
	int32_t len;
	memcpy(&len, r.data, 4);
	REQUIRE(len == -11, "claim marker %d", len);
	goipc_claim_commit(&c);
	REQUIRE(goipc_ring_read(&r, 10, record, &s) == 2);
	REQUIRE(s.types[0] == 5 && s.types[1] == 6);
	free(buf);
}

TEST(ring_try_recv_small_buffer_keeps_record, 0)
{
	void *buf;
	goipc_ring r = new_ring(4096, &buf);
	uint8_t msg[100], out[100];
	for (int i = 0; i < 100; i++)
		msg[i] = (uint8_t)i;
	uint32_t type = 0;
	size_t len = 0;
	REQUIRE_RC(goipc_ring_try_recv(&r, out, sizeof out, &type, &len), GOIPC_EEMPTY);

	/* An aborted claim ahead of the message is stepped over either way. */
	goipc_claim c;
	REQUIRE_RC(goipc_ring_try_claim(&r, 1, 4, &c), GOIPC_OK);
	goipc_claim_abort(&c);
	REQUIRE_RC(goipc_ring_try_write(&r, 9, msg, sizeof msg), GOIPC_OK);
	REQUIRE_RC(goipc_ring_try_recv(&r, out, 50, &type, &len), GOIPC_EBUFFER);
	REQUIRE(len == 100, "need %zu", len);
	REQUIRE(!goipc_ring_empty(&r));
	REQUIRE(goipc_ring_buffered(&r) == 112);
	REQUIRE_RC(goipc_ring_try_recv(&r, out, 99, &type, &len), GOIPC_EBUFFER);
	REQUIRE_RC(goipc_ring_try_recv(&r, out, 100, &type, &len), GOIPC_OK);
	REQUIRE(type == 9 && len == 100 && memcmp(out, msg, 100) == 0);
	REQUIRE(goipc_ring_empty(&r));
	free(buf);
}

TEST(ring_detects_corrupt_length, 0)
{
	void *buf;
	goipc_ring r = new_ring(4096, &buf);
	struct seen s = {.n = 0};
	REQUIRE_RC(goipc_ring_try_write(&r, 1, "abcd", 4), GOIPC_OK);
	int32_t bad = 3;
	memcpy(r.data, &bad, 4);
	REQUIRE_RC(goipc_ring_read(&r, 10, record, &s), GOIPC_ECORRUPT);
	bad = 64;
	memcpy(r.data, &bad, 4);
	REQUIRE_RC(goipc_ring_read(&r, 10, record, &s), GOIPC_ECORRUPT);
	REQUIRE(s.n == 0);
	bad = 12;
	memcpy(r.data, &bad, 4);
	REQUIRE(goipc_ring_read(&r, 10, record, &s) == 1);
	free(buf);
}

/* ---- MPSC stress ---- */

#define PRODUCERS 4
#define PER_PRODUCER 20000
#define MIN_PAYLOAD 8
#define PAYLOAD_SPAN 61

static atomic_bool stress_stop;

struct producer {
	goipc_ring *r;
	uint32_t id;
};

static size_t stress_len(uint32_t seq)
{
	return MIN_PAYLOAD + (seq * 7) % PAYLOAD_SPAN;
}

/* The ring itself never blocks, so a test producer retries on a full ring.
 * The queue tests cover the parked path. */
static void *produce(void *arg)
{
	struct producer *p = arg;
	uint8_t msg[MIN_PAYLOAD + PAYLOAD_SPAN];
	for (uint32_t seq = 0; seq < PER_PRODUCER && !atomic_load(&stress_stop); seq++) {
		size_t len = stress_len(seq);
		memcpy(msg, &p->id, 4);
		memcpy(msg + 4, &seq, 4);
		for (size_t i = 8; i < len; i++)
			msg[i] = (uint8_t)(seq + i);
		int rc;
		while ((rc = goipc_ring_try_write(p->r, p->id, msg, len)) == GOIPC_EFULL && !atomic_load(&stress_stop))
			sched_yield();
		if (rc != GOIPC_EFULL)
			CHECK_RC(rc, GOIPC_OK);
	}
	return NULL;
}

struct consumer {
	uint32_t next[PRODUCERS];
	long total;
	bool bad;
};

static void consume(void *ctx, uint32_t type, const uint8_t *payload, size_t len)
{
	struct consumer *c = ctx;
	uint32_t id, seq;
	if (c->bad)
		return;
	if (len < 8) {
		CHECK(len >= 8, "phantom record: type %u len %zu", type, len);
		c->bad = true;
		return;
	}
	memcpy(&id, payload, 4);
	memcpy(&seq, payload + 4, 4);
	if (id != type || id >= PRODUCERS || seq != c->next[id] || len != stress_len(seq)) {
		CHECK(false, "type %u id %u seq %u len %zu", type, id, seq, len);
		c->bad = true;
		return;
	}
	for (size_t i = 8; i < len; i++) {
		if (payload[i] != (uint8_t)(seq + i)) {
			CHECK(false, "producer %u seq %u byte %zu differs", id, seq, i);
			c->bad = true;
			return;
		}
	}
	c->next[id] = seq + 1;
	c->total++;
}

/* Varied sizes put record headers where earlier laps had payload bytes. */
TEST(ring_mpsc_stress_keeps_per_producer_order, T_THREADS)
{
	void *buf;
	goipc_ring r = new_ring(4096, &buf);
	pthread_t th[PRODUCERS];
	struct producer ps[PRODUCERS];
	atomic_store(&stress_stop, false);
	for (uint32_t i = 0; i < PRODUCERS; i++) {
		ps[i] = (struct producer){&r, i};
		REQUIRE(pthread_create(&th[i], NULL, produce, &ps[i]) == 0);
	}
	struct consumer c = {{0}, 0, false};
	int err = 0;
	while (c.total < (long)PRODUCERS * PER_PRODUCER && !c.bad) {
		int n = goipc_ring_read(&r, 64, consume, &c);
		if (n < 0) {
			err = n;
			break;
		}
		if (n == 0)
			sched_yield();
	}
	atomic_store(&stress_stop, true);
	for (int i = 0; i < PRODUCERS; i++)
		pthread_join(th[i], NULL);
	REQUIRE_RC(err, GOIPC_OK);
	REQUIRE(!c.bad);
	REQUIRE(goipc_ring_empty(&r));
	free(buf);
}
