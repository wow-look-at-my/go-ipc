#include <stdint.h>
#include <string.h>

#include "internal.h"

_Static_assert(offsetof(struct goipc_ring_hdr, magic) == 0, "magic offset");
_Static_assert(offsetof(struct goipc_ring_hdr, version) == 8, "version offset");
_Static_assert(offsetof(struct goipc_ring_hdr, flags) == 12, "flags offset");
_Static_assert(offsetof(struct goipc_ring_hdr, capacity) == 16, "capacity offset");
_Static_assert(offsetof(struct goipc_ring_hdr, tail) == 128, "tail offset");
_Static_assert(offsetof(struct goipc_ring_hdr, head) == 256, "head offset");
_Static_assert(offsetof(struct goipc_ring_hdr, head_cache) == 384, "head_cache offset");
_Static_assert(offsetof(struct goipc_ring_hdr, recv_waiters) == 392, "recv_waiters offset");
_Static_assert(offsetof(struct goipc_ring_hdr, send_waiters) == 396, "send_waiters offset");

static uint64_t align8(int32_t n)
{
	return ((uint64_t)(uint32_t)n + (GOIPC_RECORD_ALIGNMENT - 1)) & ~(uint64_t)(GOIPC_RECORD_ALIGNMENT - 1);
}

static _Atomic int32_t *len_at(const goipc_ring *r, uint64_t idx)
{
	return (_Atomic int32_t *)(void *)(r->data + idx);
}

/* The type field is plain memory; the len store and load beside it order it. */
static uint32_t load_type(const goipc_ring *r, uint64_t idx)
{
	uint32_t v;
	memcpy(&v, r->data + idx + 4, sizeof v);
	return v;
}

static void store_type(goipc_ring *r, uint64_t idx, uint32_t v)
{
	memcpy(r->data + idx + 4, &v, sizeof v);
}

size_t goipc_ring_size(size_t capacity)
{
	return GOIPC_HEADER_SIZE + capacity;
}

static int check_buffer(void *buf, size_t len)
{
	if (buf == NULL)
		return GOIPC_EINVAL;
	if (len < GOIPC_HEADER_SIZE + GOIPC_MIN_CAPACITY)
		return GOIPC_ETOOSMALL;
	if ((uintptr_t)buf % 8 != 0)
		return GOIPC_EUNALIGNED;
	return GOIPC_OK;
}

static void set_ring(goipc_ring *r, void *buf, uint64_t capacity)
{
	r->hdr = buf;
	r->data = (uint8_t *)buf + GOIPC_HEADER_SIZE;
	r->mask = capacity - 1;
}

int goipc_ring_init(goipc_ring *r, void *buf, size_t len)
{
	int rc = check_buffer(buf, len);
	if (rc != GOIPC_OK)
		return rc;
	uint64_t room = (uint64_t)(len - GOIPC_HEADER_SIZE);
	uint64_t capacity = UINT64_C(1) << (63 - __builtin_clzll(room));

	memset(buf, 0, len);
	struct goipc_ring_hdr *h = buf;
	h->capacity = capacity;
	h->version = GOIPC_RING_VERSION;
	/* A peer that attaches meanwhile sees no magic or a complete header. */
	atomic_store(&h->magic, GOIPC_RING_MAGIC);
	set_ring(r, buf, capacity);
	return GOIPC_OK;
}

int goipc_ring_attach(goipc_ring *r, void *buf, size_t len)
{
	int rc = check_buffer(buf, len);
	if (rc != GOIPC_OK)
		return rc;
	struct goipc_ring_hdr *h = buf;
	if (atomic_load(&h->magic) != GOIPC_RING_MAGIC || h->version != GOIPC_RING_VERSION)
		return GOIPC_EBADLAYOUT;
	uint64_t capacity = h->capacity;
	if (capacity < GOIPC_MIN_CAPACITY || (capacity & (capacity - 1)) != 0 || (uint64_t)len - GOIPC_HEADER_SIZE < capacity)
		return GOIPC_EBADLAYOUT;
	set_ring(r, buf, capacity);
	return GOIPC_OK;
}

size_t goipc_ring_capacity(const goipc_ring *r)
{
	return (size_t)r->mask + 1;
}

size_t goipc_ring_max_message_size(const goipc_ring *r)
{
	size_t limit = goipc_ring_capacity(r) / 2 - GOIPC_RECORD_HEADER_SIZE;
	if (limit > (size_t)INT32_MAX - GOIPC_RECORD_HEADER_SIZE)
		limit = (size_t)INT32_MAX - GOIPC_RECORD_HEADER_SIZE;
	return limit;
}

size_t goipc_ring_buffered(const goipc_ring *r)
{
	struct goipc_ring_hdr *h = goipc__hdr(r);
	return (size_t)(atomic_load(&h->tail) - atomic_load(&h->head));
}

int goipc_ring_empty(const goipc_ring *r)
{
	struct goipc_ring_hdr *h = goipc__hdr(r);
	return atomic_load(&h->head) == atomic_load(&h->tail);
}

int goipc_ring_try_claim(goipc_ring *r, uint32_t type, size_t len, goipc_claim *out)
{
	if (type == GOIPC_TYPE_PADDING)
		return GOIPC_ERESERVED;
	if (len > goipc_ring_max_message_size(r))
		return GOIPC_ETOOLARGE;

	struct goipc_ring_hdr *h = goipc__hdr(r);
	int32_t rec = (int32_t)(GOIPC_RECORD_HEADER_SIZE + len);
	uint64_t aligned = align8(rec);
	uint64_t capacity = r->mask + 1;
	uint64_t index;

	for (;;) {
		uint64_t tail = atomic_load(&h->tail);
		uint64_t head = atomic_load(&h->head_cache);

		index = tail & r->mask;
		uint64_t to_end = capacity - index;
		uint64_t need = aligned;
		if (to_end < aligned)
			need = aligned + to_end;

		/* A slow producer can store an old head into head_cache, so
		 * tail - head_cache can exceed capacity. The unsigned subtraction
		 * would then wrap and report room that does not exist. */
		uint64_t used = tail - head;
		if (used > capacity || capacity - used < need) {
			head = atomic_load(&h->head);
			used = tail - head;
			/* head passed this tail value, so the tail is stale. */
			if ((int64_t)used < 0)
				continue;
			if (used > capacity || capacity - used < need)
				return GOIPC_EFULL;
			atomic_store(&h->head_cache, head);
		}

		if (!atomic_compare_exchange_strong(&h->tail, &tail, tail + need))
			continue;
		if (need != aligned) {
			store_type(r, index, GOIPC_TYPE_PADDING);
			atomic_store(len_at(r, index), (int32_t)to_end);
			index = 0;
		}
		break;
	}

	/* A negative length marks a claim in flight; the reader stops on it. */
	atomic_store(len_at(r, index), -rec);
	store_type(r, index, type);

	out->ring = r;
	out->index = index;
	out->total = rec;
	out->bytes = r->data + index + GOIPC_RECORD_HEADER_SIZE;
	out->len = len;
	return GOIPC_OK;
}

void goipc_claim_commit(goipc_claim *c)
{
	atomic_store(len_at(c->ring, c->index), c->total);
}

void goipc_claim_abort(goipc_claim *c)
{
	store_type(c->ring, c->index, GOIPC_TYPE_PADDING);
	atomic_store(len_at(c->ring, c->index), c->total);
}

int goipc_ring_try_write(goipc_ring *r, uint32_t type, const void *payload, size_t len)
{
	goipc_claim c;
	int rc = goipc_ring_try_claim(r, type, len, &c);
	if (rc != GOIPC_OK)
		return rc;
	if (len > 0)
		memcpy(c.bytes, payload, len);
	goipc_claim_commit(&c);
	return GOIPC_OK;
}

int goipc__ring_read(goipc_ring *r, int limit, goipc_read_fn fn, void *ctx, size_t maxlen, size_t *need, uint32_t *need_type)
{
	if (limit <= 0)
		return 0;
	struct goipc_ring_hdr *h = goipc__hdr(r);
	uint64_t capacity = r->mask + 1;
	uint64_t head = atomic_load(&h->head);
	uint64_t available = atomic_load(&h->tail) - head;
	uint64_t consumed = 0;
	int count = 0;
	int rc = GOIPC_OK;

	while (count < limit && consumed < available) {
		uint64_t index = (head + consumed) & r->mask;
		int32_t len = atomic_load(len_at(r, index));
		if (len <= 0)
			break;
		uint64_t step = align8(len);
		/* The bound against capacity keeps a corrupt length inside the region. */
		if (len < (int32_t)GOIPC_RECORD_HEADER_SIZE || step > available - consumed || index + (uint64_t)len > capacity)
			return GOIPC_ECORRUPT;
		uint32_t type = load_type(r, index);
		size_t plen = (size_t)len - GOIPC_RECORD_HEADER_SIZE;
		if (type != GOIPC_TYPE_PADDING) {
			if (plen > maxlen) {
				*need = plen;
				*need_type = type;
				rc = GOIPC_EBUFFER;
				break;
			}
			fn(ctx, type, r->data + index + GOIPC_RECORD_HEADER_SIZE, plen);
			count++;
		}
		consumed += step;
		/* A producer claims before it stores -rec, so every byte outside
		 * [head, tail) must read as zero or a stale payload looks like a len. */
		memset(r->data + index + 4, 0, (size_t)step - 4);
		atomic_store(len_at(r, index), 0);
	}

	if (consumed > 0)
		atomic_store(&h->head, head + consumed);
	return rc != GOIPC_OK ? rc : count;
}

int goipc_ring_read(goipc_ring *r, int limit, goipc_read_fn fn, void *ctx)
{
	size_t need;
	uint32_t type;
	return goipc__ring_read(r, limit, fn, ctx, SIZE_MAX, &need, &type);
}

struct recv_ctx {
	void *dst;
	uint32_t *type;
	size_t *len;
};

static void copy_out(void *ctx, uint32_t type, const uint8_t *payload, size_t len)
{
	struct recv_ctx *rc = ctx;
	if (len > 0)
		memcpy(rc->dst, payload, len);
	*rc->type = type;
	*rc->len = len;
}

int goipc_ring_try_recv(goipc_ring *r, void *dst, size_t cap, uint32_t *type, size_t *len)
{
	struct recv_ctx ctx = {dst, type, len};
	int n = goipc__ring_read(r, 1, copy_out, &ctx, cap, len, type);
	if (n < 0)
		return n;
	return n == 0 ? GOIPC_EEMPTY : GOIPC_OK;
}
