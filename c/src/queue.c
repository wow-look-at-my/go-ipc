#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "internal.h"

struct goipc_queue {
	char *name;
	int fd;
	void *map;
	size_t map_len;
	goipc_ring ring;
	goipc_event *ne;
	goipc_event *nf;
	bool owner;
	/* An operation in flight holds a pointer into the mapping, so close
	 * waits for it rather than unmap under it. */
	struct goipc_gate gate;
};

static char *suffixed(const char *name, const char *suffix)
{
	return goipc__join(name, suffix, "");
}

static int event_for(const char *name, const char *suffix, bool create, goipc_event **out)
{
	char *n = suffixed(name, suffix);
	if (n == NULL)
		return GOIPC_ENOMEM;
	int rc = create ? goipc_event_create(n, out) : goipc_event_open(n, out);
	free(n);
	return rc;
}

static void unlink_events(const char *name)
{
	char *ne = suffixed(name, GOIPC_NOT_EMPTY_SUFFIX);
	char *nf = suffixed(name, GOIPC_NOT_FULL_SUFFIX);
	if (ne != NULL)
		goipc__event_unlink_name(ne);
	if (nf != NULL)
		goipc__event_unlink_name(nf);
	free(ne);
	free(nf);
}

/* unwind releases what a failed constructor acquired. */
static void unwind(goipc_queue *q)
{
	goipc_event_destroy(q->ne);
	goipc_event_destroy(q->nf);
	if (q->map != NULL)
		munmap(q->map, q->map_len);
	if (q->fd >= 0)
		close(q->fd);
	if (q->owner) {
		unlink_events(q->name);
		char *path = goipc__segment_path(q->name);
		if (path != NULL)
			unlink(path);
		free(path);
	}
	goipc__gate_destroy(&q->gate);
	free(q->name);
	free(q);
}

static int new_queue(const char *name, bool owner, goipc_queue **out)
{
	int rc = goipc__validate_name(name);
	if (rc != GOIPC_OK)
		return rc;
	goipc_queue *q = calloc(1, sizeof *q);
	if (q == NULL)
		return GOIPC_ENOMEM;
	q->fd = -1;
	q->owner = owner;
	q->name = strdup(name);
	if (q->name == NULL) {
		free(q);
		return GOIPC_ENOMEM;
	}
	rc = goipc__gate_init(&q->gate);
	if (rc != GOIPC_OK) {
		free(q->name);
		free(q);
		return rc;
	}
	*out = q;
	return GOIPC_OK;
}

static int map_segment(goipc_queue *q, int flags, size_t size)
{
	char *path = goipc__segment_path(q->name);
	if (path == NULL)
		return GOIPC_ENOMEM;
	q->fd = open(path, flags, 0600);
	free(path);
	if (q->fd < 0)
		return goipc__sys();
	if (flags & O_CREAT) {
		if (ftruncate(q->fd, (off_t)size) != 0)
			return goipc__sys();
	} else {
		struct stat st;
		if (fstat(q->fd, &st) != 0)
			return goipc__sys();
		size = (size_t)st.st_size;
		if (size < GOIPC_HEADER_SIZE + GOIPC_MIN_CAPACITY)
			return GOIPC_ETOOSMALL;
	}
	void *m = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, q->fd, 0);
	if (m == MAP_FAILED)
		return goipc__sys();
	q->map = m;
	q->map_len = size;
	return GOIPC_OK;
}

int goipc_queue_create(const char *name, size_t capacity, goipc_queue **out)
{
	if (capacity == 0)
		capacity = GOIPC_DEFAULT_CAPACITY;
	if (capacity < GOIPC_MIN_CAPACITY || (capacity & (capacity - 1)) != 0)
		return GOIPC_EINVALCAP;
	goipc_queue *q;
	int rc = new_queue(name, true, &q);
	if (rc != GOIPC_OK)
		return rc;

	/* Events first and the segment last, so a peer that finds the segment
	 * also finds both events. */
	if ((rc = event_for(name, GOIPC_NOT_EMPTY_SUFFIX, true, &q->ne)) != GOIPC_OK ||
	    (rc = event_for(name, GOIPC_NOT_FULL_SUFFIX, true, &q->nf)) != GOIPC_OK ||
	    (rc = map_segment(q, O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC, goipc_ring_size(capacity))) != GOIPC_OK ||
	    (rc = goipc_ring_init(&q->ring, q->map, q->map_len)) != GOIPC_OK) {
		unwind(q);
		return rc;
	}
	*out = q;
	return GOIPC_OK;
}

int goipc_queue_open(const char *name, goipc_queue **out)
{
	goipc_queue *q;
	int rc = new_queue(name, false, &q);
	if (rc != GOIPC_OK)
		return rc;
	if ((rc = map_segment(q, O_RDWR | O_CLOEXEC, 0)) != GOIPC_OK ||
	    (rc = goipc_ring_attach(&q->ring, q->map, q->map_len)) != GOIPC_OK ||
	    (rc = event_for(name, GOIPC_NOT_EMPTY_SUFFIX, false, &q->ne)) != GOIPC_OK ||
	    (rc = event_for(name, GOIPC_NOT_FULL_SUFFIX, false, &q->nf)) != GOIPC_OK) {
		unwind(q);
		return rc;
	}
	*out = q;
	return GOIPC_OK;
}

const char *goipc_queue_name(const goipc_queue *q)
{
	return q->name;
}

size_t goipc_queue_capacity(const goipc_queue *q)
{
	return goipc_ring_capacity(&q->ring);
}

size_t goipc_queue_max_message_size(const goipc_queue *q)
{
	return goipc_ring_max_message_size(&q->ring);
}

goipc_ring *goipc_queue_ring(goipc_queue *q)
{
	return &q->ring;
}

/* A signal that races a close of this queue finds the event closed; the
 * close releases every waiter anyway. */
static int signal_event(goipc_event *e, int n)
{
	int rc = goipc_event_signal(e, n);
	return rc == GOIPC_ECLOSED ? GOIPC_OK : rc;
}

static int wake_receiver(goipc_queue *q)
{
	if (atomic_load(&goipc__hdr(&q->ring)->recv_waiters) > 0)
		return signal_event(q->ne, 1);
	return GOIPC_OK;
}

/* Freed space fits an unknown number of senders, so every one wakes and
 * re-checks its own size. */
static int wake_senders(goipc_queue *q)
{
	int32_t w = atomic_load(&goipc__hdr(&q->ring)->send_waiters);
	if (w > 0)
		return signal_event(q->nf, w);
	return GOIPC_OK;
}

typedef int (*attempt_fn)(goipc_queue *q, void *arg);

/* The waiter count is published before the second attempt and read by a peer
 * after it publishes its change. Both are seq_cst, so no wakeup is lost. */
static int park(goipc_queue *q, goipc_event *ev, _Atomic int32_t *waiters, int blocked, attempt_fn attempt, void *arg, const struct timespec *deadline)
{
	for (;;) {
		int rc = attempt(q, arg);
		if (rc != blocked)
			return rc;
		atomic_fetch_add(waiters, 1);
		rc = attempt(q, arg);
		if (rc != blocked) {
			atomic_fetch_sub(waiters, 1);
			return rc;
		}
		rc = goipc__event_wait_until(ev, deadline);
		atomic_fetch_sub(waiters, 1);
		if (rc != GOIPC_OK)
			return rc;
	}
}

struct send_arg {
	uint32_t type;
	const void *payload;
	size_t len;
};

static int attempt_send(goipc_queue *q, void *arg)
{
	struct send_arg *a = arg;
	return goipc_ring_try_write(&q->ring, a->type, a->payload, a->len);
}

struct claim_arg {
	uint32_t type;
	size_t len;
	goipc_claim *out;
};

static int attempt_claim(goipc_queue *q, void *arg)
{
	struct claim_arg *a = arg;
	return goipc_ring_try_claim(&q->ring, a->type, a->len, a->out);
}

/* receive runs one read and wakes senders when head moved. Padding alone
 * moves head, and a sender parked behind an aborted claim needs that wake. */
static int receive(goipc_queue *q, int limit, goipc_read_fn fn, void *ctx, size_t maxlen, size_t *need, uint32_t *need_type)
{
	_Atomic uint64_t *head = &goipc__hdr(&q->ring)->head;
	uint64_t before = atomic_load(head);
	int n = goipc__ring_read(&q->ring, limit, fn, ctx, maxlen, need, need_type);
	if (atomic_load(head) != before) {
		int rc = wake_senders(q);
		if (rc != GOIPC_OK)
			return rc;
	}
	if (n < 0)
		return n;
	return n == 0 ? GOIPC_EEMPTY : n;
}

struct recv_arg {
	void *dst;
	size_t cap;
	uint32_t *type;
	size_t *len;
};

static void copy_out(void *ctx, uint32_t type, const uint8_t *payload, size_t len)
{
	struct recv_arg *a = ctx;
	if (len > 0)
		memcpy(a->dst, payload, len);
	*a->type = type;
	*a->len = len;
}

static int attempt_recv(goipc_queue *q, void *arg)
{
	struct recv_arg *a = arg;
	int rc = receive(q, 1, copy_out, a, a->cap, a->len, a->type);
	return rc > 0 ? GOIPC_OK : rc;
}

struct batch_arg {
	int limit;
	goipc_read_fn fn;
	void *ctx;
	int count;
};

static int attempt_batch(goipc_queue *q, void *arg)
{
	struct batch_arg *a = arg;
	size_t need;
	uint32_t type;
	int rc = receive(q, a->limit, a->fn, a->ctx, SIZE_MAX, &need, &type);
	if (rc > 0) {
		a->count = rc;
		return GOIPC_OK;
	}
	return rc;
}

int goipc_queue_try_send(goipc_queue *q, uint32_t type, const void *payload, size_t len)
{
	if (!goipc__gate_enter(&q->gate))
		return GOIPC_ECLOSED;
	int rc = goipc_ring_try_write(&q->ring, type, payload, len);
	if (rc == GOIPC_OK)
		rc = wake_receiver(q);
	goipc__gate_leave(&q->gate);
	return rc;
}

int goipc__queue_send_until(goipc_queue *q, uint32_t type, const void *payload, size_t len, const struct timespec *deadline)
{
	if (!goipc__gate_enter(&q->gate))
		return GOIPC_ECLOSED;
	struct send_arg a = {type, payload, len};
	int rc = park(q, q->nf, &goipc__hdr(&q->ring)->send_waiters, GOIPC_EFULL, attempt_send, &a, deadline);
	if (rc == GOIPC_OK)
		rc = wake_receiver(q);
	goipc__gate_leave(&q->gate);
	return rc;
}

int goipc_queue_send(goipc_queue *q, uint32_t type, const void *payload, size_t len, int64_t timeout_ns)
{
	struct timespec ts;
	return goipc__queue_send_until(q, type, payload, len, goipc__deadline(timeout_ns, &ts));
}

int goipc_queue_claim(goipc_queue *q, uint32_t type, size_t len, int64_t timeout_ns, goipc_claim *out)
{
	if (!goipc__gate_enter(&q->gate))
		return GOIPC_ECLOSED;
	struct timespec ts;
	struct claim_arg a = {type, len, out};
	int rc = park(q, q->nf, &goipc__hdr(&q->ring)->send_waiters, GOIPC_EFULL, attempt_claim, &a, goipc__deadline(timeout_ns, &ts));
	goipc__gate_leave(&q->gate);
	return rc;
}

void goipc_queue_commit(goipc_queue *q, goipc_claim *c)
{
	if (!goipc__gate_enter(&q->gate))
		return;
	goipc_claim_commit(c);
	wake_receiver(q);
	goipc__gate_leave(&q->gate);
}

/* An abort frees nothing until the receiver steps over the padding, so it
 * wakes the receiver like a commit does. */
void goipc_queue_abort(goipc_queue *q, goipc_claim *c)
{
	if (!goipc__gate_enter(&q->gate))
		return;
	goipc_claim_abort(c);
	wake_receiver(q);
	goipc__gate_leave(&q->gate);
}

int goipc_queue_try_recv(goipc_queue *q, void *dst, size_t cap, uint32_t *type, size_t *len)
{
	if (!goipc__gate_enter(&q->gate))
		return GOIPC_ECLOSED;
	struct recv_arg a = {dst, cap, type, len};
	int rc = attempt_recv(q, &a);
	goipc__gate_leave(&q->gate);
	return rc;
}

int goipc__queue_recv_until(goipc_queue *q, void *dst, size_t cap, uint32_t *type, size_t *len, const struct timespec *deadline)
{
	if (!goipc__gate_enter(&q->gate))
		return GOIPC_ECLOSED;
	struct recv_arg a = {dst, cap, type, len};
	int rc = park(q, q->ne, &goipc__hdr(&q->ring)->recv_waiters, GOIPC_EEMPTY, attempt_recv, &a, deadline);
	goipc__gate_leave(&q->gate);
	return rc;
}

int goipc_queue_recv(goipc_queue *q, void *dst, size_t cap, uint32_t *type, size_t *len, int64_t timeout_ns)
{
	struct timespec ts;
	return goipc__queue_recv_until(q, dst, cap, type, len, goipc__deadline(timeout_ns, &ts));
}

int goipc_queue_read_batch(goipc_queue *q, int limit, goipc_read_fn fn, void *ctx, int64_t timeout_ns)
{
	if (limit <= 0)
		return GOIPC_EINVAL;
	if (!goipc__gate_enter(&q->gate))
		return GOIPC_ECLOSED;
	struct timespec ts;
	struct batch_arg a = {limit, fn, ctx, 0};
	int rc = park(q, q->ne, &goipc__hdr(&q->ring)->recv_waiters, GOIPC_EEMPTY, attempt_batch, &a, goipc__deadline(timeout_ns, &ts));
	goipc__gate_leave(&q->gate);
	return rc == GOIPC_OK ? a.count : rc;
}

int goipc_queue_close(goipc_queue *q)
{
	if (!goipc__gate_close(&q->gate))
		return GOIPC_ECLOSED;
	/* Closing the events releases every parked waiter, which lets the
	 * in-flight count fall to zero. */
	int rc = goipc_event_close(q->ne);
	int rc2 = goipc_event_close(q->nf);
	if (rc == GOIPC_OK)
		rc = rc2;
	goipc__gate_drain(&q->gate);
	if (munmap(q->map, q->map_len) != 0 && rc == GOIPC_OK)
		rc = goipc__sys();
	q->map = NULL;
	if (close(q->fd) != 0 && rc == GOIPC_OK)
		rc = goipc__sys();
	q->fd = -1;
	return rc;
}

int goipc_queue_unlink(goipc_queue *q)
{
	int rc = GOIPC_OK;
	char *path = goipc__segment_path(q->name);
	if (path == NULL)
		return GOIPC_ENOMEM;
	if (unlink(path) != 0)
		rc = goipc__sys();
	free(path);
	char *ne = suffixed(q->name, GOIPC_NOT_EMPTY_SUFFIX);
	char *nf = suffixed(q->name, GOIPC_NOT_FULL_SUFFIX);
	if (ne == NULL || nf == NULL) {
		free(ne);
		free(nf);
		return GOIPC_ENOMEM;
	}
	int r2 = goipc__event_unlink_name(ne);
	int r3 = goipc__event_unlink_name(nf);
	free(ne);
	free(nf);
	if (rc == GOIPC_OK)
		rc = r2;
	if (rc == GOIPC_OK)
		rc = r3;
	return rc;
}

void goipc_queue_destroy(goipc_queue *q)
{
	if (q == NULL)
		return;
	if (!goipc__gate_closed(&q->gate))
		goipc_queue_close(q);
	goipc_event_destroy(q->ne);
	goipc_event_destroy(q->nf);
	goipc__gate_destroy(&q->gate);
	free(q->name);
	free(q);
}
