#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "internal.h"

/* A producer watch follows a process whose claim stops the receiver. */
struct producer {
	uint64_t id;
	uint64_t key;
	atomic_bool fired;
	goipc_queue *q;
	struct producer *next;
};

struct goipc_queue {
	char *name;
	char inc[GOIPC_INC_LEN + 1];
	int fd;
	void *map;
	size_t map_len;
	goipc_ring ring;
	goipc_event *ne;
	goipc_event *nf;
	uint64_t self;
	/* lock_fd holds the name. Only the creator has one. */
	int lock_fd;
	/* reader reports whether this handle owns the receiving end. */
	bool reader;
	pthread_mutex_t recv_mu;

	pthread_mutex_t slot_mu;
	int idle[GOIPC_CLAIM_SLOTS];
	int nidle;
	int owned[GOIPC_CLAIM_SLOTS];
	int nowned;

	/* The watch on the receiver, for a handle that sends. */
	pthread_mutex_t peer_mu;
	_Atomic uint64_t peer_id;
	atomic_bool peer_gone;
	atomic_int peer_failed;
	bool peer_watching;
	uint64_t peer_key;

	pthread_mutex_t prod_mu;
	struct producer *producers;
	bool prod_closed;
	atomic_int prod_failed;

	/* peer_tx is the outbound queue of a channel. Its receiver is the peer. */
	goipc_queue *peer_tx;
	/* gone_rx is the inbound queue of a channel, woken when the peer goes. */
	goipc_queue *gone_rx;

	/* An operation in flight holds a pointer into the mapping, so close waits for it rather than unmap under it. */
	struct goipc_gate gate;
};

static void free_queue(goipc_queue *q)
{
	pthread_mutex_destroy(&q->recv_mu);
	pthread_mutex_destroy(&q->slot_mu);
	pthread_mutex_destroy(&q->peer_mu);
	pthread_mutex_destroy(&q->prod_mu);
	goipc__gate_destroy(&q->gate);
	free(q->name);
	free(q);
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
	if (q->lock_fd >= 0) {
		if (q->inc[0] != '\0')
			goipc__remove_instance(q->name, q->inc);
		goipc__release_name(q->lock_fd);
	}
	free_queue(q);
}

static int new_queue(const char *name, goipc_queue **out)
{
	goipc_queue *q = calloc(1, sizeof *q);
	if (q == NULL)
		return GOIPC_ENOMEM;
	q->fd = -1;
	q->lock_fd = -1;
	q->name = strdup(name);
	if (q->name == NULL) {
		free(q);
		return GOIPC_ENOMEM;
	}
	int rc = goipc__gate_init(&q->gate);
	if (rc != GOIPC_OK) {
		free(q->name);
		free(q);
		return rc;
	}
	pthread_mutex_init(&q->recv_mu, NULL);
	pthread_mutex_init(&q->slot_mu, NULL);
	pthread_mutex_init(&q->peer_mu, NULL);
	pthread_mutex_init(&q->prod_mu, NULL);
	q->self = goipc__self_id();
	*out = q;
	return GOIPC_OK;
}

static int event_for(const char *inst, const char *suffix, bool create, goipc_event **out)
{
	char *n = goipc__join(inst, suffix, "");
	if (n == NULL)
		return GOIPC_ENOMEM;
	int rc = create ? goipc_event_create(n, out) : goipc_event_open(n, out);
	free(n);
	return rc;
}

static int map_segment(goipc_queue *q, const char *inst, int flags, size_t size)
{
	char *path = goipc__segment_path(inst);
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

/* build makes a new instance while this handle holds the name. The .inc file
 * names the instance only after the instance is complete. */
static int build(goipc_queue *q, size_t capacity, uint64_t consumer)
{
	char old[GOIPC_INC_LEN + 1];
	goipc__previous_inc(q->name, old);
	int rc;
	if (old[0] != '\0' && (rc = goipc__remove_instance(q->name, old)) != GOIPC_OK)
		return rc;
	if ((rc = goipc__new_inc(q->inc)) != GOIPC_OK)
		return rc;
	char *inst = goipc__instance_name(q->name, q->inc);
	if (inst == NULL)
		return GOIPC_ENOMEM;
	if ((rc = event_for(inst, GOIPC_NOT_EMPTY_SUFFIX, true, &q->ne)) == GOIPC_OK &&
	    (rc = event_for(inst, GOIPC_NOT_FULL_SUFFIX, true, &q->nf)) == GOIPC_OK &&
	    (rc = map_segment(q, inst, O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC, goipc_ring_size(capacity))) == GOIPC_OK &&
	    (rc = goipc__ring_init(&q->ring, q->map, q->map_len, consumer)) == GOIPC_OK)
		rc = goipc__publish_inc(q->name, q->inc);
	free(inst);
	return rc;
}

int goipc__queue_create(const char *name, size_t capacity, bool pending, goipc_queue **out)
{
	int rc = goipc__validate_name(name);
	if (rc != GOIPC_OK)
		return rc;
	if (capacity == 0)
		capacity = GOIPC_DEFAULT_CAPACITY;
	if (capacity < GOIPC_MIN_CAPACITY || (capacity & (capacity - 1)) != 0)
		return GOIPC_EINVALCAP;
	goipc__sweep();

	int lock_fd;
	if ((rc = goipc__lock_name(name, &lock_fd)) != GOIPC_OK)
		return rc;
	goipc_queue *q;
	if ((rc = new_queue(name, &q)) != GOIPC_OK) {
		goipc__release_name(lock_fd);
		return rc;
	}
	q->lock_fd = lock_fd;
	q->reader = !pending;
	if ((rc = build(q, capacity, pending ? GOIPC_PROC_PENDING : q->self)) != GOIPC_OK) {
		unwind(q);
		return rc;
	}
	*out = q;
	return GOIPC_OK;
}

int goipc_queue_create(const char *name, size_t capacity, goipc_queue **out)
{
	return goipc__queue_create(name, capacity, false, out);
}

int goipc__queue_open(const char *name, goipc_queue **out)
{
	int rc = goipc__validate_name(name);
	if (rc != GOIPC_OK)
		return rc;
	goipc_queue *q;
	if ((rc = new_queue(name, &q)) != GOIPC_OK)
		return rc;
	if ((rc = goipc__read_name(name, q->inc)) != GOIPC_OK) {
		unwind(q);
		return rc;
	}
	char *inst = goipc__instance_name(name, q->inc);
	if (inst == NULL) {
		unwind(q);
		return GOIPC_ENOMEM;
	}
	if ((rc = map_segment(q, inst, O_RDWR | O_CLOEXEC, 0)) != GOIPC_OK ||
	    (rc = goipc_ring_attach(&q->ring, q->map, q->map_len)) != GOIPC_OK ||
	    (rc = event_for(inst, GOIPC_NOT_EMPTY_SUFFIX, false, &q->ne)) != GOIPC_OK ||
	    (rc = event_for(inst, GOIPC_NOT_FULL_SUFFIX, false, &q->nf)) != GOIPC_OK) {
		free(inst);
		unwind(q);
		return rc;
	}
	free(inst);
	*out = q;
	return GOIPC_OK;
}

static int check_peer(goipc_queue *q);

int goipc_queue_open(const char *name, goipc_queue **out)
{
	goipc_queue *q;
	int rc = goipc__queue_open(name, &q);
	if (rc != GOIPC_OK)
		return rc;
	if ((rc = check_peer(q)) != GOIPC_OK) {
		goipc_queue_destroy(q);
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

/* ---- the receiver, as a sender sees it ---- */

static int peer_state(goipc_queue *q)
{
	if (!atomic_load(&q->peer_gone))
		return GOIPC_OK;
	int failed = atomic_load(&q->peer_failed);
	return failed != 0 ? goipc__sys_errno(failed) : GOIPC_EPEERGONE;
}

/* peer_exited marks the receiver gone and wakes everything in this process
 * that waits on it. */
static void peer_exited(void *arg, uint64_t id, int err)
{
	goipc_queue *q = arg;
	if (!goipc__gate_enter(&q->gate))
		return;
	if (atomic_load(&q->peer_id) == id) {
		if (err != 0)
			atomic_store(&q->peer_failed, err);
		atomic_store(&q->peer_gone, true);
		wake_senders(q);
		if (q->gone_rx != NULL)
			signal_event(q->gone_rx->ne, 1);
	}
	goipc__gate_leave(&q->gate);
}

static int watch_peer(goipc_queue *q, uint64_t id)
{
	pthread_mutex_lock(&q->peer_mu);
	int rc;
	if (atomic_load(&q->peer_id) == id) {
		rc = peer_state(q);
		goto out;
	}
	if (q->peer_watching) {
		goipc__cancel_exit(q->peer_key);
		q->peer_watching = false;
	}
	atomic_store(&q->peer_failed, 0);
	/* A receiver that is already dead is reported now. */
	bool dead = goipc__is_dead(id);
	atomic_store(&q->peer_gone, dead);
	atomic_store(&q->peer_id, id);
	if (dead) {
		rc = GOIPC_EPEERGONE;
		goto out;
	}
	rc = goipc__on_exit(id, peer_exited, q, &q->peer_key);
	if (rc == GOIPC_OK)
		q->peer_watching = true;
	else if (rc == GOIPC_EPEERGONE)
		atomic_store(&q->peer_gone, true);
	else
		atomic_store(&q->peer_id, 0);
out:
	pthread_mutex_unlock(&q->peer_mu);
	return rc;
}

static void peer_stop(goipc_queue *q)
{
	pthread_mutex_lock(&q->peer_mu);
	if (q->peer_watching)
		goipc__cancel_exit(q->peer_key);
	q->peer_watching = false;
	pthread_mutex_unlock(&q->peer_mu);
}

/* check_peer reports GOIPC_EPEERGONE once the receiver has closed or exited.
 * The first check of a new receiver starts a watch on its process. Every later
 * check is an atomic load. */
static int check_peer(goipc_queue *q)
{
	uint64_t id = atomic_load(&goipc__hdr(&q->ring)->consumer);
	if (id == GOIPC_PROC_NONE)
		return GOIPC_EPEERGONE;
	if (id == GOIPC_PROC_PENDING || id == q->self)
		return GOIPC_OK;
	/* A receiver with no life socket cannot be watched. Its close still
	 * reaches this sender through the consumer field. */
	if ((id & GOIPC_PROC_WATCHABLE) == 0)
		return GOIPC_OK;
	if (atomic_load(&q->peer_id) == id)
		return peer_state(q);
	return watch_peer(q, id);
}

static int saw_peer_gone(goipc_queue *q, int rc)
{
	if (rc == GOIPC_EPEERGONE)
		atomic_store(&q->peer_gone, true);
	return rc;
}

/* ---- producers whose claims stop the receiver ---- */

static void producer_exited(void *arg, uint64_t id, int err)
{
	struct producer *p = arg;
	goipc_queue *q = p->q;
	(void)id;
	if (!goipc__gate_enter(&q->gate))
		return;
	atomic_store(&p->fired, true);
	if (err != 0) {
		int expect = 0;
		atomic_compare_exchange_strong(&q->prod_failed, &expect, err);
	}
	signal_event(q->ne, 1);
	goipc__gate_leave(&q->gate);
}

static int watch_producer(goipc_queue *q, uint64_t id)
{
	pthread_mutex_lock(&q->prod_mu);
	int rc = GOIPC_OK;
	if (q->prod_closed)
		goto out;
	/* A kernel wait that failed is reported rather than watched again. */
	int failed = atomic_load(&q->prod_failed);
	if (failed != 0) {
		rc = goipc__sys_errno(failed);
		goto out;
	}
	for (struct producer **pp = &q->producers; *pp != NULL; pp = &(*pp)->next) {
		struct producer *p = *pp;
		if (p->id != id)
			continue;
		if (!atomic_load(&p->fired))
			goto out;
		*pp = p->next;
		free(p);
		break;
	}
	struct producer *p = calloc(1, sizeof *p);
	if (p == NULL) {
		rc = GOIPC_ENOMEM;
		goto out;
	}
	p->id = id;
	p->q = q;
	rc = goipc__on_exit(id, producer_exited, p, &p->key);
	if (rc != GOIPC_OK) {
		free(p);
		/* The producer exited since the check. The wake makes the
		 * receiver look again and reclaim. */
		if (rc == GOIPC_EPEERGONE) {
			signal_event(q->ne, 1);
			rc = GOIPC_OK;
		}
		goto out;
	}
	p->next = q->producers;
	q->producers = p;
out:
	pthread_mutex_unlock(&q->prod_mu);
	return rc;
}

static void producers_stop(goipc_queue *q)
{
	pthread_mutex_lock(&q->prod_mu);
	q->prod_closed = true;
	struct producer *p = q->producers;
	q->producers = NULL;
	while (p != NULL) {
		struct producer *next = p->next;
		goipc__cancel_exit(p->key);
		free(p);
		p = next;
	}
	pthread_mutex_unlock(&q->prod_mu);
}

/* unstall looks at the claim that stops the receiver, if one does. It sets
 * *reclaimed after it turns the claim of a dead producer into padding. A
 * claim whose producer lives gets a watch on that producer instead. */
static int unstall(goipc_queue *q, bool *reclaimed)
{
	*reclaimed = false;
	struct goipc_stall st;
	if (!goipc__ring_stalled(&q->ring, &st))
		return GOIPC_OK;
	struct goipc_ring_hdr *h = goipc__hdr(&q->ring);
	int dead[GOIPC_CLAIM_SLOTS];
	int ndead = 0;
	uint64_t end = 0;
	for (int i = 0; i < st.nslots; i++) {
		int idx = st.slots[i];
		uint64_t owner = atomic_load(&h->slots[idx].owner);
		uint64_t at = atomic_load(&h->slots[idx].at);
		uint64_t size = atomic_load(&h->slots[idx].size);
		/* Only a slot that still covers the stall counts. */
		if (owner == GOIPC_PROC_NONE || at == GOIPC_NO_INTENT || at > st.at || st.at >= at + size)
			continue;
		/* A claim of this process, or of a process with no life socket, is left alone. */
		if (owner == q->self || (owner & GOIPC_PROC_WATCHABLE) == 0)
			return GOIPC_OK;
		if (!goipc__is_dead(owner))
			return watch_producer(q, owner);
		/* Dead producers that claim different ranges here leave no way to
		 * tell which claim is real. */
		if (ndead > 0 && at + size != end)
			return GOIPC_ECORRUPT;
		end = at + size;
		dead[ndead++] = idx;
	}
	if (ndead > 0)
		*reclaimed = goipc__ring_reclaim(&q->ring, &st, end, dead, ndead);
	return GOIPC_OK;
}

/* ---- claim slots ---- */

struct dead_memo {
	uint64_t ids[16];
	bool dead[16];
	int n;
};

static bool memo_dead(void *ctx, uint64_t id)
{
	struct dead_memo *m = ctx;
	for (int i = 0; i < m->n; i++)
		if (m->ids[i] == id)
			return m->dead[i];
	bool dead = goipc__is_dead(id);
	if (m->n < 16) {
		m->ids[m->n] = id;
		m->dead[m->n++] = dead;
	}
	return dead;
}

/* take_slot hands out a claim slot this handle owns, and takes a new one from
 * the ring when none is idle. */
static int take_slot(goipc_queue *q, int *slot)
{
	pthread_mutex_lock(&q->slot_mu);
	if (q->nidle > 0) {
		*slot = q->idle[--q->nidle];
		pthread_mutex_unlock(&q->slot_mu);
		return GOIPC_OK;
	}
	pthread_mutex_unlock(&q->slot_mu);

	struct dead_memo memo = {.n = 0};
	int rc = goipc__ring_acquire_slot(&q->ring, q->self, memo_dead, &memo, slot);
	if (rc != GOIPC_OK)
		return rc;
	pthread_mutex_lock(&q->slot_mu);
	q->owned[q->nowned++] = *slot;
	pthread_mutex_unlock(&q->slot_mu);
	return GOIPC_OK;
}

static void put_slot(goipc_queue *q, int slot)
{
	if (slot < 0)
		return;
	pthread_mutex_lock(&q->slot_mu);
	q->idle[q->nidle++] = slot;
	pthread_mutex_unlock(&q->slot_mu);
}

/* queue_write is one attempt to copy payload in under a claim slot of this process. */
static int queue_write(goipc_queue *q, uint32_t type, const void *payload, size_t len)
{
	int rc = check_peer(q);
	if (rc != GOIPC_OK)
		return rc;
	int slot;
	if ((rc = take_slot(q, &slot)) != GOIPC_OK)
		return rc;
	goipc_claim c;
	rc = goipc__ring_try_claim(&q->ring, slot, type, len, &c);
	if (rc == GOIPC_OK) {
		if (len > 0)
			memcpy(c.bytes, payload, len);
		goipc_claim_commit(&c);
	}
	put_slot(q, slot);
	return rc;
}

/* queue_claim is one attempt to reserve len bytes. The slot stays with the
 * claim until its commit or abort. */
static int queue_claim(goipc_queue *q, uint32_t type, size_t len, goipc_claim *out)
{
	int rc = check_peer(q);
	if (rc != GOIPC_OK)
		return rc;
	int slot;
	if ((rc = take_slot(q, &slot)) != GOIPC_OK)
		return rc;
	rc = goipc__ring_try_claim(&q->ring, slot, type, len, out);
	if (rc != GOIPC_OK)
		put_slot(q, slot);
	return rc;
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
	return queue_write(q, a->type, a->payload, a->len);
}

struct claim_arg {
	uint32_t type;
	size_t len;
	goipc_claim *out;
};

static int attempt_claim(goipc_queue *q, void *arg)
{
	struct claim_arg *a = arg;
	return queue_claim(q, a->type, a->len, a->out);
}

/* receive runs a read and wakes senders when head moved. Padding alone moves
 * head, and a sender parked behind an aborted claim needs that wake. A read
 * that finds a claim in its way checks the claim's producer. A dead
 * producer's claim becomes padding, and the read runs again. A channel
 * reports a gone peer only after the read finds nothing. */
static int receive(goipc_queue *q, int limit, goipc_read_fn fn, void *ctx, size_t maxlen, size_t *need, uint32_t *need_type)
{
	int gone = GOIPC_OK;
	if (q->peer_tx != NULL && goipc__gate_enter(&q->peer_tx->gate)) {
		gone = check_peer(q->peer_tx);
		goipc__gate_leave(&q->peer_tx->gate);
	}
	_Atomic uint64_t *head = &goipc__hdr(&q->ring)->head;
	for (;;) {
		uint64_t before = atomic_load(head);
		int n = goipc__ring_read(&q->ring, limit, fn, ctx, maxlen, need, need_type);
		if (atomic_load(head) != before) {
			int rc = wake_senders(q);
			if (rc != GOIPC_OK)
				return rc;
		}
		if (n != 0)
			return n;
		bool reclaimed;
		int rc = unstall(q, &reclaimed);
		if (rc != GOIPC_OK)
			return rc;
		if (!reclaimed)
			break;
	}
	return gone != GOIPC_OK ? gone : GOIPC_EEMPTY;
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
	int rc = queue_write(q, type, payload, len);
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
	else
		saw_peer_gone(q, rc);
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
	saw_peer_gone(q, rc);
	goipc__gate_leave(&q->gate);
	return rc;
}

int goipc_queue_commit(goipc_queue *q, goipc_claim *c)
{
	if (!goipc__gate_enter(&q->gate))
		return GOIPC_ECLOSED;
	goipc_claim_commit(c);
	put_slot(q, c->slot);
	int rc = wake_receiver(q);
	goipc__gate_leave(&q->gate);
	return rc;
}

/* An abort frees nothing until the receiver steps over the padding, so it
 * wakes the receiver like a commit does. */
int goipc_queue_abort(goipc_queue *q, goipc_claim *c)
{
	if (!goipc__gate_enter(&q->gate))
		return GOIPC_ECLOSED;
	goipc_claim_abort(c);
	put_slot(q, c->slot);
	int rc = wake_receiver(q);
	goipc__gate_leave(&q->gate);
	return rc;
}

/* enter_reader admits a receive. Only the handle that owns the receiving end
 * may receive, and its receives run one at a time. */
static int enter_reader(goipc_queue *q)
{
	if (!goipc__gate_enter(&q->gate))
		return GOIPC_ECLOSED;
	if (!q->reader) {
		goipc__gate_leave(&q->gate);
		return GOIPC_ENOTCONSUMER;
	}
	pthread_mutex_lock(&q->recv_mu);
	return GOIPC_OK;
}

static void leave_reader(goipc_queue *q)
{
	pthread_mutex_unlock(&q->recv_mu);
	goipc__gate_leave(&q->gate);
}

int goipc_queue_try_recv(goipc_queue *q, void *dst, size_t cap, uint32_t *type, size_t *len)
{
	int rc = enter_reader(q);
	if (rc != GOIPC_OK)
		return rc;
	struct recv_arg a = {dst, cap, type, len};
	rc = attempt_recv(q, &a);
	leave_reader(q);
	return rc;
}

int goipc__queue_recv_until(goipc_queue *q, void *dst, size_t cap, uint32_t *type, size_t *len, const struct timespec *deadline)
{
	int rc = enter_reader(q);
	if (rc != GOIPC_OK)
		return rc;
	struct recv_arg a = {dst, cap, type, len};
	rc = park(q, q->ne, &goipc__hdr(&q->ring)->recv_waiters, GOIPC_EEMPTY, attempt_recv, &a, deadline);
	leave_reader(q);
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
	int rc = enter_reader(q);
	if (rc != GOIPC_OK)
		return rc;
	struct timespec ts;
	struct batch_arg a = {limit, fn, ctx, 0};
	rc = park(q, q->ne, &goipc__hdr(&q->ring)->recv_waiters, GOIPC_EEMPTY, attempt_batch, &a, goipc__deadline(timeout_ns, &ts));
	leave_reader(q);
	return rc == GOIPC_OK ? a.count : rc;
}

int goipc_queue_close(goipc_queue *q)
{
	if (!goipc__gate_close(&q->gate))
		return GOIPC_ECLOSED;
	/* The receiving end goes first, so a sender parked on a full queue wakes
	 * and finds nobody left to drain it. */
	if (q->reader) {
		uint64_t self = q->self;
		atomic_compare_exchange_strong(&goipc__hdr(&q->ring)->consumer, &self, GOIPC_PROC_NONE);
		wake_senders(q);
	}
	peer_stop(q);
	producers_stop(q);

	/* Closing the events releases every parked waiter, which lets the in-flight count fall to zero. */
	int rc = goipc_event_close(q->ne);
	int rc2 = goipc_event_close(q->nf);
	if (rc == GOIPC_OK)
		rc = rc2;
	goipc__gate_drain(&q->gate);

	pthread_mutex_lock(&q->slot_mu);
	for (int i = 0; i < q->nowned; i++)
		goipc__ring_drop_slot(&q->ring, q->self, q->owned[i]);
	q->nowned = q->nidle = 0;
	pthread_mutex_unlock(&q->slot_mu);

	if (munmap(q->map, q->map_len) != 0 && rc == GOIPC_OK)
		rc = goipc__sys();
	q->map = NULL;
	if (close(q->fd) != 0 && rc == GOIPC_OK)
		rc = goipc__sys();
	q->fd = -1;
	if (q->lock_fd >= 0) {
		goipc__release_name(q->lock_fd);
		q->lock_fd = -1;
	}
	return rc;
}

int goipc_queue_unlink(goipc_queue *q)
{
	int rc = goipc__remove_instance(q->name, q->inc);
	int rc2 = goipc__unlink_name(q->name, q->inc);
	return rc != GOIPC_OK ? rc : rc2;
}

void goipc_queue_destroy(goipc_queue *q)
{
	if (q == NULL)
		return;
	if (!goipc__gate_closed(&q->gate))
		goipc_queue_close(q);
	goipc_event_destroy(q->ne);
	goipc_event_destroy(q->nf);
	free_queue(q);
}

/* ---- channel support ---- */

void goipc__channel_link(goipc_queue *tx, goipc_queue *rx)
{
	rx->peer_tx = tx;
	tx->gone_rx = rx;
}

/* goipc__channel_connect makes this process the reader of rx, by a swap of
 * the consumer from pending to its procID. */
int goipc__channel_connect(goipc_queue *rx)
{
	uint64_t expect = GOIPC_PROC_PENDING;
	if (!atomic_compare_exchange_strong(&goipc__hdr(&rx->ring)->consumer, &expect, rx->self))
		return GOIPC_EINUSE;
	rx->reader = true;
	return wake_senders(rx);
}

int goipc__queue_wake_receiver(goipc_queue *q)
{
	if (!goipc__gate_enter(&q->gate))
		return GOIPC_ECLOSED;
	int rc = signal_event(q->ne, 1);
	goipc__gate_leave(&q->gate);
	return rc;
}
