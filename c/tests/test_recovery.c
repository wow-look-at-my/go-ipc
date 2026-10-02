/* The v2 protocol: the header layout, claim slots, recovery of a dead
 * producer's claim, peer checks, name locks and the sweep. */
#define _GNU_SOURCE
#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "harness.h"
#include "internal.h"

static struct goipc_ring_hdr *hdr_of(goipc_queue *q)
{
	return goipc__hdr(goipc_queue_ring(q));
}

static goipc_queue *receiver(char *name, size_t len, size_t capacity)
{
	unique_name(name, len, "rq");
	goipc_queue *q;
	REQUIRE_RC(goipc_queue_create(name, capacity, &q), GOIPC_OK);
	return q;
}

static void drop(goipc_queue *q)
{
	goipc_queue_close(q);
	goipc_queue_unlink(q);
	goipc_queue_destroy(q);
}

static void pause_ms(int ms)
{
	struct timespec ts = {ms / 1000, (long)(ms % 1000) * 1000000};
	nanosleep(&ts, NULL);
}

/* dead_id is a watchable procID with no life socket behind it. */
static uint64_t dead_id(void)
{
	return GOIPC_PROC_WATCHABLE | GOIPC_PROC_ANY | ((uint64_t)getpid() << 16) | 0xDEAD;
}

/* strand leaves a claim of size bytes at the tail, owned by owner. With
 * header set, the claim has its header written, as a producer that dies
 * before its commit leaves it. */
static void strand(goipc_queue *q, uint64_t owner, int slot, uint64_t size, bool header)
{
	goipc_ring *r = goipc_queue_ring(q);
	struct goipc_ring_hdr *h = goipc__hdr(r);
	uint64_t tail = atomic_load(&h->tail);
	atomic_store(&h->slots[slot].owner, owner);
	atomic_store(&h->slots[slot].size, size);
	atomic_store(&h->slots[slot].at, tail);
	atomic_store(&h->tail, tail + size);
	if (header) {
		uint64_t idx = tail & r->mask;
		uint32_t type = 5;
		int32_t len = -(int32_t)size;
		memcpy(r->data + idx + 4, &type, 4);
		atomic_store((_Atomic int32_t *)(void *)(r->data + idx), len);
	}
}

static void expect_msg(goipc_queue *q, const char *want, int64_t timeout)
{
	char buf[64];
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_queue_recv(q, buf, sizeof buf, &type, &len, timeout), GOIPC_OK);
	REQUIRE(len == strlen(want) && memcmp(buf, want, len) == 0, "got %.*s, want %s", (int)len, buf, want);
}

/* ---- layout ---- */

TEST(v2_header_layout, 0)
{
	CHECK(GOIPC_RING_VERSION == 2);
	CHECK(GOIPC_HEADER_SIZE == 512 + 256 * 64);
	CHECK(offsetof(struct goipc_ring_hdr, consumer) == 24);
	CHECK(offsetof(struct goipc_ring_hdr, slots) == 512);
	CHECK(offsetof(struct goipc_ring_hdr, slots[1]) == 512 + 64);
	CHECK(offsetof(struct goipc_claim_slot, owner) == 0);
	CHECK(offsetof(struct goipc_claim_slot, at) == 8);
	CHECK(offsetof(struct goipc_claim_slot, size) == 16);
	CHECK(offsetof(goipc_claim, slot) == 20 && sizeof(goipc_claim) == 40, "the slot must sit in the old padding");

	size_t len = goipc_ring_size(4096);
	uint8_t *buf = aligned_buffer(len);
	memset(buf, 0xAB, len);
	goipc_ring r;
	REQUIRE_RC(goipc_ring_init(&r, buf, len), GOIPC_OK);
	struct goipc_ring_hdr *h = goipc__hdr(&r);
	CHECK(atomic_load(&h->consumer) == 0);
	for (size_t i = 0; i < GOIPC_CLAIM_SLOTS; i++) {
		CHECK(atomic_load(&h->slots[i].owner) == 0);
		CHECK(atomic_load(&h->slots[i].at) == UINT64_MAX);
		CHECK(atomic_load(&h->slots[i].size) == 0);
	}
	uint64_t magic;
	memcpy(&magic, buf, 8);
	CHECK(magic == GOIPC_RING_MAGIC);
	free(buf);
}

TEST(created_queue_names_its_reader, 0)
{
	char name[96];
	goipc_queue *q = receiver(name, sizeof name, 4096);
	uint64_t self = goipc__self_id();
	CHECK(self & GOIPC_PROC_WATCHABLE);
	CHECK(self & GOIPC_PROC_ANY);
	CHECK(atomic_load(&hdr_of(q)->consumer) == self);

	/* Close clears the consumer, which a second handle still maps. */
	goipc_queue *s;
	REQUIRE_RC(goipc_queue_open(name, &s), GOIPC_OK);
	REQUIRE_RC(goipc_queue_close(q), GOIPC_OK);
	CHECK(atomic_load(&hdr_of(s)->consumer) == GOIPC_PROC_NONE);
	goipc_queue_destroy(s);
	REQUIRE_RC(goipc_queue_unlink(q), GOIPC_OK);
	goipc_queue_destroy(q);
}

TEST(self_is_alive, 0)
{
	CHECK(goipc__self_errno() == 0, "errno %d", goipc__self_errno());
	uint64_t self = goipc__self_id();
	CHECK(!goipc__is_dead(self));
	CHECK(goipc__is_dead(dead_id()));
	/* A process with no life socket is never judged. */
	CHECK(!goipc__is_dead(dead_id() & ~GOIPC_PROC_WATCHABLE));
	char *path = goipc__life_path(self);
	CHECK(path_exists(path), "%s", path);
	free(path);
}

/* ---- claim slots ---- */

TEST(queue_claim_records_its_intent, 0)
{
	char name[96];
	goipc_queue *q = receiver(name, sizeof name, 4096);
	struct goipc_ring_hdr *h = hdr_of(q);
	uint64_t tail = atomic_load(&h->tail);
	goipc_claim c;
	REQUIRE_RC(goipc_queue_claim(q, 3, 10, -1, &c), GOIPC_OK);
	REQUIRE(c.slot >= 0 && c.slot < (int)GOIPC_CLAIM_SLOTS, "slot %d", c.slot);
	struct goipc_claim_slot *s = &h->slots[c.slot];
	CHECK(atomic_load(&s->owner) == goipc__self_id());
	CHECK(atomic_load(&s->at) == tail);
	CHECK(atomic_load(&s->size) == 24);
	/* The claim stores the type, then the negative length. */
	int32_t len;
	uint32_t type;
	memcpy(&len, goipc_queue_ring(q)->data + c.index, 4);
	memcpy(&type, goipc_queue_ring(q)->data + c.index + 4, 4);
	CHECK(len == -18 && type == 3, "len %d type %u", len, type);

	memcpy(c.bytes, "0123456789", 10);
	REQUIRE_RC(goipc_queue_commit(q, &c), GOIPC_OK);
	CHECK(atomic_load(&s->at) == UINT64_MAX);
	CHECK(atomic_load(&s->owner) == goipc__self_id(), "the owner keeps its slot");

	/* A raw ring claim names nobody. */
	REQUIRE_RC(goipc_ring_try_claim(goipc_queue_ring(q), 1, 1, &c), GOIPC_OK);
	CHECK(c.slot == -1);
	goipc_claim_abort(&c);

	/* A send that finds the ring full clears its intent. */
	static uint8_t big[2000];
	while (goipc_queue_try_send(q, 1, big, sizeof big) == GOIPC_OK)
		;
	for (size_t i = 0; i < GOIPC_CLAIM_SLOTS; i++)
		CHECK(atomic_load(&h->slots[i].at) == UINT64_MAX, "slot %zu keeps an intent", i);

	/* Close returns the slots to the shared pool. */
	goipc_queue *other;
	REQUIRE_RC(goipc_queue_open(name, &other), GOIPC_OK);
	REQUIRE_RC(goipc_queue_close(q), GOIPC_OK);
	for (size_t i = 0; i < GOIPC_CLAIM_SLOTS; i++)
		CHECK(atomic_load(&hdr_of(other)->slots[i].owner) == 0, "slot %zu kept its owner", i);
	goipc_queue_destroy(other);
	drop(q);
}

TEST(claim_slots_run_out, 0)
{
	char name[96];
	goipc_queue *q = receiver(name, sizeof name, 1 << 16);
	static goipc_claim claims[GOIPC_CLAIM_SLOTS];
	for (size_t i = 0; i < GOIPC_CLAIM_SLOTS; i++)
		REQUIRE_RC(goipc_queue_claim(q, 0, 0, -1, &claims[i]), GOIPC_OK);
	goipc_claim extra;
	REQUIRE_RC(goipc_queue_claim(q, 0, 0, -1, &extra), GOIPC_ETOOMANYCLAIMS);
	REQUIRE_RC(goipc_queue_try_send(q, 0, "x", 1), GOIPC_ETOOMANYCLAIMS);
	for (size_t i = 0; i < GOIPC_CLAIM_SLOTS; i++)
		REQUIRE_RC(goipc_queue_abort(q, &claims[i]), GOIPC_OK);
	REQUIRE_RC(goipc_queue_try_send(q, 0, "after", 5), GOIPC_OK);
	expect_msg(q, "after", 0);
	drop(q);
}

TEST(slots_of_dead_producers_are_reused, 0)
{
	char name[96];
	goipc_queue *q = receiver(name, sizeof name, 4096);
	for (size_t i = 0; i < GOIPC_CLAIM_SLOTS; i++)
		atomic_store(&hdr_of(q)->slots[i].owner, dead_id());
	REQUIRE_RC(goipc_queue_try_send(q, 0, "reused", 6), GOIPC_OK);
	expect_msg(q, "reused", 0);
	drop(q);
}

/* A process with no life socket cannot be checked, so its claims and its
 * slots are never judged. */
TEST(unwatchable_is_never_judged, 0)
{
	char name[96];
	goipc_queue *q = receiver(name, sizeof name, 4096);
	uint64_t dead = dead_id() & ~GOIPC_PROC_WATCHABLE;
	strand(q, dead, 1, 16, false);
	for (size_t i = 2; i < GOIPC_CLAIM_SLOTS; i++)
		atomic_store(&hdr_of(q)->slots[i].owner, dead);
	uint8_t buf[8];
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_queue_try_recv(q, buf, sizeof buf, &type, &len), GOIPC_EEMPTY);
	CHECK(atomic_load(&hdr_of(q)->slots[1].owner) == dead);
	goipc_claim held, extra;
	REQUIRE_RC(goipc_queue_claim(q, 0, 0, 0, &held), GOIPC_OK);
	CHECK(held.slot == 0);
	REQUIRE_RC(goipc_queue_claim(q, 0, 0, 0, &extra), GOIPC_ETOOMANYCLAIMS);
	goipc_queue_abort(q, &held);
	drop(q);
}

/* ---- recovery of dead claims ---- */

TEST(receiver_reclaims_bare_claim_of_dead_producer, 0)
{
	char name[96];
	goipc_queue *q = receiver(name, sizeof name, 4096);
	strand(q, dead_id(), 7, 16, false);
	REQUIRE_RC(goipc_queue_try_send(q, 0, "after", 5), GOIPC_OK);
	expect_msg(q, "after", MS(5000));
	CHECK(atomic_load(&hdr_of(q)->slots[7].at) == UINT64_MAX);
	CHECK(goipc_ring_empty(goipc_queue_ring(q)));
	drop(q);
}

TEST(receiver_reclaims_written_claim_of_dead_producer, 0)
{
	char name[96];
	goipc_queue *q = receiver(name, sizeof name, 4096);
	strand(q, dead_id(), 3, 24, true);
	REQUIRE_RC(goipc_queue_try_send(q, 0, "after", 5), GOIPC_OK);
	char buf[16];
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_queue_try_recv(q, buf, sizeof buf, &type, &len), GOIPC_OK);
	CHECK(type == 0 && len == 5 && memcmp(buf, "after", 5) == 0);
	drop(q);
}

TEST(receiver_reclaims_dead_claim_across_wrap, 0)
{
	char name[96];
	goipc_queue *q = receiver(name, sizeof name, 4096);
	static uint8_t big[2040];
	size_t sizes[] = {2040, 2024};
	for (int i = 0; i < 2; i++) {
		REQUIRE_RC(goipc_queue_try_send(q, 0, big, sizes[i]), GOIPC_OK);
		uint32_t type;
		size_t len;
		REQUIRE_RC(goipc_queue_recv(q, big, sizeof big, &type, &len, 0), GOIPC_OK);
	}
	REQUIRE(atomic_load(&hdr_of(q)->tail) == 4096 - 16);
	strand(q, dead_id(), 7, 48, false);
	REQUIRE_RC(goipc_queue_try_send(q, 0, "after", 5), GOIPC_OK);
	expect_msg(q, "after", MS(5000));
	CHECK(atomic_load(&hdr_of(q)->slots[7].at) == UINT64_MAX);
	drop(q);
}

TEST(dead_producers_that_disagree_are_corrupt, 0)
{
	char name[96];
	goipc_queue *q = receiver(name, sizeof name, 4096);
	uint64_t dead = dead_id();
	strand(q, dead, 1, 16, false);
	struct goipc_ring_hdr *h = hdr_of(q);
	atomic_store(&h->slots[2].owner, dead);
	atomic_store(&h->slots[2].size, 32);
	atomic_store(&h->slots[2].at, atomic_load(&h->slots[1].at));
	uint8_t buf[8];
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_queue_try_recv(q, buf, sizeof buf, &type, &len), GOIPC_ECORRUPT);
	drop(q);
}

struct recv_op {
	goipc_queue *q;
	int64_t timeout;
	int rc;
	atomic_bool done;
	char buf[64];
	size_t len;
};

static void *recv_thread(void *arg)
{
	struct recv_op *o = arg;
	uint32_t type;
	o->rc = goipc_queue_recv(o->q, o->buf, sizeof o->buf, &type, &o->len, o->timeout);
	atomic_store(&o->done, true);
	return NULL;
}

/* A child process claims and dies without a commit. The receiver must wait
 * while the child lives, wake when it dies, pad its claim and read on. */
TEST(receiver_recovers_claim_of_producer_that_died, T_FORK)
{
	char name[96];
	goipc_queue *q = receiver(name, sizeof name, 4096);
	int ready[2], hold[2];
	REQUIRE(pipe(ready) == 0 && pipe(hold) == 0);
	pid_t pid = fork();
	REQUIRE(pid >= 0);
	if (pid == 0) {
		close(ready[0]);
		close(hold[1]);
		goipc_queue *cq;
		goipc_claim c;
		if (goipc_queue_open(name, &cq) != GOIPC_OK || goipc_queue_claim(cq, 9, 100, 0, &c) != GOIPC_OK)
			_exit(2);
		if (write(ready[1], "c", 1) != 1)
			_exit(3);
		char b;
		if (read(hold[0], &b, 1) < 0)
			_exit(4);
		_exit(0);
	}
	close(ready[1]);
	close(hold[0]);
	char b;
	REQUIRE(read(ready[0], &b, 1) == 1);
	REQUIRE_RC(goipc_queue_try_send(q, 0, "after", 5), GOIPC_OK);

	/* The producer lives, so its claim stops the receiver. */
	uint8_t buf[64];
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_queue_try_recv(q, buf, sizeof buf, &type, &len), GOIPC_EEMPTY);

	struct recv_op o = {.q = q, .timeout = MS(10000), .rc = 99};
	pthread_t th;
	REQUIRE(pthread_create(&th, NULL, recv_thread, &o) == 0);
	pause_ms(100);
	CHECK(!atomic_load(&o.done), "receive passed a live producer's claim");

	close(hold[1]);
	int status;
	REQUIRE(waitpid(pid, &status, 0) == pid);
	CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0, "child status %d", status);
	int64_t start = now_ns();
	pthread_join(th, NULL);
	REQUIRE_RC(o.rc, GOIPC_OK);
	CHECK(o.len == 5 && memcmp(o.buf, "after", 5) == 0);
	CHECK(now_ns() - start < MS(5000), "the receiver woke late");
	CHECK(goipc_ring_empty(goipc_queue_ring(q)));
	close(ready[0]);
	drop(q);
}

/* ---- peers ---- */

TEST(sender_sees_closed_receiver, 0)
{
	char name[96];
	goipc_queue *q = receiver(name, sizeof name, 4096);
	goipc_queue *s;
	REQUIRE_RC(goipc_queue_open(name, &s), GOIPC_OK);
	REQUIRE_RC(goipc_queue_try_send(s, 0, "x", 1), GOIPC_OK);
	REQUIRE_RC(goipc_queue_close(q), GOIPC_OK);
	CHECK_RC(goipc_queue_try_send(s, 0, "x", 1), GOIPC_EPEERGONE);
	CHECK_RC(goipc_queue_send(s, 0, "x", 1, MS(100)), GOIPC_EPEERGONE);
	goipc_claim c;
	CHECK_RC(goipc_queue_claim(s, 0, 1, MS(100), &c), GOIPC_EPEERGONE);
	goipc_queue *again;
	CHECK_RC(goipc_queue_open(name, &again), GOIPC_EPEERGONE);
	goipc_queue_destroy(s);
	goipc_queue_unlink(q);
	goipc_queue_destroy(q);
}

TEST(open_tells_not_found_from_not_ready, 0)
{
	char name[96];
	unique_name(name, sizeof name, "nr");
	goipc_queue *q;
	REQUIRE_RC(goipc_queue_open(name, &q), GOIPC_ESYS);
	CHECK(goipc_last_errno() == ENOENT, "errno %d", goipc_last_errno());
	/* A name file with no instance id is a create in progress. */
	char *path = goipc__name_path(name);
	FILE *f = fopen(path, "w");
	REQUIRE(f != NULL);
	fclose(f);
	REQUIRE_RC(goipc_queue_open(name, &q), GOIPC_ESYS);
	CHECK(goipc_last_errno() == EAGAIN, "errno %d", goipc_last_errno());
	unlink(path);
	free(path);
}

TEST(open_handle_cannot_receive, 0)
{
	char name[96];
	goipc_queue *q = receiver(name, sizeof name, 4096);
	goipc_queue *s;
	REQUIRE_RC(goipc_queue_open(name, &s), GOIPC_OK);
	uint8_t buf[8];
	uint32_t type;
	size_t len;
	CHECK_RC(goipc_queue_try_recv(s, buf, sizeof buf, &type, &len), GOIPC_ENOTCONSUMER);
	CHECK_RC(goipc_queue_recv(s, buf, sizeof buf, &type, &len, MS(10)), GOIPC_ENOTCONSUMER);
	CHECK_RC(goipc_queue_read_batch(s, 1, NULL, NULL, MS(10)), GOIPC_ENOTCONSUMER);
	goipc_queue_destroy(s);
	drop(q);
}

struct send_op {
	goipc_queue *q;
	int rc;
	atomic_bool done;
};

static void *send_thread(void *arg)
{
	struct send_op *o = arg;
	static const uint8_t payload[512];
	o->rc = goipc_queue_send(o->q, 0, payload, sizeof payload, MS(10000));
	atomic_store(&o->done, true);
	return NULL;
}

/* child_hold runs fn in a child that then waits for hold to close. It
 * returns once fn has written its line to the ready pipe. */
static pid_t child_hold(void (*fn)(const char *name, int out), const char *name, int *hold_w, char *line, size_t cap)
{
	int ready[2], hold[2];
	REQUIRE(pipe(ready) == 0 && pipe(hold) == 0);
	pid_t pid = fork();
	REQUIRE(pid >= 0);
	if (pid == 0) {
		close(ready[0]);
		close(hold[1]);
		fn(name, ready[1]);
		char b;
		if (read(hold[0], &b, 1) < 0)
			_exit(4);
		_exit(0);
	}
	close(ready[1]);
	close(hold[0]);
	size_t have = 0;
	while (have + 1 < cap) {
		ssize_t n = read(ready[0], line + have, 1);
		if (n <= 0 || line[have] == '\n')
			break;
		have++;
	}
	line[have] = '\0';
	close(ready[0]);
	*hold_w = hold[1];
	return pid;
}

static void say(int fd, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void say(int fd, const char *fmt, ...)
{
	char buf[128];
	va_list ap;
	va_start(ap, fmt);
	int n = vsnprintf(buf, sizeof buf, fmt, ap);
	va_end(ap);
	if (write(fd, buf, (size_t)n) != n)
		_exit(5);
}

static void create_and_report(const char *name, int out)
{
	goipc_queue *q;
	int rc = goipc_queue_create(name, 4096, &q);
	if (rc != GOIPC_OK) {
		say(out, "err %d\n", rc);
		return;
	}
	char inc[GOIPC_INC_LEN + 1];
	goipc__read_name(name, inc);
	say(out, "%s %016llx\n", inc, (unsigned long long)goipc__self_id());
}

static void release_child(pid_t pid, int hold_w)
{
	close(hold_w);
	int status;
	REQUIRE(waitpid(pid, &status, 0) == pid);
	CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0, "child status %d", status);
}

TEST(parked_sender_sees_receiver_exit, T_FORK)
{
	char name[96], line[64];
	unique_name(name, sizeof name, "dead-rx");
	int hold;
	pid_t pid = child_hold(create_and_report, name, &hold, line, sizeof line);
	REQUIRE(strncmp(line, "err", 3) != 0, "child: %s", line);

	goipc_queue *s;
	REQUIRE_RC(goipc_queue_open(name, &s), GOIPC_OK);
	static const uint8_t payload[512];
	while (goipc_queue_try_send(s, 0, payload, sizeof payload) == GOIPC_OK)
		;
	struct send_op o = {.q = s, .rc = 99};
	pthread_t th;
	REQUIRE(pthread_create(&th, NULL, send_thread, &o) == 0);
	pause_ms(100);
	CHECK(!atomic_load(&o.done), "send returned on a full queue");
	CHECK(atomic_load(&hdr_of(s)->send_waiters) == 1);

	int64_t start = now_ns();
	release_child(pid, hold);
	pthread_join(th, NULL);
	CHECK_RC(o.rc, GOIPC_EPEERGONE);
	CHECK(now_ns() - start < MS(5000), "the sender woke late");
	CHECK_RC(goipc_queue_try_send(s, 0, payload, sizeof payload), GOIPC_EPEERGONE);
	goipc_queue_destroy(s);

	/* The name outlives its holder, and names a receiver that is gone. */
	goipc_queue *again;
	CHECK_RC(goipc_queue_open(name, &again), GOIPC_EPEERGONE);
	goipc_queue *fresh;
	REQUIRE_RC(goipc_queue_create(name, 4096, &fresh), GOIPC_OK);
	drop(fresh);
}

TEST(second_creator_gets_einuse, T_FORK)
{
	char name[96], line[64];
	unique_name(name, sizeof name, "inuse");
	goipc_queue *a, *b;
	REQUIRE_RC(goipc_queue_create(name, 4096, &a), GOIPC_OK);
	CHECK_RC(goipc_queue_create(name, 4096, &b), GOIPC_EINUSE);
	REQUIRE_RC(goipc_queue_close(a), GOIPC_OK);
	REQUIRE_RC(goipc_queue_create(name, 4096, &b), GOIPC_OK);
	drop(b);
	goipc_queue_destroy(a);

	/* The same holds across processes, and the kernel drops the lock at exit. */
	int hold;
	pid_t pid = child_hold(create_and_report, name, &hold, line, sizeof line);
	REQUIRE(strncmp(line, "err", 3) != 0, "child: %s", line);
	char old[GOIPC_INC_LEN + 1];
	memcpy(old, line, GOIPC_INC_LEN);
	old[GOIPC_INC_LEN] = '\0';
	CHECK_RC(goipc_queue_create(name, 4096, &b), GOIPC_EINUSE);
	release_child(pid, hold);
	REQUIRE_RC(goipc_queue_create(name, 4096, &b), GOIPC_OK);
	char inc[GOIPC_INC_LEN + 1];
	REQUIRE_RC(goipc__read_name(name, inc), GOIPC_OK);
	CHECK(strcmp(inc, old) != 0, "the new creator reused instance %s", inc);
	char *seg_old_name = goipc__instance_name(name, old);
	char *seg_old = goipc__segment_path(seg_old_name);
	CHECK(!path_exists(seg_old), "the old instance %s survived", seg_old);
	free(seg_old);
	free(seg_old_name);
	REQUIRE_RC(goipc_queue_try_send(b, 0, "fresh", 5), GOIPC_OK);
	expect_msg(b, "fresh", 0);
	drop(b);
}

TEST(sweep_removes_name_of_dead_creator, T_FORK)
{
	char name[96], line[64];
	unique_name(name, sizeof name, "sweep");
	int hold;
	pid_t pid = child_hold(create_and_report, name, &hold, line, sizeof line);
	char inc[GOIPC_INC_LEN + 1];
	unsigned long long child_id;
	REQUIRE(sscanf(line, "%16s %llx", inc, &child_id) == 2, "child: %s", line);
	char *inst = goipc__instance_name(name, inc);
	char *paths[6] = {goipc__name_path(name), goipc__inc_path(name), goipc__segment_path(inst), NULL, NULL, goipc__life_path(child_id)};
	char *ne = goipc__join(inst, GOIPC_NOT_EMPTY_SUFFIX, "");
	char *nf = goipc__join(inst, GOIPC_NOT_FULL_SUFFIX, "");
	paths[3] = goipc__event_path(ne);
	paths[4] = goipc__event_path(nf);
	for (int i = 0; i < 6; i++)
		REQUIRE(path_exists(paths[i]), "%s is missing", paths[i]);

	/* A live holder keeps everything. */
	goipc__sweep_dir(goipc_runtime_dir());
	for (int i = 0; i < 6; i++)
		CHECK(path_exists(paths[i]), "the sweep removed %s of a live process", paths[i]);

	release_child(pid, hold);
	goipc__sweep_dir(goipc_runtime_dir());
	for (int i = 0; i < 6; i++) {
		CHECK(!path_exists(paths[i]), "the sweep left %s", paths[i]);
		free(paths[i]);
	}
	char *own = goipc__life_path(goipc__self_id());
	CHECK(path_exists(own), "the sweep removed the life socket of this process");
	free(own);
	free(ne);
	free(nf);
	free(inst);
}

TEST(fork_child_has_its_own_identity, T_FORK)
{
	uint64_t self = goipc__self_id();
	int p[2];
	REQUIRE(pipe(p) == 0);
	pid_t pid = fork();
	REQUIRE(pid >= 0);
	if (pid == 0) {
		uint64_t id = goipc__self_id();
		_exit(write(p[1], &id, sizeof id) == sizeof id ? 0 : 1);
	}
	close(p[1]);
	uint64_t child = 0;
	REQUIRE(read(p[0], &child, sizeof child) == sizeof child);
	close(p[0]);
	int status;
	REQUIRE(waitpid(pid, &status, 0) == pid);
	CHECK(child != self && (child & GOIPC_PROC_WATCHABLE), "child id %016llx", (unsigned long long)child);
	CHECK(goipc__is_dead(child), "the child's life socket outlived it");
	CHECK(!goipc__is_dead(self), "the child took this process's life socket with it");
	char *path = goipc__life_path(child);
	unlink(path);
	free(path);
}

/* ---- watches, driven by a fake process ---- */

/* fake_process listens on a life socket for a made-up procID. Closing the
 * returned fd ends the fake process: watches see their connection end, and
 * checks find a socket that refuses. */
static int fake_process(uint64_t *id)
{
	static atomic_int seq;
	*id = GOIPC_PROC_WATCHABLE | GOIPC_PROC_ANY | ((uint64_t)getpid() << 20) | (uint64_t)atomic_fetch_add(&seq, 1);
	char *path = goipc__life_path(*id);
	struct sockaddr_un addr = {.sun_family = AF_UNIX};
	REQUIRE(strlen(path) < sizeof addr.sun_path);
	strcpy(addr.sun_path, path);
	free(path);
	int fd = goipc__socket(false);
	REQUIRE(fd >= 0);
	REQUIRE(bind(fd, (struct sockaddr *)&addr, sizeof addr) == 0 && listen(fd, 16) == 0);
	return fd;
}

static void end_fake(uint64_t id, int fd)
{
	(void)id;
	close(fd);
}

static void forget_fake(uint64_t id)
{
	char *path = goipc__life_path(id);
	unlink(path);
	free(path);
}

static void note_exit(void *arg, uint64_t id, int err)
{
	int *fds = arg;
	(void)id;
	uint8_t b = err == 0 ? 'x' : 'e';
	if (write(fds[1], &b, 1) != 1)
		abort();
}

TEST(on_exit_reports_exit_and_cancel_stops_it, T_THREADS)
{
	uint64_t id;
	int fd = fake_process(&id);
	CHECK(!goipc__is_dead(id));
	int first[2], second[2];
	REQUIRE(pipe(first) == 0 && pipe(second) == 0);
	uint64_t k1, k2;
	REQUIRE_RC(goipc__on_exit(id, note_exit, first, &k1), GOIPC_OK);
	REQUIRE_RC(goipc__on_exit(id, note_exit, second, &k2), GOIPC_OK);
	goipc__cancel_exit(k2);

	end_fake(id, fd);
	uint8_t b = 0;
	REQUIRE(read(first[0], &b, 1) == 1);
	CHECK(b == 'x', "the watch reported a failure");
	CHECK(goipc__is_dead(id));
	REQUIRE(fcntl(second[0], F_SETFL, O_NONBLOCK) == 0);
	CHECK(read(second[0], &b, 1) < 0 && errno == EAGAIN, "a cancelled watch fired");

	/* A gone process is reported at once, and nothing is registered. */
	uint64_t k3;
	CHECK_RC(goipc__on_exit(id, note_exit, first, &k3), GOIPC_EPEERGONE);
	forget_fake(id);
	for (int i = 0; i < 2; i++) {
		close(first[i]);
		close(second[i]);
	}
}

/* A sender parked on a full queue wakes with peer-gone when the receiver's
 * process ends. */
TEST(parked_sender_wakes_when_receiver_process_ends, T_THREADS)
{
	char name[96];
	goipc_queue *q = receiver(name, sizeof name, 4096);
	uint64_t id;
	int fd = fake_process(&id);
	atomic_store(&hdr_of(q)->consumer, id);

	goipc_queue *s;
	REQUIRE_RC(goipc_queue_open(name, &s), GOIPC_OK);
	static const uint8_t payload[512];
	while (goipc_queue_try_send(s, 0, payload, sizeof payload) == GOIPC_OK)
		;
	struct send_op o = {.q = s, .rc = 99};
	pthread_t th;
	REQUIRE(pthread_create(&th, NULL, send_thread, &o) == 0);
	pause_ms(50);
	CHECK(!atomic_load(&o.done), "send returned on a full queue");

	end_fake(id, fd);
	pthread_join(th, NULL);
	CHECK_RC(o.rc, GOIPC_EPEERGONE);
	CHECK_RC(goipc_queue_try_send(s, 0, payload, sizeof payload), GOIPC_EPEERGONE);
	goipc_queue_destroy(s);
	forget_fake(id);
	atomic_store(&hdr_of(q)->consumer, goipc__self_id());
	drop(q);
}

/* A receiver parked behind the claim of a live producer wakes when that
 * producer's process ends, and reclaims the claim. */
TEST(parked_receiver_wakes_when_producer_process_ends, T_THREADS)
{
	char name[96];
	goipc_queue *q = receiver(name, sizeof name, 4096);
	uint64_t id;
	int fd = fake_process(&id);
	strand(q, id, 5, 32, true);
	REQUIRE_RC(goipc_queue_try_send(q, 0, "after", 5), GOIPC_OK);

	struct recv_op o = {.q = q, .timeout = MS(10000), .rc = 99};
	pthread_t th;
	REQUIRE(pthread_create(&th, NULL, recv_thread, &o) == 0);
	pause_ms(50);
	CHECK(!atomic_load(&o.done), "receive passed a live producer's claim");

	end_fake(id, fd);
	pthread_join(th, NULL);
	REQUIRE_RC(o.rc, GOIPC_OK);
	CHECK(o.len == 5 && memcmp(o.buf, "after", 5) == 0);
	forget_fake(id);
	drop(q);
}

/* ---- channels ---- */

TEST(channel_rejects_second_peer, 0)
{
	char name[96];
	unique_name(name, sizeof name, "ch2");
	goipc_channel *server, *first, *second;
	REQUIRE_RC(goipc_channel_create(name, 4096, &server), GOIPC_OK);
	CHECK(atomic_load(&goipc__hdr(goipc_queue_ring(goipc_channel_tx(server)))->consumer) == GOIPC_PROC_PENDING);
	REQUIRE_RC(goipc_channel_open(name, &first), GOIPC_OK);
	CHECK(atomic_load(&goipc__hdr(goipc_queue_ring(goipc_channel_tx(server)))->consumer) == goipc__self_id());
	CHECK_RC(goipc_channel_open(name, &second), GOIPC_EINUSE);
	goipc_channel_destroy(first);
	goipc_channel_close(server);
	goipc_channel_unlink(server);
	goipc_channel_destroy(server);
}

TEST(channel_sends_before_peer_connects, 0)
{
	char name[96];
	unique_name(name, sizeof name, "chp");
	goipc_channel *server, *client;
	REQUIRE_RC(goipc_channel_create(name, 4096, &server), GOIPC_OK);
	REQUIRE_RC(goipc_channel_send(server, 1, "early", 5, 0), GOIPC_OK);
	REQUIRE_RC(goipc_channel_open(name, &client), GOIPC_OK);
	char buf[16];
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_channel_recv(client, buf, sizeof buf, &type, &len, MS(1000)), GOIPC_OK);
	CHECK(len == 5 && memcmp(buf, "early", 5) == 0);
	goipc_channel_destroy(client);
	goipc_channel_close(server);
	goipc_channel_unlink(server);
	goipc_channel_destroy(server);
}

TEST(channel_recv_reports_peer_close, 0)
{
	char name[96];
	unique_name(name, sizeof name, "chc");
	goipc_channel *server, *client;
	REQUIRE_RC(goipc_channel_create(name, 4096, &server), GOIPC_OK);
	REQUIRE_RC(goipc_channel_open(name, &client), GOIPC_OK);
	REQUIRE_RC(goipc_channel_send(client, 0, "last", 4, -1), GOIPC_OK);
	REQUIRE_RC(goipc_channel_close(client), GOIPC_OK);
	goipc_channel_destroy(client);

	char buf[16];
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_channel_recv(server, buf, sizeof buf, &type, &len, MS(1000)), GOIPC_OK);
	CHECK(len == 4 && memcmp(buf, "last", 4) == 0);
	CHECK_RC(goipc_channel_recv(server, buf, sizeof buf, &type, &len, MS(1000)), GOIPC_EPEERGONE);
	CHECK_RC(goipc_channel_send(server, 0, "x", 1, MS(1000)), GOIPC_EPEERGONE);
	goipc_channel_close(server);
	goipc_channel_unlink(server);
	goipc_channel_destroy(server);
}

struct chan_recv {
	goipc_channel *c;
	int rc;
};

static void *chan_recv_thread(void *arg)
{
	struct chan_recv *o = arg;
	char buf[16];
	uint32_t type;
	size_t len;
	o->rc = goipc_channel_recv(o->c, buf, sizeof buf, &type, &len, MS(10000));
	return NULL;
}

TEST(channel_recv_wakes_on_peer_close, T_THREADS)
{
	char name[96];
	unique_name(name, sizeof name, "chw");
	goipc_channel *server, *client;
	REQUIRE_RC(goipc_channel_create(name, 4096, &server), GOIPC_OK);
	REQUIRE_RC(goipc_channel_open(name, &client), GOIPC_OK);
	struct chan_recv o = {client, 99};
	pthread_t th;
	REQUIRE(pthread_create(&th, NULL, chan_recv_thread, &o) == 0);
	pause_ms(50);
	REQUIRE_RC(goipc_channel_close(server), GOIPC_OK);
	pthread_join(th, NULL);
	CHECK_RC(o.rc, GOIPC_EPEERGONE);
	goipc_channel_destroy(client);
	goipc_channel_unlink(server);
	goipc_channel_destroy(server);
}

static void open_channel_and_send(const char *name, int out)
{
	goipc_channel *c;
	int rc = goipc_channel_open(name, &c);
	if (rc == GOIPC_OK)
		rc = goipc_channel_send(c, 0, "hello", 5, MS(5000));
	say(out, "%d\n", rc);
}

TEST(channel_recv_reports_peer_exit, T_FORK)
{
	char name[96], line[32];
	unique_name(name, sizeof name, "chx");
	goipc_channel *server;
	REQUIRE_RC(goipc_channel_create(name, 4096, &server), GOIPC_OK);
	int hold;
	pid_t pid = child_hold(open_channel_and_send, name, &hold, line, sizeof line);
	REQUIRE(strcmp(line, "0") == 0, "child: %s", line);

	char buf[16];
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_channel_recv(server, buf, sizeof buf, &type, &len, MS(1000)), GOIPC_OK);
	CHECK(len == 5 && memcmp(buf, "hello", 5) == 0);

	struct chan_recv o = {server, 99};
	pthread_t th;
	REQUIRE(pthread_create(&th, NULL, chan_recv_thread, &o) == 0);
	pause_ms(50);
	release_child(pid, hold);
	pthread_join(th, NULL);
	CHECK_RC(o.rc, GOIPC_EPEERGONE);
	CHECK_RC(goipc_channel_send(server, 0, "x", 1, MS(1000)), GOIPC_EPEERGONE);
	goipc_channel_close(server);
	goipc_channel_unlink(server);
	goipc_channel_destroy(server);
}
