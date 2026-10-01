#define _GNU_SOURCE
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "harness.h"
#include "internal.h"

static goipc_queue *new_queue(char *name, size_t len, size_t capacity)
{
	unique_name(name, len, "q");
	goipc_queue *q;
	REQUIRE_RC(goipc_queue_create(name, capacity, &q), GOIPC_OK);
	return q;
}

static void drop_queue(goipc_queue *q)
{
	goipc_queue_close(q);
	goipc_queue_unlink(q);
	goipc_queue_destroy(q);
}

static void pause_ms(int ms)
{
	struct timespec ts = {0, (long)ms * 1000000};
	nanosleep(&ts, NULL);
}

static int fill(goipc_queue *q, size_t len)
{
	static uint8_t p[4096];
	int n = 0;
	while (goipc_queue_try_send(q, 1, p, len) == GOIPC_OK)
		n++;
	return n;
}

TEST(queue_create_lays_out_three_files, 0)
{
	char name[96];
	goipc_queue *q = new_queue(name, sizeof name, 8192);
	REQUIRE(strcmp(goipc_queue_name(q), name) == 0);
	REQUIRE(goipc_queue_capacity(q) == 8192);
	REQUIRE(goipc_queue_max_message_size(q) == 4088);
	REQUIRE(goipc_ring_capacity(goipc_queue_ring(q)) == 8192);

	char *seg = goipc__segment_path(name);
	char ne[128], nf[128];
	snprintf(ne, sizeof ne, "/dev/shm/go-ipc-%s.ne.event", name);
	snprintf(nf, sizeof nf, "/dev/shm/go-ipc-%s.nf.event", name);
	struct stat st;
	REQUIRE(stat(seg, &st) == 0);
	CHECK(S_ISREG(st.st_mode) && st.st_size == 512 + 8192, "segment size %lld", (long long)st.st_size);
	CHECK((st.st_mode & 0077) == 0, "mode %o", st.st_mode & 0777);
	REQUIRE(stat(ne, &st) == 0);
	CHECK(S_ISFIFO(st.st_mode));
	REQUIRE(stat(nf, &st) == 0);
	CHECK(S_ISFIFO(st.st_mode));

	goipc_queue *peer;
	REQUIRE_RC(goipc_queue_open(name, &peer), GOIPC_OK);
	REQUIRE(goipc_queue_capacity(peer) == 8192);
	REQUIRE_RC(goipc_queue_try_send(peer, 7, "hi", 2), GOIPC_OK);
	char buf[8];
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_queue_try_recv(q, buf, sizeof buf, &type, &len), GOIPC_OK);
	REQUIRE(type == 7 && len == 2 && memcmp(buf, "hi", 2) == 0);
	REQUIRE_RC(goipc_queue_close(peer), GOIPC_OK);
	goipc_queue_destroy(peer);

	REQUIRE_RC(goipc_queue_close(q), GOIPC_OK);
	REQUIRE_RC(goipc_queue_unlink(q), GOIPC_OK);
	CHECK(!path_exists(seg));
	CHECK(!path_exists(ne));
	CHECK(!path_exists(nf));
	goipc_queue_destroy(q);
	free(seg);
}

TEST(queue_rejects_bad_arguments, 0)
{
	goipc_queue *q;
	char name[96];
	unique_name(name, sizeof name, "q");
	CHECK_RC(goipc_queue_create(name, 1000, &q), GOIPC_EINVALCAP);
	CHECK_RC(goipc_queue_create(name, 5000, &q), GOIPC_EINVALCAP);
	CHECK_RC(goipc_queue_create(name, 2048, &q), GOIPC_EINVALCAP);
	CHECK_RC(goipc_queue_create("a/b", 4096, &q), GOIPC_EINVALNAME);
	CHECK_RC(goipc_queue_open("..", &q), GOIPC_EINVALNAME);
	CHECK_RC(goipc_queue_open(name, &q), GOIPC_ESYS);
	CHECK(goipc_last_errno() == ENOENT, "errno %d", goipc_last_errno());

	REQUIRE_RC(goipc_queue_create(name, 0, &q), GOIPC_OK);
	CHECK(goipc_queue_capacity(q) == GOIPC_DEFAULT_CAPACITY);
	uint8_t big[8];
	CHECK_RC(goipc_queue_send(q, GOIPC_TYPE_PADDING, big, 1, 0), GOIPC_ERESERVED);
	CHECK_RC(goipc_queue_send(q, 1, big, goipc_queue_max_message_size(q) + 1, 0), GOIPC_ETOOLARGE);
	drop_queue(q);
}

TEST(queue_operations_after_close_fail, 0)
{
	char name[96];
	goipc_queue *q = new_queue(name, sizeof name, 4096);
	REQUIRE_RC(goipc_queue_close(q), GOIPC_OK);
	uint8_t buf[8];
	uint32_t type;
	size_t len;
	goipc_claim c;
	CHECK_RC(goipc_queue_close(q), GOIPC_ECLOSED);
	CHECK_RC(goipc_queue_try_send(q, 1, buf, 1), GOIPC_ECLOSED);
	CHECK_RC(goipc_queue_send(q, 1, buf, 1, -1), GOIPC_ECLOSED);
	CHECK_RC(goipc_queue_claim(q, 1, 1, -1, &c), GOIPC_ECLOSED);
	CHECK_RC(goipc_queue_try_recv(q, buf, sizeof buf, &type, &len), GOIPC_ECLOSED);
	CHECK_RC(goipc_queue_recv(q, buf, sizeof buf, &type, &len, -1), GOIPC_ECLOSED);
	CHECK_RC(goipc_queue_unlink(q), GOIPC_OK);
	goipc_queue_destroy(q);
}

TEST(queue_recv_small_buffer_keeps_message, 0)
{
	char name[96];
	goipc_queue *q = new_queue(name, sizeof name, 4096);
	uint8_t msg[100] = {1, 2, 3}, out[100];
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_queue_send(q, 4, msg, sizeof msg, -1), GOIPC_OK);
	REQUIRE_RC(goipc_queue_recv(q, out, 10, &type, &len, -1), GOIPC_EBUFFER);
	REQUIRE(len == 100);
	REQUIRE_RC(goipc_queue_try_recv(q, out, 10, &type, &len), GOIPC_EBUFFER);
	REQUIRE_RC(goipc_queue_recv(q, out, sizeof out, &type, &len, -1), GOIPC_OK);
	REQUIRE(type == 4 && len == 100 && memcmp(out, msg, 100) == 0);
	REQUIRE_RC(goipc_queue_try_recv(q, out, sizeof out, &type, &len), GOIPC_EEMPTY);
	drop_queue(q);
}

TEST(queue_timeouts, 0)
{
	char name[96];
	goipc_queue *q = new_queue(name, sizeof name, 4096);
	uint8_t out[16];
	uint32_t type;
	size_t len;
	int64_t start = now_ns();
	REQUIRE_RC(goipc_queue_recv(q, out, sizeof out, &type, &len, MS(30)), GOIPC_ETIMEDOUT);
	CHECK(now_ns() - start >= MS(30));
	REQUIRE_RC(goipc_queue_recv(q, out, sizeof out, &type, &len, 0), GOIPC_ETIMEDOUT);
	REQUIRE(fill(q, 1000) > 0);
	start = now_ns();
	REQUIRE_RC(goipc_queue_send(q, 1, out, 1000, MS(30)), GOIPC_ETIMEDOUT);
	CHECK(now_ns() - start >= MS(30));
	goipc_claim c;
	REQUIRE_RC(goipc_queue_claim(q, 1, 1000, 0, &c), GOIPC_ETIMEDOUT);
	CHECK(atomic_load(&goipc__hdr(goipc_queue_ring(q))->send_waiters) == 0);
	CHECK(atomic_load(&goipc__hdr(goipc_queue_ring(q))->recv_waiters) == 0);
	drop_queue(q);
}

struct op {
	goipc_queue *q;
	int rc;
	atomic_bool done;
	uint8_t buf[64];
	uint32_t type;
	size_t len;
};

static void *recv_thread(void *arg)
{
	struct op *o = arg;
	o->rc = goipc_queue_recv(o->q, o->buf, sizeof o->buf, &o->type, &o->len, -1);
	atomic_store(&o->done, true);
	return NULL;
}

static void *send_thread(void *arg)
{
	struct op *o = arg;
	o->rc = goipc_queue_send(o->q, 3, o->buf, sizeof o->buf, -1);
	atomic_store(&o->done, true);
	return NULL;
}

TEST(queue_blocking_recv_wakes_on_send, T_THREADS)
{
	char name[96];
	goipc_queue *q = new_queue(name, sizeof name, 4096);
	struct op o = {.q = q, .rc = 99};
	pthread_t th;
	REQUIRE(pthread_create(&th, NULL, recv_thread, &o) == 0);
	pause_ms(30);
	CHECK(!atomic_load(&o.done), "recv returned on an empty queue");
	CHECK(atomic_load(&goipc__hdr(goipc_queue_ring(q))->recv_waiters) == 1);
	REQUIRE_RC(goipc_queue_send(q, 8, "wake", 4, -1), GOIPC_OK);
	pthread_join(th, NULL);
	REQUIRE_RC(o.rc, GOIPC_OK);
	REQUIRE(o.type == 8 && o.len == 4 && memcmp(o.buf, "wake", 4) == 0);
	CHECK(atomic_load(&goipc__hdr(goipc_queue_ring(q))->recv_waiters) == 0);
	drop_queue(q);
}

TEST(queue_full_blocks_sender_until_read, T_THREADS)
{
	char name[96];
	goipc_queue *q = new_queue(name, sizeof name, 4096);
	REQUIRE(fill(q, 1000) == 4);
	struct op o = {.q = q, .rc = 99};
	pthread_t th;
	REQUIRE(pthread_create(&th, NULL, send_thread, &o) == 0);
	pause_ms(30);
	CHECK(!atomic_load(&o.done), "send returned on a full queue");
	uint8_t out[1000];
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_queue_recv(q, out, sizeof out, &type, &len, -1), GOIPC_OK);
	pthread_join(th, NULL);
	REQUIRE_RC(o.rc, GOIPC_OK);
	CHECK(atomic_load(&goipc__hdr(goipc_queue_ring(q))->send_waiters) == 0);
	drop_queue(q);
}

TEST(queue_close_releases_blocked_send_and_recv, T_THREADS)
{
	char name1[96], name2[96];
	goipc_queue *empty = new_queue(name1, sizeof name1, 4096);
	goipc_queue *full = new_queue(name2, sizeof name2, 4096);
	REQUIRE(fill(full, 1000) == 4);
	struct op r = {.q = empty, .rc = 99}, s = {.q = full, .rc = 99};
	pthread_t tr, ts;
	REQUIRE(pthread_create(&tr, NULL, recv_thread, &r) == 0);
	REQUIRE(pthread_create(&ts, NULL, send_thread, &s) == 0);
	pause_ms(30);
	REQUIRE_RC(goipc_queue_close(empty), GOIPC_OK);
	REQUIRE_RC(goipc_queue_close(full), GOIPC_OK);
	pthread_join(tr, NULL);
	pthread_join(ts, NULL);
	CHECK_RC(r.rc, GOIPC_ECLOSED);
	CHECK_RC(s.rc, GOIPC_ECLOSED);
	drop_queue(empty);
	drop_queue(full);
}

struct batch {
	int n;
	uint32_t types[16];
};

static void batch_fn(void *ctx, uint32_t type, const uint8_t *payload, size_t len)
{
	struct batch *b = ctx;
	(void)payload, (void)len;
	if (b->n < 16)
		b->types[b->n] = type;
	b->n++;
}

TEST(queue_read_batch, 0)
{
	char name[96];
	goipc_queue *q = new_queue(name, sizeof name, 4096);
	for (uint32_t i = 0; i < 5; i++)
		REQUIRE_RC(goipc_queue_send(q, i, "x", 1, -1), GOIPC_OK);
	struct batch b = {0};
	REQUIRE(goipc_queue_read_batch(q, 3, batch_fn, &b, -1) == 3);
	REQUIRE(goipc_queue_read_batch(q, 10, batch_fn, &b, -1) == 2);
	REQUIRE(b.n == 5);
	for (uint32_t i = 0; i < 5; i++)
		CHECK(b.types[i] == i);
	REQUIRE_RC(goipc_queue_read_batch(q, 10, batch_fn, &b, MS(10)), GOIPC_ETIMEDOUT);
	REQUIRE_RC(goipc_queue_read_batch(q, 0, batch_fn, &b, MS(10)), GOIPC_EINVAL);
	drop_queue(q);
}

TEST(queue_claim_commit_and_abort, 0)
{
	char name[96];
	goipc_queue *q = new_queue(name, sizeof name, 4096);
	goipc_claim c;
	REQUIRE_RC(goipc_queue_claim(q, 2, 5, -1, &c), GOIPC_OK);
	memcpy(c.bytes, "hello", 5);
	goipc_queue_commit(q, &c);
	REQUIRE_RC(goipc_queue_claim(q, 3, 4, -1, &c), GOIPC_OK);
	goipc_queue_abort(q, &c);
	uint8_t out[16];
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_queue_recv(q, out, sizeof out, &type, &len, 0), GOIPC_OK);
	REQUIRE(type == 2 && len == 5 && memcmp(out, "hello", 5) == 0);
	REQUIRE_RC(goipc_queue_recv(q, out, sizeof out, &type, &len, 0), GOIPC_ETIMEDOUT);
	REQUIRE(goipc_ring_empty(goipc_queue_ring(q)));
	drop_queue(q);
}

static void *claim_thread(void *arg)
{
	struct op *o = arg;
	goipc_claim c;
	o->rc = goipc_queue_claim(o->q, 3, 1000, -1, &c);
	if (o->rc == GOIPC_OK)
		goipc_queue_commit(o->q, &c);
	atomic_store(&o->done, true);
	return NULL;
}

/* A read that only steps over aborted padding still frees room, so it must
 * wake a parked sender. */
TEST(queue_abort_then_read_wakes_parked_sender, T_THREADS)
{
	char name[96];
	goipc_queue *q = new_queue(name, sizeof name, 4096);
	goipc_claim c;
	REQUIRE_RC(goipc_queue_claim(q, 1, 1016, -1, &c), GOIPC_OK);
	REQUIRE(fill(q, 1016) == 3);
	struct op o = {.q = q, .rc = 99};
	pthread_t th;
	REQUIRE(pthread_create(&th, NULL, claim_thread, &o) == 0);
	pause_ms(30);
	CHECK(!atomic_load(&o.done));
	goipc_queue_abort(q, &c);
	/* A zero-size buffer steps over the padding and then stops. */
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_queue_try_recv(q, NULL, 0, &type, &len), GOIPC_EBUFFER);
	REQUIRE(len == 1016);
	pthread_join(th, NULL);
	REQUIRE_RC(o.rc, GOIPC_OK);
	struct batch b = {0};
	REQUIRE(goipc_queue_read_batch(q, 10, batch_fn, &b, -1) == 4);
	drop_queue(q);
}

#define FORK_SENDERS 3
#define FORK_COUNT 3000

TEST(queue_cross_process_keeps_order, T_FORK)
{
	char name[96];
	goipc_queue *q = new_queue(name, sizeof name, 4096);
	pid_t pids[FORK_SENDERS];
	for (int s = 0; s < FORK_SENDERS; s++) {
		pids[s] = fork();
		REQUIRE(pids[s] >= 0);
		if (pids[s] == 0) {
			goipc_queue *cq;
			if (goipc_queue_open(name, &cq) != GOIPC_OK)
				_exit(2);
			char msg[64];
			for (uint32_t i = 0; i < FORK_COUNT; i++) {
				int n = snprintf(msg, sizeof msg, "%d:%u", s, i);
				if (goipc_queue_send(cq, i, msg, (size_t)n, MS(20000)) != GOIPC_OK)
					_exit(3);
			}
			if (goipc_queue_close(cq) != GOIPC_OK)
				_exit(4);
			goipc_queue_destroy(cq);
			_exit(0);
		}
	}
	uint32_t next[FORK_SENDERS] = {0};
	char buf[64];
	for (int i = 0; i < FORK_SENDERS * FORK_COUNT; i++) {
		uint32_t type;
		size_t len;
		REQUIRE_RC(goipc_queue_recv(q, buf, sizeof buf - 1, &type, &len, MS(20000)), GOIPC_OK);
		buf[len] = '\0';
		int s;
		unsigned seq;
		REQUIRE(sscanf(buf, "%d:%u", &s, &seq) == 2 && s >= 0 && s < FORK_SENDERS, "payload %s", buf);
		REQUIRE(seq == next[s] && type == seq, "sender %d seq %u type %u, want %u", s, seq, type, next[s]);
		next[s]++;
	}
	for (int s = 0; s < FORK_SENDERS; s++) {
		int status;
		REQUIRE(waitpid(pids[s], &status, 0) == pids[s]);
		CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0, "sender %d status %d", s, status);
	}
	drop_queue(q);
}

#define THREAD_SENDERS 4
#define THREAD_COUNT 5000

struct tsender {
	goipc_queue *q;
	uint32_t id;
};

static void *tsend(void *arg)
{
	struct tsender *t = arg;
	uint32_t msg[2] = {t->id, 0};
	for (uint32_t i = 0; i < THREAD_COUNT; i++) {
		msg[1] = i;
		CHECK_RC(goipc_queue_send(t->q, t->id, msg, sizeof msg, MS(20000)), GOIPC_OK);
	}
	return NULL;
}

TEST(queue_threaded_senders_keep_order, T_THREADS)
{
	char name[96];
	goipc_queue *q = new_queue(name, sizeof name, 4096);
	pthread_t th[THREAD_SENDERS];
	struct tsender ts[THREAD_SENDERS];
	for (uint32_t i = 0; i < THREAD_SENDERS; i++) {
		ts[i] = (struct tsender){q, i};
		REQUIRE(pthread_create(&th[i], NULL, tsend, &ts[i]) == 0);
	}
	uint32_t next[THREAD_SENDERS] = {0};
	for (int i = 0; i < THREAD_SENDERS * THREAD_COUNT; i++) {
		uint32_t msg[2], type;
		size_t len;
		REQUIRE_RC(goipc_queue_recv(q, msg, sizeof msg, &type, &len, MS(20000)), GOIPC_OK);
		REQUIRE(len == sizeof msg && msg[0] == type && type < THREAD_SENDERS);
		REQUIRE(msg[1] == next[type], "sender %u seq %u, want %u", type, msg[1], next[type]);
		next[type]++;
	}
	for (int i = 0; i < THREAD_SENDERS; i++)
		pthread_join(th[i], NULL);
	drop_queue(q);
}
