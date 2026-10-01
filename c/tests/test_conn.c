#define _GNU_SOURCE
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "harness.h"
#include "internal.h"

TEST(channel_carries_both_directions, 0)
{
	char name[96];
	unique_name(name, sizeof name, "ch");
	goipc_channel *a, *b;
	REQUIRE_RC(goipc_channel_create(name, 4096, &a), GOIPC_OK);
	REQUIRE_RC(goipc_channel_open(name, &b), GOIPC_OK);
	REQUIRE(goipc_channel_max_message_size(a) == 2040);
	char c2o[128];
	snprintf(c2o, sizeof c2o, "%s.c2o", name);
	REQUIRE(strcmp(goipc_queue_name(goipc_channel_tx(a)), c2o) == 0);
	REQUIRE(strcmp(goipc_queue_name(goipc_channel_rx(b)), c2o) == 0);

	char buf[16];
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_channel_send(a, 1, "ping", 4, -1), GOIPC_OK);
	REQUIRE_RC(goipc_channel_recv(b, buf, sizeof buf, &type, &len, MS(1000)), GOIPC_OK);
	REQUIRE(type == 1 && len == 4 && memcmp(buf, "ping", 4) == 0);
	REQUIRE_RC(goipc_channel_send(b, 2, "pong", 4, -1), GOIPC_OK);
	REQUIRE_RC(goipc_channel_recv(a, buf, sizeof buf, &type, &len, MS(1000)), GOIPC_OK);
	REQUIRE(type == 2 && len == 4 && memcmp(buf, "pong", 4) == 0);

	CHECK_RC(goipc_channel_create("..", 4096, &a), GOIPC_EINVALNAME);
	REQUIRE_RC(goipc_channel_close(b), GOIPC_OK);
	goipc_channel_destroy(b);
	REQUIRE_RC(goipc_channel_close(a), GOIPC_OK);
	REQUIRE_RC(goipc_channel_unlink(a), GOIPC_OK);
	goipc_channel_destroy(a);
}

static uint8_t pattern(size_t i)
{
	return (uint8_t)(i * 31 + 7);
}

struct writer {
	goipc_conn *c;
	const uint8_t *data;
	size_t len;
	bool close_write;
	int rc;
	size_t written;
};

static void *write_thread(void *arg)
{
	struct writer *w = arg;
	w->rc = goipc_conn_write(w->c, w->data, w->len, MS(20000), &w->written);
	if (w->rc == GOIPC_OK && w->close_write)
		w->rc = goipc_conn_close_write(w->c, MS(20000));
	return NULL;
}

static void conn_pair(char *name, size_t len, goipc_conn **l, goipc_conn **d)
{
	unique_name(name, len, "conn");
	REQUIRE_RC(goipc_conn_listen(name, 4096, l), GOIPC_OK);
	REQUIRE_RC(goipc_conn_dial(name, d), GOIPC_OK);
}

static void drop_pair(goipc_conn *l, goipc_conn *d)
{
	goipc_conn_close(d);
	goipc_conn_destroy(d);
	goipc_conn_close(l);
	goipc_conn_unlink(l);
	goipc_conn_destroy(l);
}

/* read_until_eof reads into buf until end-of-stream and returns the count. */
static size_t read_until_eof(goipc_conn *c, uint8_t *buf, size_t cap)
{
	size_t have = 0;
	for (;;) {
		size_t n;
		int rc = goipc_conn_read(c, buf + have, cap - have, MS(20000), &n);
		if (rc == GOIPC_EOF)
			return have;
		REQUIRE_RC(rc, GOIPC_OK);
		have += n;
		REQUIRE(have < cap, "more bytes than expected");
	}
}

TEST(conn_splits_large_write_and_reassembles, T_THREADS)
{
	char name[96];
	goipc_conn *l, *d;
	conn_pair(name, sizeof name, &l, &d);
	enum { N = 50000 };
	uint8_t *data = malloc(N), *got = malloc(N + 1);
	for (size_t i = 0; i < N; i++)
		data[i] = pattern(i);
	struct writer w = {d, data, N, true, 99, 0};
	pthread_t th;
	REQUIRE(pthread_create(&th, NULL, write_thread, &w) == 0);
	size_t have = read_until_eof(l, got, N + 1);
	pthread_join(th, NULL);
	REQUIRE_RC(w.rc, GOIPC_OK);
	REQUIRE(w.written == N);
	REQUIRE(have == N, "read %zu bytes", have);
	REQUIRE(memcmp(got, data, N) == 0);
	free(data);
	free(got);
	drop_pair(l, d);
}

TEST(conn_close_gives_peer_eof, 0)
{
	char name[96];
	goipc_conn *l, *d;
	conn_pair(name, sizeof name, &l, &d);
	size_t n;
	REQUIRE_RC(goipc_conn_write(l, "last", 4, -1, &n), GOIPC_OK);
	REQUIRE(n == 4);
	REQUIRE_RC(goipc_conn_close(l), GOIPC_OK);
	REQUIRE_RC(goipc_conn_close(l), GOIPC_ECLOSED);
	char buf[2];
	REQUIRE_RC(goipc_conn_read(d, buf, 2, MS(1000), &n), GOIPC_OK);
	REQUIRE(n == 2 && memcmp(buf, "la", 2) == 0);
	REQUIRE_RC(goipc_conn_read(d, buf, 2, MS(1000), &n), GOIPC_OK);
	REQUIRE(n == 2 && memcmp(buf, "st", 2) == 0);
	REQUIRE_RC(goipc_conn_read(d, buf, 2, MS(1000), &n), GOIPC_EOF);
	REQUIRE(n == 0);
	REQUIRE_RC(goipc_conn_read(d, buf, 2, MS(1000), &n), GOIPC_EOF);
	drop_pair(l, d);
}

TEST(conn_read_times_out, 0)
{
	char name[96];
	goipc_conn *l, *d;
	conn_pair(name, sizeof name, &l, &d);
	char buf[4];
	size_t n;
	REQUIRE_RC(goipc_conn_read(d, buf, sizeof buf, MS(20), &n), GOIPC_ETIMEDOUT);
	REQUIRE_RC(goipc_conn_write(d, "", 0, MS(20), &n), GOIPC_OK);
	REQUIRE(n == 0);
	drop_pair(l, d);
}

TEST(conn_close_write_half_closes, 0)
{
	char name[96];
	goipc_conn *l, *d;
	conn_pair(name, sizeof name, &l, &d);
	size_t n;
	REQUIRE_RC(goipc_conn_write(d, "abc", 3, -1, &n), GOIPC_OK);
	REQUIRE_RC(goipc_conn_close_write(d, MS(1000)), GOIPC_OK);
	REQUIRE_RC(goipc_conn_write(d, "x", 1, -1, &n), GOIPC_ECLOSED);
	REQUIRE(n == 0);
	REQUIRE_RC(goipc_conn_close_write(d, MS(1000)), GOIPC_ECLOSED);

	uint8_t buf[16];
	REQUIRE(read_until_eof(l, buf, sizeof buf) == 3);
	REQUIRE(memcmp(buf, "abc", 3) == 0);

	/* The half-closed side still reads what the peer writes back. */
	REQUIRE_RC(goipc_conn_write(l, "reply", 5, -1, &n), GOIPC_OK);
	REQUIRE_RC(goipc_conn_read(d, buf, sizeof buf, MS(1000), &n), GOIPC_OK);
	REQUIRE(n == 5 && memcmp(buf, "reply", 5) == 0);

	/* Close after close_write sends no second end-of-stream. The empty
	 * channel then reports its closed peer. */
	REQUIRE_RC(goipc_conn_close(d), GOIPC_OK);
	uint32_t type;
	REQUIRE_RC(goipc_queue_try_recv(goipc_channel_rx(goipc_conn_channel(l)), buf, sizeof buf, &type, &n), GOIPC_EPEERGONE);
	goipc_conn_destroy(d);
	REQUIRE_RC(goipc_conn_close(l), GOIPC_OK);
	goipc_conn_unlink(l);
	goipc_conn_destroy(l);
}

TEST(conn_close_write_waits_for_room, T_THREADS)
{
	char name[96];
	goipc_conn *l, *d;
	conn_pair(name, sizeof name, &l, &d);
	goipc_queue *tx = goipc_channel_tx(goipc_conn_channel(d));
	static uint8_t p[1016];
	while (goipc_queue_try_send(tx, GOIPC_CONN_TYPE_DATA, p, sizeof p) == GOIPC_OK)
		;
	struct writer w = {d, p, 0, true, 99, 0};
	pthread_t th;
	REQUIRE(pthread_create(&th, NULL, write_thread, &w) == 0);
	static uint8_t buf[8192];
	REQUIRE(read_until_eof(l, buf, sizeof buf) == 4 * 1016);
	pthread_join(th, NULL);
	REQUIRE_RC(w.rc, GOIPC_OK);
	drop_pair(l, d);
}
