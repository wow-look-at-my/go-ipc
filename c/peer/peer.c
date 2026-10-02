/* goipc-peer: the C side of the cross-language suite. spec/peer.md is the
 * contract. */
#define _GNU_SOURCE
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "goipc.h"

#include "demo.h"
#include "messages.h"

/* fixture.h builds each values.json entry. Its check functions are unused here. */
static int eq_mem(const void *a, size_t an, const void *b, size_t bn)
{
	return an == bn && (an == 0 || memcmp(a, b, an) == 0);
}
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
#include "fixture.h"
/* T##_peer_type is the type ID of the C type T. A message absent from values.json leaves one unused. */
#pragma GCC diagnostic ignored "-Wunused-const-variable"
#define PEER_TYPE_ID(T, id) static const uint32_t T##_peer_type = id;
PEER_MESSAGES(PEER_TYPE_ID)
#pragma GCC diagnostic pop

#define PEER_TIMEOUT_NS (INT64_C(60) * 1000000000)

static struct timespec started;

static int64_t left_ns(void)
{
	struct timespec now;
	clock_gettime(CLOCK_MONOTONIC, &now);
	int64_t spent = ((int64_t)now.tv_sec - (int64_t)started.tv_sec) * 1000000000 + (now.tv_nsec - started.tv_nsec);
	int64_t left = PEER_TIMEOUT_NS - spent;
	return left > 0 ? left : 0;
}

static int fail(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	fputs("goipc-peer: ", stderr);
	vfprintf(stderr, fmt, ap);
	fputc('\n', stderr);
	va_end(ap);
	return 1;
}

static int fail_rc(const char *what, int rc)
{
	if (rc == GOIPC_ESYS)
		return fail("%s: %s: %s", what, goipc_strerror(rc), strerror(goipc_last_errno()));
	return fail("%s: %s", what, goipc_strerror(rc));
}

static int parse_u64(const char *s, uint64_t max, uint64_t *out)
{
	if (*s == '\0')
		return -1;
	uint64_t v = 0;
	for (; *s; s++) {
		if (*s < '0' || *s > '9')
			return -1;
		uint64_t d = (uint64_t)(*s - '0');
		if (v > (max - d) / 10)
			return -1;
		v = v * 10 + d;
	}
	*out = v;
	return 0;
}

static int ready(void)
{
	if (fputs("ready\n", stdout) == EOF || fflush(stdout) != 0)
		return fail("write ready: %s", strerror(errno));
	return 0;
}

struct sender {
	char *name;
	uint64_t next;
};

static int role_recv(const char *name, uint64_t total, uint64_t capacity)
{
	goipc_queue *q;
	int rc = goipc_queue_create(name, (size_t)capacity, &q);
	if (rc != GOIPC_OK)
		return fail_rc("create queue", rc);
	int status = ready();
	size_t cap = goipc_queue_max_message_size(q);
	char *buf = malloc(cap + 1);
	struct sender *senders = NULL;
	size_t nsenders = 0;
	if (buf == NULL)
		status = fail("out of memory");

	for (uint64_t i = 0; status == 0 && i < total; i++) {
		uint32_t type;
		size_t len;
		rc = goipc_queue_recv(q, buf, cap, &type, &len, left_ns());
		if (rc != GOIPC_OK) {
			status = fail("message %" PRIu64 " of %" PRIu64 ": %s", i, total, goipc_strerror(rc));
			break;
		}
		buf[len] = '\0';
		char *colon = memchr(buf, ':', len);
		if (colon == NULL || strlen(buf) != len) {
			status = fail("message %" PRIu64 ": payload %s has no ':'", i, buf);
			break;
		}
		*colon = '\0';
		uint64_t seq;
		if (parse_u64(colon + 1, UINT32_MAX, &seq) != 0) {
			status = fail("message %" PRIu64 ": bad sequence %s", i, colon + 1);
			break;
		}
		if ((uint64_t)type != seq) {
			status = fail("message %" PRIu64 ": type %" PRIu32 ", seq %" PRIu64, i, type, seq);
			break;
		}
		struct sender *s = NULL;
		for (size_t k = 0; k < nsenders; k++)
			if (strcmp(senders[k].name, buf) == 0)
				s = &senders[k];
		if (s == NULL) {
			struct sender *grown = realloc(senders, (nsenders + 1) * sizeof *senders);
			char *dup = strdup(buf);
			if (grown == NULL || dup == NULL) {
				free(dup);
				if (grown != NULL)
					senders = grown;
				status = fail("out of memory");
				break;
			}
			senders = grown;
			s = &senders[nsenders++];
			s->name = dup;
			s->next = 0;
		}
		if (seq != s->next) {
			status = fail("message %" PRIu64 ": sender %s sent seq %" PRIu64 ", want %" PRIu64, i, buf, seq, s->next);
			break;
		}
		s->next = seq + 1;
	}
	if (status == 0 && (printf("ok %" PRIu64 "\n", total) < 0 || fflush(stdout) != 0))
		status = fail("write ok: %s", strerror(errno));

	if ((rc = goipc_queue_close(q)) != GOIPC_OK && status == 0)
		status = fail_rc("close", rc);
	if ((rc = goipc_queue_unlink(q)) != GOIPC_OK && status == 0)
		status = fail_rc("unlink", rc);
	goipc_queue_destroy(q);
	for (size_t k = 0; k < nsenders; k++)
		free(senders[k].name);
	free(senders);
	free(buf);
	return status;
}

/* A seq_check follows the sequence of each sender, as the recv role does. */
struct seq_check {
	struct sender *senders;
	size_t n;
};

static void seq_free(struct seq_check *c)
{
	for (size_t k = 0; k < c->n; k++)
		free(c->senders[k].name);
	free(c->senders);
}

/* */
static int seq_next(struct seq_check *c, uint64_t i, uint32_t type, char *buf, size_t len)
{
	buf[len] = '\0';
	char *colon = memchr(buf, ':', len);
	if (colon == NULL || strlen(buf) != len)
		return fail("message %" PRIu64 ": payload %s has no ':'", i, buf);
	*colon = '\0';
	uint64_t seq;
	if (parse_u64(colon + 1, UINT32_MAX, &seq) != 0)
		return fail("message %" PRIu64 ": bad sequence %s", i, colon + 1);
	if ((uint64_t)type != seq)
		return fail("message %" PRIu64 ": type %" PRIu32 ", seq %" PRIu64, i, type, seq);
	struct sender *s = NULL;
	for (size_t k = 0; k < c->n; k++)
		if (strcmp(c->senders[k].name, buf) == 0)
			s = &c->senders[k];
	if (s == NULL) {
		struct sender *grown = realloc(c->senders, (c->n + 1) * sizeof *grown);
		if (grown == NULL)
			return fail("out of memory");
		c->senders = grown;
		char *dup = strdup(buf);
		if (dup == NULL)
			return fail("out of memory");
		s = &c->senders[c->n++];
		s->name = dup;
		s->next = 0;
	}
	if (seq != s->next)
		return fail("message %" PRIu64 ": sender %s sent seq %" PRIu64 ", want %" PRIu64, i, buf, seq, s->next);
	s->next = seq + 1;
	return 0;
}

/* ---- roles that stand in for a process that dies ---- */

static int role_claim_and_die(const char *name, uint64_t length)
{
	goipc_queue *q;
	int rc = goipc_queue_open(name, &q);
	if (rc != GOIPC_OK)
		return fail_rc("open queue", rc);
	goipc_claim c;
	rc = goipc_queue_claim(q, 1, (size_t)length, left_ns(), &c);
	if (rc != GOIPC_OK)
		return fail_rc("claim", rc);
	memset(c.bytes, 0xAB, c.len);
	_exit(0);
}

static int role_send_until_gone(const char *name, const char *sender)
{
	goipc_queue *q;
	int rc = goipc_queue_open(name, &q);
	if (rc != GOIPC_OK)
		return fail_rc("open queue", rc);
	size_t cap = strlen(sender) + 32;
	char *payload = malloc(cap);
	int status = payload == NULL ? fail("out of memory") : 0;
	uint64_t sent = 0;
	while (status == 0) {
		if (sent > UINT32_MAX) {
			status = fail("the receiver never went");
			break;
		}
		int n = snprintf(payload, cap, "%s:%" PRIu64, sender, sent);
		rc = goipc_queue_send(q, (uint32_t)sent, payload, (size_t)n, left_ns());
		if (rc == GOIPC_EPEERGONE)
			break;
		if (rc != GOIPC_OK)
			status = fail_rc("send", rc);
		else
			sent++;
	}
	if (status == 0 && (printf("gone %" PRIu64 "\n", sent) < 0 || fflush(stdout) != 0))
		status = fail("write gone: %s", strerror(errno));
	if ((rc = goipc_queue_close(q)) != GOIPC_OK && status == 0)
		status = fail_rc("close", rc);
	goipc_queue_destroy(q);
	free(payload);
	return status;
}

enum stop_mode { STOP_CLOSE, STOP_EXIT };

static int parse_mode(const char *s, enum stop_mode *mode)
{
	if (strcmp(s, "close") == 0)
		*mode = STOP_CLOSE;
	else if (strcmp(s, "exit") == 0)
		*mode = STOP_EXIT;
	else
		return fail("mode: %s is neither close nor exit", s);
	return 0;
}

static int role_recv_then_stop(const char *name, uint64_t count, uint64_t capacity, enum stop_mode mode)
{
	goipc_queue *q;
	int rc = goipc_queue_create(name, (size_t)capacity, &q);
	if (rc != GOIPC_OK)
		return fail_rc("create queue", rc);
	int status = ready();
	size_t cap = goipc_queue_max_message_size(q);
	char *buf = malloc(cap + 1);
	struct seq_check check = {NULL, 0};
	if (buf == NULL && status == 0)
		status = fail("out of memory");
	for (uint64_t i = 0; status == 0 && i < count; i++) {
		uint32_t type;
		size_t len;
		rc = goipc_queue_recv(q, buf, cap, &type, &len, left_ns());
		if (rc != GOIPC_OK)
			status = fail("message %" PRIu64 " of %" PRIu64 ": %s", i, count, goipc_strerror(rc));
		else
			status = seq_next(&check, i, type, buf, len);
	}
	seq_free(&check);
	free(buf);
	if (status == 0 && (printf("ok %" PRIu64 "\n", count) < 0 || fflush(stdout) != 0))
		status = fail("write ok: %s", strerror(errno));
	if (mode == STOP_EXIT) {
		if ((rc = goipc_queue_unlink(q)) != GOIPC_OK && status == 0)
			status = fail_rc("unlink", rc);
		_exit(status);
	}
	if ((rc = goipc_queue_close(q)) != GOIPC_OK && status == 0)
		status = fail_rc("close", rc);
	if ((rc = goipc_queue_unlink(q)) != GOIPC_OK && status == 0)
		status = fail_rc("unlink", rc);
	goipc_queue_destroy(q);
	return status;
}

static int role_chan_recv_until_gone(const char *name, uint64_t capacity, uint64_t count)
{
	goipc_channel *c;
	int rc = goipc_channel_create(name, (size_t)capacity, &c);
	if (rc != GOIPC_OK)
		return fail_rc("create channel", rc);
	int status = ready();
	char buf[64], want[64];
	uint64_t got = 0;
	while (status == 0) {
		uint32_t type;
		size_t len;
		rc = goipc_channel_recv(c, buf, sizeof buf, &type, &len, left_ns());
		if (rc == GOIPC_EPEERGONE)
			break;
		if (rc != GOIPC_OK) {
			status = fail("message %" PRIu64 ": %s", got, goipc_strerror(rc));
			break;
		}
		int n = snprintf(want, sizeof want, "0:%" PRIu64, got);
		if ((uint64_t)type != got || len != (size_t)n || memcmp(buf, want, len) != 0)
			status = fail("message %" PRIu64 ": type %" PRIu32 " payload %.*s, want %s", got, type, (int)len, buf, want);
		got++;
	}
	if (status == 0 && got != count)
		status = fail("received %" PRIu64 " messages before peer-gone, want %" PRIu64, got, count);
	if (status == 0 && (rc = goipc_channel_send(c, 0, "x", 1, left_ns())) != GOIPC_EPEERGONE)
		status = fail("a send after the peer went reports %s, want peer-gone", goipc_strerror(rc));
	if (status == 0 && (printf("ok %" PRIu64 "\n", count) < 0 || fflush(stdout) != 0))
		status = fail("write ok: %s", strerror(errno));
	if ((rc = goipc_channel_close(c)) != GOIPC_OK && status == 0)
		status = fail_rc("close", rc);
	if ((rc = goipc_channel_unlink(c)) != GOIPC_OK && status == 0)
		status = fail_rc("unlink", rc);
	goipc_channel_destroy(c);
	return status;
}

static int role_chan_send_then_stop(const char *name, uint64_t count, enum stop_mode mode)
{
	goipc_channel *c;
	int rc = goipc_channel_open(name, &c);
	if (rc != GOIPC_OK)
		return fail_rc("open channel", rc);
	int status = 0;
	char payload[32];
	for (uint64_t i = 0; status == 0 && i < count; i++) {
		int n = snprintf(payload, sizeof payload, "0:%" PRIu64, i);
		if ((rc = goipc_channel_send(c, (uint32_t)i, payload, (size_t)n, left_ns())) != GOIPC_OK)
			status = fail_rc("send", rc);
	}
	if (mode == STOP_EXIT)
		_exit(status);
	if ((rc = goipc_channel_close(c)) != GOIPC_OK && status == 0)
		status = fail_rc("close", rc);
	goipc_channel_destroy(c);
	return status;
}

/* ---- the service roles ---- */

enum {
	SERVICE_ECHO = 1,
	SERVICE_ECHOED = 2,
	SERVICE_FAIL = 3,
	SERVICE_WHO = 4,
	SERVICE_WHOAMI = 5,
};

/* A counter counts the clients that went, for the serve role to wait on. */
struct counter {
	pthread_mutex_t mu;
	pthread_cond_t cv;
	uint64_t gone;
};

static void service_answer(void *ctx, goipc_session *s, uint32_t type, const uint8_t *payload, size_t len, goipc_reply *reply)
{
	(void)ctx;
	char text[64];
	switch (type) {
	case SERVICE_ECHO:
		goipc_reply_set(reply, SERVICE_ECHOED, payload, len);
		return;
	case SERVICE_FAIL: {
		char *msg = malloc(len + 1);
		if (msg == NULL) {
			goipc_reply_error(reply, "out of memory");
			return;
		}
		memcpy(msg, payload, len);
		msg[len] = '\0';
		goipc_reply_error(reply, msg);
		free(msg);
		return;
	}
	case SERVICE_WHO: {
		uint64_t ordinal = goipc_session_ordinal(s);
		uint8_t b[8];
		for (int i = 0; i < 8; i++)
			b[i] = (uint8_t)(ordinal >> (8 * i));
		goipc_reply_set(reply, SERVICE_WHOAMI, b, sizeof b);
		return;
	}
	}
	snprintf(text, sizeof text, "type %" PRIu32 " is not a request", type);
	goipc_reply_error(reply, text);
}

static void service_gone(void *ctx, goipc_session *s)
{
	(void)s;
	struct counter *c = ctx;
	pthread_mutex_lock(&c->mu);
	c->gone++;
	pthread_cond_broadcast(&c->cv);
	pthread_mutex_unlock(&c->mu);
}

static int role_service_serve(const char *name, uint64_t capacity, uint64_t clients)
{
	struct counter counter = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0};
	goipc_service *svc;
	int rc = goipc_service_serve(name, (size_t)capacity, service_answer, service_gone, &counter, &svc);
	if (rc != GOIPC_OK)
		return fail_rc("serve", rc);
	int status = ready();
	struct timespec deadline;
	clock_gettime(CLOCK_REALTIME, &deadline);
	deadline.tv_sec += PEER_TIMEOUT_NS / 1000000000;
	pthread_mutex_lock(&counter.mu);
	while (status == 0 && counter.gone < clients) {
		if (pthread_cond_timedwait(&counter.cv, &counter.mu, &deadline) == ETIMEDOUT)
			status = fail("client %" PRIu64 " of %" PRIu64 " never went", counter.gone, clients);
	}
	pthread_mutex_unlock(&counter.mu);
	if (status == 0 && (printf("ok %" PRIu64 "\n", clients) < 0 || fflush(stdout) != 0))
		status = fail("write ok: %s", strerror(errno));
	if ((rc = goipc_service_close(svc)) != GOIPC_OK && status == 0)
		status = fail_rc("close", rc);
	goipc_service_destroy(svc);
	return status;
}

static int role_service_call(const char *name, uint64_t count, enum stop_mode mode)
{
	goipc_client *c;
	int rc = goipc_client_connect(name, GOIPC_DEFAULT_CAPACITY, left_ns(), &c);
	if (rc != GOIPC_OK)
		return fail_rc("connect", rc);
	int status = 0;
	char want[32], boom[48], reply[64];
	uint32_t type;
	size_t len;
	for (uint64_t i = 0; status == 0 && i < count; i++) {
		int n = snprintf(want, sizeof want, "%" PRIu64, i);
		rc = goipc_client_call(c, SERVICE_ECHO, want, (size_t)n, left_ns(), &type, reply, sizeof reply, &len);
		if (rc != GOIPC_OK) {
			status = fail("call %" PRIu64 ": %s", i, goipc_strerror(rc));
			break;
		}
		if (type != SERVICE_ECHOED || len != (size_t)n || memcmp(reply, want, len) != 0) {
			status = fail("call %" PRIu64 ": reply type %" PRIu32 " payload %.*s, want type %d payload %s", i, type, (int)len, reply, SERVICE_ECHOED, want);
			break;
		}
		n = snprintf(boom, sizeof boom, "boom %" PRIu64, i);
		rc = goipc_client_call(c, SERVICE_FAIL, boom, (size_t)n, left_ns(), &type, reply, sizeof reply, &len);
		if (rc != GOIPC_ECALL) {
			status = fail("call %" PRIu64 ": fail returned %s, want a call error", i, goipc_strerror(rc));
			break;
		}
		if (len != (size_t)n || memcmp(reply, boom, len) != 0)
			status = fail("call %" PRIu64 ": error %.*s, want %s", i, (int)len, reply, boom);
	}
	if (status == 0) {
		rc = goipc_client_call(c, SERVICE_WHO, NULL, 0, left_ns(), &type, reply, sizeof reply, &len);
		if (rc != GOIPC_OK)
			status = fail("who: %s", goipc_strerror(rc));
		else {
			uint64_t got = 0;
			for (size_t i = 0; i < len && i < 8; i++)
				got |= (uint64_t)(uint8_t)reply[i] << (8 * i);
			if (type != SERVICE_WHOAMI || len != 8 || got != goipc_client_ordinal(c))
				status = fail("who: reply type %" PRIu32 " ordinal %" PRIu64 ", want ordinal %" PRIu64, type, got, goipc_client_ordinal(c));
		}
	}
	if (status == 0 && (printf("ok %" PRIu64 "\n", count) < 0 || fflush(stdout) != 0))
		status = fail("write ok: %s", strerror(errno));
	if (mode == STOP_EXIT)
		_exit(status);
	if ((rc = goipc_client_close(c)) != GOIPC_OK && status == 0)
		status = fail_rc("close", rc);
	goipc_client_destroy(c);
	return status;
}

static int role_send(const char *name, const char *sender, uint64_t count)
{
	goipc_queue *q;
	int rc = goipc_queue_open(name, &q);
	if (rc != GOIPC_OK)
		return fail_rc("open queue", rc);
	int status = 0;
	size_t cap = strlen(sender) + 32;
	char *payload = malloc(cap);
	if (payload == NULL)
		status = fail("out of memory");
	for (uint64_t i = 0; status == 0 && i < count; i++) {
		int n = snprintf(payload, cap, "%s:%" PRIu64, sender, i);
		rc = goipc_queue_send(q, (uint32_t)i, payload, (size_t)n, left_ns());
		if (rc != GOIPC_OK)
			status = fail_rc("send", rc);
	}
	if ((rc = goipc_queue_close(q)) != GOIPC_OK && status == 0)
		status = fail_rc("close", rc);
	goipc_queue_destroy(q);
	free(payload);
	return status;
}

static int role_listen_echo(const char *name, uint64_t capacity)
{
	goipc_conn *c;
	int rc = goipc_conn_listen(name, (size_t)capacity, &c);
	if (rc != GOIPC_OK)
		return fail_rc("listen", rc);
	int status = ready();
	static uint8_t buf[32 * 1024];
	while (status == 0) {
		size_t n, w;
		rc = goipc_conn_read(c, buf, sizeof buf, left_ns(), &n);
		if (rc == GOIPC_EOF)
			break;
		if (rc != GOIPC_OK) {
			status = fail_rc("read", rc);
			break;
		}
		rc = goipc_conn_write(c, buf, n, left_ns(), &w);
		if (rc != GOIPC_OK)
			status = fail_rc("write", rc);
	}
	if ((rc = goipc_conn_close(c)) != GOIPC_OK && status == 0)
		status = fail_rc("close", rc);
	if ((rc = goipc_conn_unlink(c)) != GOIPC_OK && status == 0)
		status = fail_rc("unlink", rc);
	goipc_conn_destroy(c);
	return status;
}

struct writer {
	goipc_conn *c;
	const uint8_t *data;
	size_t len;
	int rc;
};

static void *write_all(void *arg)
{
	struct writer *w = arg;
	size_t done;
	w->rc = goipc_conn_write(w->c, w->data, w->len, left_ns(), &done);
	if (w->rc == GOIPC_OK)
		w->rc = goipc_conn_close_write(w->c, left_ns());
	return NULL;
}

static int role_dial_check(const char *name, uint64_t nbytes)
{
	goipc_conn *c;
	int rc = goipc_conn_dial(name, &c);
	if (rc != GOIPC_OK)
		return fail_rc("dial", rc);
	int status = 0;
	size_t n = (size_t)nbytes;
	uint8_t *want = malloc(n + 1);
	uint8_t *got = malloc(n + 1);
	if (want == NULL || got == NULL) {
		status = fail("out of memory");
		goto out;
	}
	for (size_t i = 0; i < n; i++)
		want[i] = (uint8_t)(i * 31 + 7);

	/* The ring holds less than the stream, so writing and reading overlap. */
	struct writer w = {c, want, n, GOIPC_OK};
	pthread_t th;
	int err = pthread_create(&th, NULL, write_all, &w);
	if (err != 0) {
		status = fail("start writer: %s", strerror(err));
		goto out;
	}
	/* The writer half-closes when it is done, and the echo side answers
	 * with its own end-of-stream. One spare byte catches an overlong echo. */
	size_t have = 0;
	for (;;) {
		size_t r;
		rc = goipc_conn_read(c, got + have, n + 1 - have, left_ns(), &r);
		if (rc == GOIPC_EOF)
			break;
		if (rc != GOIPC_OK) {
			status = fail_rc("read echo", rc);
			break;
		}
		have += r;
		if (have > n) {
			status = fail("echo returned more than %zu bytes", n);
			break;
		}
	}
	if (status != 0)
		goipc_conn_close(c);
	pthread_join(th, NULL);
	if (status != 0)
		goto out;
	if (w.rc != GOIPC_OK) {
		status = fail_rc("write", w.rc);
		goto out;
	}
	if (have != n) {
		status = fail("echo returned %zu bytes, want %zu", have, n);
		goto out;
	}
	for (size_t i = 0; i < n; i++) {
		if (got[i] != want[i]) {
			status = fail("echo differs at byte %zu: got %u, want %u", i, got[i], want[i]);
			goto out;
		}
	}
out:
	rc = goipc_conn_close(c);
	if (rc != GOIPC_OK && rc != GOIPC_ECLOSED && status == 0)
		status = fail_rc("close", rc);
	goipc_conn_destroy(c);
	free(want);
	free(got);
	return status;
}

/* FX_CASE in role_typed_send encodes one values.json entry and sends it. */
#define FX_CASE(idx, T)                                                                                       \
	if (status == 0) {                                                                                    \
		T v;                                                                                          \
		fx_build_##idx(&v);                                                                           \
		size_t n = T##_size(&v);                                                                      \
		uint8_t *buf = malloc(n > 0 ? n : 1);                                                         \
		if (buf == NULL)                                                                              \
			status = fail("out of memory");                                                       \
		else if (T##_encode(&v, buf, n) != n)                                                         \
			status = fail("entry %d: " #T "_encode does not write %zu bytes", idx, n);            \
		else if ((rc = goipc_queue_send(q, T##_peer_type, buf, n, left_ns())) != GOIPC_OK)            \
			status = fail_rc("send entry " #idx, rc);                                             \
		free(buf);                                                                                    \
	}

static int role_typed_send(const char *name)
{
	goipc_queue *q;
	int rc = goipc_queue_open(name, &q);
	if (rc != GOIPC_OK)
		return fail_rc("open queue", rc);
	int status = 0;
	FX_CASES
	if ((rc = goipc_queue_close(q)) != GOIPC_OK && status == 0)
		status = fail_rc("close", rc);
	goipc_queue_destroy(q);
	return status;
}
#undef FX_CASE

/* reencode decodes in with the decoder for type, then encodes the result into *out. */
static int reencode(uint32_t type, const uint8_t *in, size_t len, uint8_t **out, size_t *out_len)
{
	switch (type) {
#define PEER_REENCODE(T, id)                                                                                  \
	case id: {                                                                                            \
		T m;                                                                                          \
		int rc = T##_decode(&m, in, len);                                                             \
		if (rc != 0)                                                                                  \
			return fail(#T "_decode fails with %d", rc);                                          \
		size_t n = T##_size(&m);                                                                      \
		*out = malloc(n > 0 ? n : 1);                                                                 \
		if (*out == NULL)                                                                             \
			return fail("out of memory");                                                         \
		*out_len = T##_encode(&m, *out, n);                                                           \
		if (*out_len != n)                                                                            \
			return fail(#T "_encode does not write %zu bytes", n);                                \
		return 0;                                                                                     \
	}
		PEER_MESSAGES(PEER_REENCODE)
#undef PEER_REENCODE
	}
	return fail("no message has type ID %" PRIu32, type);
}

static int read_file(const char *path, uint8_t **out, size_t *len)
{
	FILE *f = fopen(path, "rb");
	if (f == NULL)
		return fail("open %s: %s", path, strerror(errno));
	size_t cap = 256, n = 0;
	uint8_t *buf = malloc(cap);
	while (buf != NULL) {
		n += fread(buf + n, 1, cap - n, f);
		if (n < cap)
			break;
		uint8_t *grown = realloc(buf, cap * 2);
		if (grown == NULL)
			free(buf);
		buf = grown;
		cap *= 2;
	}
	int err = ferror(f);
	fclose(f);
	if (buf == NULL)
		return fail("out of memory");
	if (err) {
		free(buf);
		return fail("read %s", path);
	}
	*out = buf;
	*len = n;
	return 0;
}

/* typed_entry receives entry idx and requires its re-encoding to equal <idx>.bin. */
static int typed_entry(goipc_queue *q, uint8_t *buf, size_t cap, const char *spec, int idx, uint32_t want_type)
{
	uint32_t type;
	size_t len;
	int rc = goipc_queue_recv(q, buf, cap, &type, &len, left_ns());
	if (rc != GOIPC_OK)
		return fail("entry %d: %s", idx, goipc_strerror(rc));
	if (type != want_type)
		return fail("entry %d: type %" PRIu32 ", want %" PRIu32, idx, type, want_type);
	char path[4096];
	snprintf(path, sizeof path, "%s/vectors/schema/%d.bin", spec, idx);
	uint8_t *want = NULL, *got = NULL;
	size_t want_len = 0, got_len = 0;
	int status = read_file(path, &want, &want_len);
	if (status == 0 && reencode(type, buf, len, &got, &got_len) != 0)
		status = fail("entry %d: cannot re-encode", idx);
	if (status == 0 && (got_len != want_len || (want_len > 0 && memcmp(got, want, want_len) != 0)))
		status = fail("entry %d: re-encoding differs from %s", idx, path);
	free(want);
	free(got);
	return status;
}

#define FX_CASE(idx, T)                                                                                       \
	if (status == 0) {                                                                                    \
		status = typed_entry(q, buf, cap, spec, idx, T##_peer_type);                                  \
		count++;                                                                                      \
	}

static int role_typed_recv(const char *name, uint64_t capacity)
{
	const char *spec = getenv("GOIPC_SPEC_DIR");
	if (spec == NULL || *spec == '\0')
		return fail("GOIPC_SPEC_DIR is not set; it must name the spec directory");
	goipc_queue *q;
	int rc = goipc_queue_create(name, (size_t)capacity, &q);
	if (rc != GOIPC_OK)
		return fail_rc("create queue", rc);
	int status = ready();
	size_t cap = goipc_queue_max_message_size(q);
	uint8_t *buf = malloc(cap > 0 ? cap : 1);
	int count = 0;
	if (buf == NULL)
		status = fail("out of memory");
	FX_CASES
	if (status == 0 && (printf("ok %d\n", count) < 0 || fflush(stdout) != 0))
		status = fail("write ok: %s", strerror(errno));
	if ((rc = goipc_queue_close(q)) != GOIPC_OK && status == 0)
		status = fail_rc("close", rc);
	if ((rc = goipc_queue_unlink(q)) != GOIPC_OK && status == 0)
		status = fail_rc("unlink", rc);
	goipc_queue_destroy(q);
	free(buf);
	return status;
}
#undef FX_CASE

static int usage(const char *u)
{
	return fail("usage: %s", u);
}

int main(int argc, char **argv)
{
	clock_gettime(CLOCK_MONOTONIC, &started);
	if (argc < 2)
		return usage("goipc-peer <role> <args...>");
	const char *role = argv[1];
	uint64_t a, b;

	if (strcmp(role, "recv") == 0) {
		if (argc != 5)
			return usage("recv <name> <total> <capacity>");
		if (parse_u64(argv[3], SIZE_MAX, &a) != 0)
			return fail("total: %s is not a count", argv[3]);
		if (parse_u64(argv[4], SIZE_MAX, &b) != 0)
			return fail("capacity: %s is not a size", argv[4]);
		return role_recv(argv[2], a, b);
	}
	if (strcmp(role, "send") == 0) {
		if (argc != 5)
			return usage("send <name> <sender> <count>");
		if (parse_u64(argv[4], UINT32_MAX, &a) != 0)
			return fail("count: %s is not a count", argv[4]);
		return role_send(argv[2], argv[3], a);
	}
	if (strcmp(role, "listen-echo") == 0) {
		if (argc != 4)
			return usage("listen-echo <name> <capacity>");
		if (parse_u64(argv[3], SIZE_MAX, &a) != 0)
			return fail("capacity: %s is not a size", argv[3]);
		return role_listen_echo(argv[2], a);
	}
	if (strcmp(role, "dial-check") == 0) {
		if (argc != 4)
			return usage("dial-check <name> <bytes>");
		if (parse_u64(argv[3], SIZE_MAX - 1, &a) != 0)
			return fail("bytes: %s is not a size", argv[3]);
		return role_dial_check(argv[2], a);
	}
	if (strcmp(role, "typed-send") == 0) {
		if (argc != 3)
			return usage("typed-send <name>");
		return role_typed_send(argv[2]);
	}
	if (strcmp(role, "typed-recv") == 0) {
		if (argc != 4)
			return usage("typed-recv <name> <capacity>");
		if (parse_u64(argv[3], SIZE_MAX, &a) != 0)
			return fail("capacity: %s is not a size", argv[3]);
		return role_typed_recv(argv[2], a);
	}
	if (strcmp(role, "claim-and-die") == 0) {
		if (argc != 4)
			return usage("claim-and-die <name> <length>");
		if (parse_u64(argv[3], INT32_MAX, &a) != 0)
			return fail("length: %s is not a size", argv[3]);
		return role_claim_and_die(argv[2], a);
	}
	if (strcmp(role, "send-until-gone") == 0) {
		if (argc != 4)
			return usage("send-until-gone <name> <sender>");
		return role_send_until_gone(argv[2], argv[3]);
	}
	enum stop_mode mode;
	if (strcmp(role, "recv-then-stop") == 0) {
		if (argc != 6)
			return usage("recv-then-stop <name> <count> <capacity> <mode>");
		if (parse_u64(argv[3], SIZE_MAX, &a) != 0)
			return fail("count: %s is not a count", argv[3]);
		if (parse_u64(argv[4], SIZE_MAX, &b) != 0)
			return fail("capacity: %s is not a size", argv[4]);
		if (parse_mode(argv[5], &mode) != 0)
			return 1;
		return role_recv_then_stop(argv[2], a, b, mode);
	}
	if (strcmp(role, "chan-recv-until-gone") == 0) {
		if (argc != 5)
			return usage("chan-recv-until-gone <name> <capacity> <count>");
		if (parse_u64(argv[3], SIZE_MAX, &a) != 0)
			return fail("capacity: %s is not a size", argv[3]);
		if (parse_u64(argv[4], UINT32_MAX, &b) != 0)
			return fail("count: %s is not a count", argv[4]);
		return role_chan_recv_until_gone(argv[2], a, b);
	}
	if (strcmp(role, "chan-send-then-stop") == 0) {
		if (argc != 5)
			return usage("chan-send-then-stop <name> <count> <mode>");
		if (parse_u64(argv[3], UINT32_MAX, &a) != 0)
			return fail("count: %s is not a count", argv[3]);
		if (parse_mode(argv[4], &mode) != 0)
			return 1;
		return role_chan_send_then_stop(argv[2], a, mode);
	}
	if (strcmp(role, "service-serve") == 0) {
		if (argc != 5)
			return usage("service-serve <name> <capacity> <clients>");
		if (parse_u64(argv[3], SIZE_MAX, &a) != 0)
			return fail("capacity: %s is not a size", argv[3]);
		if (parse_u64(argv[4], UINT32_MAX, &b) != 0)
			return fail("clients: %s is not a count", argv[4]);
		return role_service_serve(argv[2], a, b);
	}
	if (strcmp(role, "service-call") == 0) {
		if (argc != 5)
			return usage("service-call <name> <count> <mode>");
		if (parse_u64(argv[3], UINT32_MAX, &a) != 0)
			return fail("count: %s is not a count", argv[3]);
		if (parse_mode(argv[4], &mode) != 0)
			return 1;
		return role_service_call(argv[2], a, mode);
	}
	return fail("unknown role %s", role);
}
