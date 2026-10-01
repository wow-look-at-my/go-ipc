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
	return fail("unknown role %s", role);
}
