/* The service layer: a service in this process, its clients, and the service
 * cells of spec/peer.md driven through build/goipc-peer. */
#define _GNU_SOURCE
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

#include "harness.h"

enum {
	T_ECHO = 1,
	T_ECHOED = 2,
	T_FAIL = 3,
	T_WHO = 4,
	T_WHOAMI = 5,
	/* A park request is answered once the test releases the handler. */
	T_PARK = 6,
	T_PARKED = 7,
};

/* A fixture counts the clients that went and holds the park requests. */
struct fixture {
	pthread_mutex_t mu;
	pthread_cond_t cv;
	int gone;
	bool released;
};

#define FIXTURE_INIT {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0, false}

static void handle(void *ctx, goipc_session *s, uint32_t type, const uint8_t *payload, size_t len, goipc_reply *reply)
{
	struct fixture *f = ctx;
	switch (type) {
	case T_ECHO:
		goipc_reply_set(reply, T_ECHOED, payload, len);
		return;
	case T_FAIL: {
		char msg[128];
		snprintf(msg, sizeof msg, "%.*s", (int)len, (const char *)payload);
		goipc_reply_error(reply, msg);
		return;
	}
	case T_WHO: {
		uint64_t ordinal = goipc_session_ordinal(s);
		goipc_reply_set(reply, T_WHOAMI, &ordinal, sizeof ordinal);
		return;
	}
	case T_PARK:
		pthread_mutex_lock(&f->mu);
		while (!f->released)
			pthread_cond_wait(&f->cv, &f->mu);
		pthread_mutex_unlock(&f->mu);
		goipc_reply_set(reply, T_PARKED, NULL, 0);
		return;
	}
	goipc_reply_error(reply, "not a request");
}

static void handle_gone(void *ctx, goipc_session *s)
{
	(void)s;
	struct fixture *f = ctx;
	pthread_mutex_lock(&f->mu);
	f->gone++;
	pthread_cond_broadcast(&f->cv);
	pthread_mutex_unlock(&f->mu);
}

static void release(struct fixture *f)
{
	pthread_mutex_lock(&f->mu);
	f->released = true;
	pthread_cond_broadcast(&f->cv);
	pthread_mutex_unlock(&f->mu);
}

static int gone_count(struct fixture *f, int want)
{
	pthread_mutex_lock(&f->mu);
	while (f->gone < want)
		pthread_cond_wait(&f->cv, &f->mu);
	int n = f->gone;
	pthread_mutex_unlock(&f->mu);
	return n;
}

static goipc_service *serve(const char *name, struct fixture *f)
{
	goipc_service *svc;
	REQUIRE_RC(goipc_service_serve(name, 4096, handle, handle_gone, f, &svc), GOIPC_OK);
	return svc;
}

static goipc_client *connect_to(const char *name)
{
	goipc_client *c;
	REQUIRE_RC(goipc_client_connect(name, 4096, MS(5000), &c), GOIPC_OK);
	return c;
}

/* echo calls T_ECHO with text and checks the reply. */
static void echo(goipc_client *c, const char *text)
{
	char reply[128];
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_client_call(c, T_ECHO, text, strlen(text), MS(5000), &type, reply, sizeof reply, &len), GOIPC_OK);
	CHECK(type == T_ECHOED, "reply type %u", type);
	CHECK(len == strlen(text) && memcmp(reply, text, len) == 0, "reply %.*s, want %s", (int)len, reply, text);
}

TEST(service_answers_calls, T_THREADS)
{
	char name[96];
	unique_name(name, sizeof name, "svc");
	struct fixture f = FIXTURE_INIT;
	goipc_service *svc = serve(name, &f);
	goipc_client *c = connect_to(name);
	CHECK(goipc_client_ordinal(c) == 0, "ordinal %llu", (unsigned long long)goipc_client_ordinal(c));
	CHECK(goipc_client_max_payload_size(c) == 2040 - GOIPC_SERVICE_SEQUENCE_SIZE, "max payload %zu", goipc_client_max_payload_size(c));
	echo(c, "hello");
	echo(c, "");

	char reply[128];
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_client_call(c, T_FAIL, "boom", 4, MS(5000), &type, reply, sizeof reply, &len), GOIPC_ECALL);
	CHECK(len == 4 && memcmp(reply, "boom", 4) == 0, "error %.*s", (int)len, reply);
	REQUIRE_RC(goipc_client_call(c, T_FAIL, "boom", 4, MS(5000), &type, reply, 2, &len), GOIPC_ECALL);
	CHECK(len == 4 && memcmp(reply, "bo", 2) == 0, "cut error %.*s, length %zu", 2, reply, len);
	REQUIRE_RC(goipc_client_call(c, 99, NULL, 0, MS(5000), &type, reply, sizeof reply, &len), GOIPC_ECALL);
	CHECK(len == 13 && memcmp(reply, "not a request", 13) == 0, "error %.*s", (int)len, reply);

	REQUIRE_RC(goipc_client_call(c, T_ECHO, "hello", 5, MS(5000), &type, reply, 3, &len), GOIPC_EBUFFER);
	CHECK(len == 5, "needed %zu", len);
	echo(c, "after");

	REQUIRE_RC(goipc_client_close(c), GOIPC_OK);
	goipc_client_destroy(c);
	CHECK(gone_count(&f, 1) == 1);
	REQUIRE_RC(goipc_service_close(svc), GOIPC_OK);
	CHECK_RC(goipc_service_close(svc), GOIPC_ECLOSED);
	goipc_service_destroy(svc);
	char path[160];
	snprintf(path, sizeof path, "/dev/shm/go-ipc-%s.svc.name", name);
	CHECK(!path_exists(path), "close left %s behind", path);
}

TEST(service_gives_each_client_an_ordinal, T_THREADS)
{
	char name[96];
	unique_name(name, sizeof name, "svco");
	struct fixture f = FIXTURE_INIT;
	goipc_service *svc = serve(name, &f);
	goipc_client *cs[3];
	bool seen[3] = {false, false, false};
	for (int i = 0; i < 3; i++) {
		cs[i] = connect_to(name);
		uint64_t ordinal = 99;
		uint32_t type;
		size_t len;
		REQUIRE_RC(goipc_client_call(cs[i], T_WHO, NULL, 0, MS(5000), &type, &ordinal, sizeof ordinal, &len), GOIPC_OK);
		CHECK(type == T_WHOAMI && len == 8, "who reply type %u length %zu", type, len);
		CHECK(ordinal == goipc_client_ordinal(cs[i]), "service says %llu, client says %llu",
		      (unsigned long long)ordinal, (unsigned long long)goipc_client_ordinal(cs[i]));
		REQUIRE(ordinal < 3, "ordinal %llu", (unsigned long long)ordinal);
		CHECK(!seen[ordinal], "ordinal %llu twice", (unsigned long long)ordinal);
		seen[ordinal] = true;
	}
	for (int i = 0; i < 3; i++)
		goipc_client_destroy(cs[i]);
	CHECK(gone_count(&f, 3) == 3);
	goipc_service_destroy(svc);
}

struct waiter {
	const char *name;
	int64_t timeout_ns;
	goipc_client *c;
	int rc;
};

static void *connect_in_thread(void *arg)
{
	struct waiter *w = arg;
	w->rc = goipc_client_connect(w->name, 4096, w->timeout_ns, &w->c);
	return NULL;
}

/* A client that connects before the service exists is adopted when it
 * starts, whether the service finds its channel. In the scan or hears
 * its knock. */
TEST(service_client_waits_for_the_service, T_THREADS)
{
	char name[96];
	unique_name(name, sizeof name, "svcw");
	struct waiter w = {name, MS(5000), NULL, 99};
	pthread_t th;
	REQUIRE(pthread_create(&th, NULL, connect_in_thread, &w) == 0);
	struct fixture f = FIXTURE_INIT;
	goipc_service *svc = serve(name, &f);
	pthread_join(th, NULL);
	REQUIRE_RC(w.rc, GOIPC_OK);
	echo(w.c, "early");
	goipc_client_destroy(w.c);
	CHECK(gone_count(&f, 1) == 1);
	goipc_service_destroy(svc);
}

TEST(service_close_fails_every_call, T_THREADS)
{
	char name[96];
	unique_name(name, sizeof name, "svcc");
	struct fixture f = FIXTURE_INIT;
	goipc_service *svc = serve(name, &f);
	goipc_client *c = connect_to(name);
	echo(c, "before");
	REQUIRE_RC(goipc_service_close(svc), GOIPC_OK);
	char reply[16];
	uint32_t type;
	size_t len;
	CHECK_RC(goipc_client_call(c, T_ECHO, "x", 1, MS(5000), &type, reply, sizeof reply, &len), GOIPC_EPEERGONE);
	goipc_client_destroy(c);
	goipc_service_destroy(svc);
	goipc_client *again;
	CHECK_RC(goipc_client_connect(name, 4096, MS(50), &again), GOIPC_ETIMEDOUT);
}

TEST(service_name_is_held_while_it_runs, T_THREADS)
{
	char name[96];
	unique_name(name, sizeof name, "svch");
	struct fixture f = FIXTURE_INIT;
	goipc_service *svc = serve(name, &f);
	goipc_service *second;
	CHECK_RC(goipc_service_serve(name, 4096, handle, NULL, &f, &second), GOIPC_EINUSE);
	goipc_service_destroy(svc);
	second = serve(name, &f);
	goipc_service_destroy(second);
}

/* A reply to a call that timed out is discarded by the next call. */
TEST(service_client_discards_a_stale_reply, T_THREADS)
{
	char name[96];
	unique_name(name, sizeof name, "svcs");
	struct fixture f = FIXTURE_INIT;
	goipc_service *svc = serve(name, &f);
	goipc_client *c = connect_to(name);
	char reply[16];
	uint32_t type;
	size_t len;
	REQUIRE_RC(goipc_client_call(c, T_PARK, NULL, 0, MS(50), &type, reply, sizeof reply, &len), GOIPC_ETIMEDOUT);
	release(&f);
	echo(c, "fresh");
	goipc_client_destroy(c);
	goipc_service_destroy(svc);
}

TEST(service_rejects_bad_calls, T_THREADS)
{
	char name[96];
	unique_name(name, sizeof name, "svcb");
	struct fixture f = FIXTURE_INIT;
	goipc_service *svc = serve(name, &f);
	goipc_client *c = connect_to(name);
	char reply[16];
	uint32_t type;
	size_t len;
	CHECK_RC(goipc_client_call(c, GOIPC_SERVICE_TYPE_KNOCK, NULL, 0, MS(5000), &type, reply, sizeof reply, &len), GOIPC_ERESERVED);
	size_t big = goipc_client_max_payload_size(c) + 1;
	char *payload = calloc(big, 1);
	REQUIRE(payload != NULL);
	CHECK_RC(goipc_client_call(c, T_ECHO, payload, big, MS(5000), &type, reply, sizeof reply, &len), GOIPC_ETOOLARGE);
	free(payload);
	echo(c, "still fine");
	goipc_client_destroy(c);
	goipc_service_destroy(svc);

	goipc_service *bad;
	CHECK_RC(goipc_service_serve("a/b", 4096, handle, NULL, &f, &bad), GOIPC_EINVALNAME);
	CHECK_RC(goipc_service_serve(name, 4096, NULL, NULL, &f, &bad), GOIPC_EINVAL);
	goipc_client *nobody;
	CHECK_RC(goipc_client_connect("a/b", 4096, MS(50), &nobody), GOIPC_EINVALNAME);
}

/* A handler's reply set with a reserved type is refused at the handler. */
static void handle_reserved(void *ctx, goipc_session *s, uint32_t type, const uint8_t *payload, size_t len, goipc_reply *reply)
{
	(void)s;
	(void)type;
	(void)payload;
	(void)len;
	int *rc = ctx;
	*rc = goipc_reply_set(reply, GOIPC_SERVICE_TYPE_HELLO, NULL, 0);
}

TEST(service_handler_cannot_reply_with_a_reserved_type, T_THREADS)
{
	char name[96];
	unique_name(name, sizeof name, "svcr");
	int set_rc = GOIPC_OK;
	goipc_service *svc;
	REQUIRE_RC(goipc_service_serve(name, 4096, handle_reserved, NULL, &set_rc, &svc), GOIPC_OK);
	goipc_client *c = connect_to(name);
	char reply[64];
	uint32_t type;
	size_t len;
	CHECK_RC(goipc_client_call(c, T_ECHO, "x", 1, MS(5000), &type, reply, sizeof reply, &len), GOIPC_ECALL);
	CHECK(len == 31 && memcmp(reply, "goipc: the handler set no reply", 31) == 0, "error %.*s", (int)len, reply);
	CHECK_RC(set_rc, GOIPC_ERESERVED);
	goipc_client_destroy(c);
	goipc_service_destroy(svc);
}

/* ---- the service cells of spec/peer.md, through goipc-peer ---- */

struct proc {
	pid_t pid;
	int out;
};

static struct proc spawn_peer(const char *const argv[])
{
	int fds[2];
	REQUIRE(pipe2(fds, O_CLOEXEC) == 0);
	pid_t pid = fork();
	REQUIRE(pid >= 0);
	if (pid == 0) {
		dup2(fds[1], STDOUT_FILENO);
		execv(GOIPC_PEER_PATH, (char *const *)argv);
		_exit(127);
	}
	close(fds[1]);
	return (struct proc){pid, fds[0]};
}

static void read_line(struct proc *p, char *buf, size_t cap)
{
	size_t have = 0;
	while (have + 1 < cap) {
		ssize_t n = read(p->out, buf + have, 1);
		if (n <= 0)
			break;
		have++;
		if (buf[have - 1] == '\n')
			break;
	}
	buf[have] = '\0';
}

static int finish_peer(struct proc *p)
{
	close(p->out);
	int status;
	REQUIRE(waitpid(p->pid, &status, 0) == p->pid);
	return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
}

TEST(peer_service_cell, T_FORK)
{
	char name[96], line[64];
	unique_name(name, sizeof name, "peersvc");
	const char *serve_args[] = {"goipc-peer", "service-serve", name, "4096", "2", NULL};
	struct proc s = spawn_peer(serve_args);
	read_line(&s, line, sizeof line);
	REQUIRE(strcmp(line, "ready\n") == 0, "first line %s", line);
	const char *close_args[] = {"goipc-peer", "service-call", name, "200", "close", NULL};
	const char *exit_args[] = {"goipc-peer", "service-call", name, "200", "exit", NULL};
	struct proc a = spawn_peer(close_args);
	struct proc b = spawn_peer(exit_args);
	read_line(&a, line, sizeof line);
	CHECK(strcmp(line, "ok 200\n") == 0, "close caller printed %s", line);
	CHECK(finish_peer(&a) == 0, "close caller failed");
	read_line(&b, line, sizeof line);
	CHECK(strcmp(line, "ok 200\n") == 0, "exit caller printed %s", line);
	CHECK(finish_peer(&b) == 0, "exit caller failed");
	read_line(&s, line, sizeof line);
	CHECK(strcmp(line, "ok 2\n") == 0, "service-serve printed %s", line);
	CHECK(finish_peer(&s) == 0, "service-serve failed");
	char path[160];
	snprintf(path, sizeof path, "/dev/shm/go-ipc-%s.svc.name", name);
	CHECK(!path_exists(path), "service-serve left %s behind", path);
}

TEST(peer_service_early_cell, T_FORK)
{
	char name[96], line[64];
	unique_name(name, sizeof name, "peerearly");
	const char *call_args[] = {"goipc-peer", "service-call", name, "200", "close", NULL};
	struct proc c = spawn_peer(call_args);
	const char *serve_args[] = {"goipc-peer", "service-serve", name, "4096", "1", NULL};
	struct proc s = spawn_peer(serve_args);
	read_line(&s, line, sizeof line);
	REQUIRE(strcmp(line, "ready\n") == 0, "first line %s", line);
	read_line(&c, line, sizeof line);
	CHECK(strcmp(line, "ok 200\n") == 0, "caller printed %s", line);
	CHECK(finish_peer(&c) == 0, "caller failed");
	read_line(&s, line, sizeof line);
	CHECK(strcmp(line, "ok 1\n") == 0, "service-serve printed %s", line);
	CHECK(finish_peer(&s) == 0, "service-serve failed");
}

/* ---- a parked call costs no CPU ---- */

#define PARK_WINDOW_MS 300

static void handle_hold(void *ctx, goipc_session *s, uint32_t type, const uint8_t *payload, size_t len, goipc_reply *reply)
{
	(void)ctx;
	(void)s;
	(void)type;
	(void)payload;
	(void)len;
	/* The handler outlives the client's timeout, so the client parks the whole window. */
	struct timespec ts = {0, (long)PARK_WINDOW_MS * 2 * 1000000};
	nanosleep(&ts, NULL);
	goipc_reply_set(reply, T_PARKED, NULL, 0);
}

/* The child parks one client in a call, and another in a connect to a
 * service that never comes, for the whole window. It exits 0 only if both
 * waits timed out. */
static int run_parked_child(const char *name, const char *absent)
{
	goipc_service *svc;
	if (goipc_service_serve(name, 4096, handle_hold, NULL, NULL, &svc) != GOIPC_OK)
		return 2;
	goipc_client *c;
	if (goipc_client_connect(name, 4096, MS(5000), &c) != GOIPC_OK)
		return 3;
	struct waiter w = {absent, MS(PARK_WINDOW_MS), NULL, 99};
	pthread_t th;
	if (pthread_create(&th, NULL, connect_in_thread, &w) != 0)
		return 4;
	char reply[16];
	uint32_t type;
	size_t len;
	int rc = goipc_client_call(c, T_PARK, NULL, 0, MS(PARK_WINDOW_MS), &type, reply, sizeof reply, &len);
	int status = rc == GOIPC_ETIMEDOUT ? 0 : 5;
	goipc_client_destroy(c);
	goipc_service_destroy(svc);
	pthread_join(th, NULL);
	if (w.rc != GOIPC_ETIMEDOUT)
		status = 6;
	return status;
}

TEST(parked_call_consumes_no_cpu, T_FORK)
{
	char name[96], absent[96];
	unique_name(name, sizeof name, "svcpark");
	unique_name(absent, sizeof absent, "svcabsent");
	int64_t start = now_ns();
	pid_t pid = fork();
	REQUIRE(pid >= 0);
	if (pid == 0)
		_exit(run_parked_child(name, absent));
	int status;
	struct rusage ru;
	REQUIRE(wait4(pid, &status, 0, &ru) == pid);
	int64_t wall = now_ns() - start;
	REQUIRE(WIFEXITED(status) && WEXITSTATUS(status) == 0, "child status %d", status);
	int64_t cpu = ((int64_t)ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) * 1000000000 +
		      ((int64_t)ru.ru_utime.tv_usec + ru.ru_stime.tv_usec) * 1000;
	CHECK(wall >= MS(PARK_WINDOW_MS), "child returned after %lld ms", (long long)(wall / 1000000));
	CHECK(cpu < MS(PARK_WINDOW_MS) / 10, "a parked call and connect burned %lld ms of CPU in a %d ms window",
	      (long long)(cpu / 1000000), PARK_WINDOW_MS);
}
