#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "harness.h"
#include "internal.h"

static goipc_event *new_event(char *name, size_t len)
{
	unique_name(name, len, "ev");
	goipc_event *e;
	REQUIRE_RC(goipc_event_create(name, &e), GOIPC_OK);
	return e;
}

static void drop_event(goipc_event *e)
{
	goipc_event_unlink(e);
	goipc_event_destroy(e);
}

static void pause_ms(int ms)
{
	struct timespec ts = {0, (long)ms * 1000000};
	nanosleep(&ts, NULL);
}

TEST(event_rejects_bad_names, 0)
{
	goipc_event *e;
	const char *bad[] = {"", "a/b", "a\\b", ".", ".."};
	for (size_t i = 0; i < sizeof bad / sizeof bad[0]; i++) {
		CHECK_RC(goipc_event_create(bad[i], &e), GOIPC_EINVALNAME);
		CHECK_RC(goipc_event_open(bad[i], &e), GOIPC_EINVALNAME);
	}
	CHECK_RC(goipc_event_create(NULL, &e), GOIPC_EINVALNAME);
}

TEST(event_file_is_a_private_fifo, 0)
{
	char name[96];
	goipc_event *e = new_event(name, sizeof name);
	char *path = goipc__event_path(name);
	struct stat st;
	REQUIRE(stat(path, &st) == 0);
	CHECK(S_ISFIFO(st.st_mode));
	CHECK((st.st_mode & 0077) == 0, "mode %o", st.st_mode & 0777);
	REQUIRE_RC(goipc_event_unlink(e), GOIPC_OK);
	CHECK(!path_exists(path));
	CHECK_RC(goipc_event_unlink(e), GOIPC_OK);
	goipc_event_destroy(e);
	free(path);
}

TEST(event_open_refuses_missing_and_regular_files, 0)
{
	char name[96];
	unique_name(name, sizeof name, "ev");
	goipc_event *e;
	REQUIRE_RC(goipc_event_open(name, &e), GOIPC_ESYS);
	CHECK(goipc_last_errno() == ENOENT, "errno %d", goipc_last_errno());

	char *path = goipc__event_path(name);
	int fd = open(path, O_CREAT | O_WRONLY, 0600);
	REQUIRE(fd >= 0);
	close(fd);
	CHECK_RC(goipc_event_open(name, &e), GOIPC_EINVAL);
	unlink(path);
	free(path);
}

TEST(event_signal_before_wait_is_kept, 0)
{
	char name[96];
	goipc_event *e = new_event(name, sizeof name);
	REQUIRE_RC(goipc_event_signal(e, 1), GOIPC_OK);
	REQUIRE_RC(goipc_event_wait(e, 0), GOIPC_OK);
	REQUIRE_RC(goipc_event_wait(e, 0), GOIPC_ETIMEDOUT);
	REQUIRE_RC(goipc_event_signal(e, 0), GOIPC_OK);
	REQUIRE_RC(goipc_event_wait(e, 0), GOIPC_ETIMEDOUT);
	REQUIRE_RC(goipc_event_signal(e, -1), GOIPC_EINVAL);
	drop_event(e);
}

TEST(event_timeout_consumes_nothing, 0)
{
	char name[96];
	goipc_event *e = new_event(name, sizeof name);
	int64_t start = now_ns();
	REQUIRE_RC(goipc_event_wait(e, MS(50)), GOIPC_ETIMEDOUT);
	int64_t spent = now_ns() - start;
	CHECK(spent >= MS(50), "returned after %lld ns", (long long)spent);
	CHECK(spent < MS(2000), "returned after %lld ns", (long long)spent);
	REQUIRE_RC(goipc_event_signal(e, 2), GOIPC_OK);
	REQUIRE_RC(goipc_event_wait(e, MS(10)), GOIPC_OK);
	REQUIRE_RC(goipc_event_wait(e, MS(10)), GOIPC_OK);
	REQUIRE_RC(goipc_event_wait(e, MS(10)), GOIPC_ETIMEDOUT);
	drop_event(e);
}

TEST(event_signal_caps_at_pipe_buf, 0)
{
	char name[96];
	goipc_event *e = new_event(name, sizeof name);
	REQUIRE_RC(goipc_event_signal(e, 10000), GOIPC_OK);
	char *path = goipc__event_path(name);
	int fd = open(path, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
	REQUIRE(fd >= 0);
	int pending = 0;
	REQUIRE(ioctl(fd, FIONREAD, &pending) == 0);
	CHECK(pending == GOIPC_SIGNAL_MAX_TOKENS, "pending %d", pending);
	/* A full pipe takes no more, and the signal still succeeds. */
	REQUIRE_RC(goipc_event_signal(e, 1), GOIPC_OK);
	close(fd);
	free(path);
	drop_event(e);
}

struct waiter {
	goipc_event *e;
	int64_t timeout;
	int rc;
};

static void *wait_thread(void *arg)
{
	struct waiter *w = arg;
	w->rc = goipc_event_wait(w->e, w->timeout);
	return NULL;
}

TEST(event_signal_n_wakes_n_waiters, T_THREADS)
{
	char name[96];
	goipc_event *e = new_event(name, sizeof name);
	enum { N = 6 };
	pthread_t th[N];
	struct waiter w[N];
	for (int i = 0; i < N; i++) {
		w[i] = (struct waiter){e, MS(10000), 99};
		REQUIRE(pthread_create(&th[i], NULL, wait_thread, &w[i]) == 0);
	}
	pause_ms(20);
	REQUIRE_RC(goipc_event_signal(e, N), GOIPC_OK);
	for (int i = 0; i < N; i++) {
		pthread_join(th[i], NULL);
		CHECK_RC(w[i].rc, GOIPC_OK);
	}
	REQUIRE_RC(goipc_event_wait(e, 0), GOIPC_ETIMEDOUT);
	drop_event(e);
}

TEST(event_close_releases_blocked_waiters, T_THREADS)
{
	char name[96];
	goipc_event *e = new_event(name, sizeof name);
	enum { N = 4 };
	pthread_t th[N];
	struct waiter w[N];
	for (int i = 0; i < N; i++) {
		w[i] = (struct waiter){e, -1, 99};
		REQUIRE(pthread_create(&th[i], NULL, wait_thread, &w[i]) == 0);
	}
	pause_ms(50);
	REQUIRE_RC(goipc_event_close(e), GOIPC_OK);
	for (int i = 0; i < N; i++) {
		pthread_join(th[i], NULL);
		CHECK_RC(w[i].rc, GOIPC_ECLOSED);
	}
	CHECK_RC(goipc_event_close(e), GOIPC_ECLOSED);
	CHECK_RC(goipc_event_wait(e, 0), GOIPC_ECLOSED);
	CHECK_RC(goipc_event_signal(e, 1), GOIPC_ECLOSED);
	drop_event(e);
}

TEST(event_wakes_across_processes, T_FORK)
{
	char name[96];
	goipc_event *e = new_event(name, sizeof name);
	char back_name[128];
	snprintf(back_name, sizeof back_name, "%s.back", name);
	goipc_event *back;
	REQUIRE_RC(goipc_event_create(back_name, &back), GOIPC_OK);

	pid_t pid = fork();
	REQUIRE(pid >= 0);
	if (pid == 0) {
		goipc_event *ce, *cb;
		if (goipc_event_open(name, &ce) != GOIPC_OK || goipc_event_open(back_name, &cb) != GOIPC_OK)
			_exit(2);
		if (goipc_event_wait(ce, MS(10000)) != GOIPC_OK)
			_exit(3);
		if (goipc_event_signal(cb, 1) != GOIPC_OK)
			_exit(4);
		goipc_event_destroy(ce);
		goipc_event_destroy(cb);
		_exit(0);
	}
	pause_ms(20);
	REQUIRE_RC(goipc_event_signal(e, 1), GOIPC_OK);
	CHECK_RC(goipc_event_wait(back, MS(10000)), GOIPC_OK);
	int status;
	REQUIRE(waitpid(pid, &status, 0) == pid);
	CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0, "child status %d", status);
	drop_event(e);
	drop_event(back);
}
