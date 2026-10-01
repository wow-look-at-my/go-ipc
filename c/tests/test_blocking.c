#define _GNU_SOURCE
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

#include "harness.h"

#define WINDOW_MS 1000
#define SENDERS 8

struct parked {
	int rc;
	goipc_queue *q;
};

static void *child_recv(void *arg)
{
	struct parked *p = arg;
	uint8_t buf[64];
	uint32_t type;
	size_t len;
	p->rc = goipc_queue_recv(p->q, buf, sizeof buf, &type, &len, MS(WINDOW_MS));
	return NULL;
}

static void *child_send(void *arg)
{
	struct parked *p = arg;
	static const uint8_t payload[504];
	p->rc = goipc_queue_send(p->q, 1, payload, sizeof payload, MS(WINDOW_MS));
	return NULL;
}

/* The child parks one receiver on an empty queue and several senders on a
 * full one for the whole window. It exits 0 only if every wait timed out. */
static int run_parked_child(const char *idle_name, const char *full_name)
{
	goipc_queue *idle, *full;
	if (goipc_queue_create(idle_name, 4096, &idle) != GOIPC_OK || goipc_queue_create(full_name, 4096, &full) != GOIPC_OK)
		return 2;
	static const uint8_t payload[504];
	while (goipc_queue_try_send(full, 1, payload, sizeof payload) == GOIPC_OK)
		;

	struct parked r = {99, idle};
	struct parked s[SENDERS];
	pthread_t tr, ts[SENDERS];
	if (pthread_create(&tr, NULL, child_recv, &r) != 0)
		return 3;
	for (int i = 0; i < SENDERS; i++) {
		s[i] = (struct parked){99, full};
		if (pthread_create(&ts[i], NULL, child_send, &s[i]) != 0)
			return 3;
	}
	pthread_join(tr, NULL);
	int status = r.rc == GOIPC_ETIMEDOUT ? 0 : 4;
	for (int i = 0; i < SENDERS; i++) {
		pthread_join(ts[i], NULL);
		if (s[i].rc != GOIPC_ETIMEDOUT)
			status = 5;
	}
	goipc_queue_close(idle);
	goipc_queue_unlink(idle);
	goipc_queue_close(full);
	goipc_queue_unlink(full);
	return status;
}

TEST(blocked_endpoints_consume_no_cpu, T_FORK)
{
	char idle[96], full[96];
	unique_name(idle, sizeof idle, "idle");
	unique_name(full, sizeof full, "full");
	int64_t start = now_ns();
	pid_t pid = fork();
	REQUIRE(pid >= 0);
	if (pid == 0)
		_exit(run_parked_child(idle, full));
	int status;
	struct rusage ru;
	REQUIRE(wait4(pid, &status, 0, &ru) == pid);
	int64_t wall = now_ns() - start;
	REQUIRE(WIFEXITED(status) && WEXITSTATUS(status) == 0, "child status %d", status);
	int64_t cpu = ((int64_t)ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) * 1000000000 +
		      ((int64_t)ru.ru_utime.tv_usec + ru.ru_stime.tv_usec) * 1000;
	CHECK(wall >= MS(WINDOW_MS), "child returned after %lld ms", (long long)(wall / 1000000));
	/* A spinning waiter burns a whole core; a parked one burns nearly nothing. */
	CHECK(cpu < MS(WINDOW_MS) / 10, "%d parked endpoints burned %lld ms of CPU in a %d ms window", SENDERS + 1,
	      (long long)(cpu / 1000000), WINDOW_MS);
}
