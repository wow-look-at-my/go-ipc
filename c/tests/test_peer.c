/* Drives build/goipc-peer through the roles of spec/peer.md, and checks
 * what the shared library exports. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "harness.h"

struct proc {
	pid_t pid;
	int out;
};

static struct proc spawn_fd(const char *const argv[], int target)
{
	int fds[2];
	REQUIRE(pipe2(fds, O_CLOEXEC) == 0);
	pid_t pid = fork();
	REQUIRE(pid >= 0);
	if (pid == 0) {
		dup2(fds[1], target);
		execv(GOIPC_PEER_PATH, (char *const *)argv);
		_exit(127);
	}
	close(fds[1]);
	return (struct proc){pid, fds[0]};
}

static struct proc spawn(const char *const argv[])
{
	return spawn_fd(argv, STDOUT_FILENO);
}

/* read_output returns what the process wrote, up to and including the first
 * newline when line is set, or to end-of-file otherwise. */
static void read_output(struct proc *p, char *buf, size_t cap, bool line)
{
	size_t have = 0;
	while (have + 1 < cap) {
		ssize_t n = read(p->out, buf + have, 1);
		if (n <= 0)
			break;
		have++;
		if (line && buf[have - 1] == '\n')
			break;
	}
	buf[have] = '\0';
}

static int finish(struct proc *p)
{
	close(p->out);
	int status;
	REQUIRE(waitpid(p->pid, &status, 0) == p->pid);
	return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
}

TEST(peer_recv_and_send_roles, T_FORK)
{
	char name[96];
	unique_name(name, sizeof name, "peerq");
	const char *recv_args[] = {"goipc-peer", "recv", name, "3000", "4096", NULL};
	struct proc r = spawn(recv_args);
	char line[64];
	read_output(&r, line, sizeof line, true);
	REQUIRE(strcmp(line, "ready\n") == 0, "first line %s", line);

	struct proc s[3];
	const char *ids[] = {"a", "b", "c"};
	for (int i = 0; i < 3; i++) {
		const char *send_args[] = {"goipc-peer", "send", name, ids[i], "1000", NULL};
		s[i] = spawn(send_args);
	}
	for (int i = 0; i < 3; i++)
		CHECK(finish(&s[i]) == 0, "sender %s failed", ids[i]);
	read_output(&r, line, sizeof line, false);
	CHECK(strcmp(line, "ok 3000\n") == 0, "recv printed %s", line);
	REQUIRE(finish(&r) == 0, "recv failed");
	char path[160];
	snprintf(path, sizeof path, "/dev/shm/go-shm-%s", name);
	CHECK(!path_exists(path), "recv left %s behind", path);
}

TEST(peer_listen_echo_and_dial_check_roles, T_FORK)
{
	char name[96];
	unique_name(name, sizeof name, "peerc");
	const char *listen_args[] = {"goipc-peer", "listen-echo", name, "4096", NULL};
	struct proc l = spawn(listen_args);
	char line[64];
	read_output(&l, line, sizeof line, true);
	REQUIRE(strcmp(line, "ready\n") == 0, "first line %s", line);
	const char *dial_args[] = {"goipc-peer", "dial-check", name, "100000", NULL};
	struct proc d = spawn(dial_args);
	CHECK(finish(&d) == 0, "dial-check failed");
	CHECK(finish(&l) == 0, "listen-echo failed");
	char path[160];
	snprintf(path, sizeof path, "/dev/shm/go-shm-%s.c2o", name);
	CHECK(!path_exists(path), "listen-echo left %s behind", path);
}

TEST(peer_fails_on_bad_usage, T_FORK)
{
	const char *no_role[] = {"goipc-peer", NULL};
	const char *unknown[] = {"goipc-peer", "fly", NULL};
	const char *short_args[] = {"goipc-peer", "recv", "x", NULL};
	const char *bad_number[] = {"goipc-peer", "send", "x", "a", "ten", NULL};
	char name[96];
	unique_name(name, sizeof name, "absent");
	const char *missing[] = {"goipc-peer", "send", name, "a", "1", NULL};
	const char *const *cases[] = {no_role, unknown, short_args, bad_number, missing};
	for (size_t i = 0; i < sizeof cases / sizeof cases[0]; i++) {
		struct proc p = spawn_fd(cases[i], STDERR_FILENO);
		char reason[256];
		read_output(&p, reason, sizeof reason, false);
		CHECK(strncmp(reason, "goipc-peer: ", 12) == 0, "case %zu printed %s", i, reason);
		CHECK(finish(&p) == 1, "case %zu did not exit 1", i);
	}
}

TEST(shared_library_exports_only_the_api, 0)
{
	void *h = dlopen(GOIPC_SHARED_LIB, RTLD_NOW | RTLD_LOCAL);
	REQUIRE(h != NULL, "dlopen: %s", dlerror());
	const char *api[] = {"goipc_strerror", "goipc_last_errno", "goipc_ring_init", "goipc_queue_create",
			     "goipc_conn_close_write", "goipc_channel_open", "goipc_event_wait"};
	for (size_t i = 0; i < sizeof api / sizeof api[0]; i++)
		CHECK(dlsym(h, api[i]) != NULL, "%s is not exported", api[i]);
	CHECK(dlsym(h, "goipc__join") == NULL, "an internal symbol is exported");
	dlclose(h);
}
