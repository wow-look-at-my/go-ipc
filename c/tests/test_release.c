/* goipc_release: a process removes its own life socket before it exits. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include "harness.h"
#include "internal.h"

static bool read_full(int fd, void *buf, size_t len)
{
	uint8_t *p = buf;
	while (len > 0) {
		ssize_t n = read(fd, p, len);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			return false;
		p += n;
		len -= (size_t)n;
	}
	return true;
}

/* release_child reports its procID, releases on the first byte from in, says
 * so, and exits when in reaches end-of-file. */
static void release_child(int in, int out)
{
	/* Release before any procID makes none. */
	if (goipc_release() != GOIPC_OK)
		_exit(2);
	uint64_t id = goipc__self_id();
	uint8_t b;
	if (write(out, &id, sizeof id) != sizeof id || !read_full(in, &b, 1))
		_exit(3);
	if (goipc_release() != GOIPC_OK || goipc_release() != GOIPC_OK)
		_exit(4);
	if (write(out, &b, 1) != 1)
		_exit(5);
	while (read(in, &b, 1) > 0)
		;
	_exit(0);
}

static int dial_path(const char *path)
{
	struct sockaddr_un addr = {.sun_family = AF_UNIX};
	REQUIRE(strlen(path) < sizeof addr.sun_path);
	strcpy(addr.sun_path, path);
	int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
	REQUIRE(fd >= 0);
	REQUIRE(connect(fd, (struct sockaddr *)&addr, sizeof addr) == 0, "connect %s: %s", path, strerror(errno));
	return fd;
}

static void on_exit_fn(void *arg, uint64_t id, int err)
{
	(void)id;
	int *fds = arg;
	if (write(fds[1], &err, sizeof err) != sizeof err)
		abort();
}

TEST(release_removes_the_life_socket_and_keeps_watches, T_FORK)
{
	int to_child[2], from_child[2], fired[2];
	REQUIRE(pipe2(to_child, O_CLOEXEC) == 0 && pipe2(from_child, O_CLOEXEC) == 0 && pipe2(fired, O_CLOEXEC) == 0);
	pid_t pid = fork();
	REQUIRE(pid >= 0);
	if (pid == 0) {
		close(to_child[1]);
		close(from_child[0]);
		release_child(to_child[0], from_child[1]);
	}
	close(to_child[0]);
	close(from_child[1]);

	uint64_t id;
	REQUIRE(read_full(from_child[0], &id, sizeof id), "the child reported no procID");
	REQUIRE(id & GOIPC_PROC_WATCHABLE, "the child has no life socket");
	char *path = goipc__life_path(id);
	REQUIRE(path != NULL);

	/* Both watches start before the release. */
	int conn = dial_path(path);
	uint64_t key;
	REQUIRE_RC(goipc__on_exit(id, on_exit_fn, fired, &key), GOIPC_OK);

	uint8_t b = 1;
	REQUIRE(write(to_child[1], &b, 1) == 1);
	REQUIRE(read_full(from_child[0], &b, 1), "the child did not release");

	CHECK(!path_exists(path), "%s is still there after release", path);
	CHECK(goipc__is_dead(id), "a check after release must judge the process gone");
	/* The child still lives, so the earlier connection stays open. */
	struct pollfd p = {.fd = conn, .events = POLLIN};
	CHECK(poll(&p, 1, 100) == 0, "release ended a connection that a peer held");
	p = (struct pollfd){.fd = fired[0], .events = POLLIN};
	CHECK(poll(&p, 1, 0) == 0, "the watch fired at release, not at exit");

	/* The exit ends the connection and fires the watch. */
	close(to_child[1]);
	int status;
	REQUIRE(waitpid(pid, &status, 0) == pid);
	CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0, "child status %d", status);
	CHECK(read(conn, &b, 1) == 0, "the connection did not end at the exit");
	int err = -1;
	REQUIRE(read_full(fired[0], &err, sizeof err), "the watch did not fire");
	CHECK(err == 0, "the watch failed with errno %d", err);

	close(conn);
	close(from_child[0]);
	close(fired[0]);
	close(fired[1]);
	free(path);
}
