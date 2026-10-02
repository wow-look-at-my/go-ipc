/* The system calls that Linux and macOS spell differently, and the runtime
 * directory of each. Everything else in the library is plain POSIX. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <unistd.h>

#include "internal.h"

/* ---- the runtime directory ---- */

#ifdef __linux__

const char *goipc_runtime_dir(void)
{
	return "/dev/shm";
}

#else

/* macOS has no /dev/shm. Its per-user temporary directory is shared by every
 * process of that user, which is the scope a queue needs, and it is where the
 * Go package puts its files on that host. Trailing slashes are dropped so the
 * paths here match the ones Go's filepath.Join produces. */
static pthread_once_t dir_once = PTHREAD_ONCE_INIT;
static char *dir_value;

static void init_dir(void)
{
	const char *env = getenv("TMPDIR");
	if (env == NULL || env[0] == '\0')
		env = "/tmp";
	size_t n = strlen(env);
	while (n > 1 && env[n - 1] == '/')
		n--;
	dir_value = strndup(env, n);
}

const char *goipc_runtime_dir(void)
{
	pthread_once(&dir_once, init_dir);
	/* Without memory for the copy, the default stands in: the process is
	 * about to fail its allocations anyway. */
	return dir_value != NULL ? dir_value : "/tmp";
}

#endif

char *goipc__runtime_path(const char *prefix, const char *name, const char *suffix)
{
	char *head = goipc__join(goipc_runtime_dir(), "/", prefix);
	if (head == NULL)
		return NULL;
	char *path = goipc__join(head, name, suffix);
	free(head);
	return path;
}

/* ---- random bytes ---- */

int goipc__random(void *buf, size_t len)
{
	uint8_t *p = buf;
	size_t have = 0;
	while (have < len) {
#ifdef __linux__
		ssize_t n = getrandom(p + have, len - have, 0);
		if (n < 0) {
			if (errno == EINTR)
				continue;
			return errno;
		}
		have += (size_t)n;
#else
		/* getentropy takes at most 256 bytes per call. */
		size_t chunk = len - have < 256 ? len - have : 256;
		if (getentropy(p + have, chunk) != 0)
			return errno;
		have += chunk;
#endif
	}
	return 0;
}

/* ---- descriptors with their flags set at creation ---- */

#ifndef __linux__
/* set_flags adds close-on-exec and, when asked, non-blocking to fd. It
 * returns 0 or -1 with errno set, and closes fd on failure. */
static int set_flags(int fd, bool nonblock)
{
	if (fcntl(fd, F_SETFD, FD_CLOEXEC) == 0 && (!nonblock || fcntl(fd, F_SETFL, O_NONBLOCK) == 0))
		return 0;
	int e = errno;
	close(fd);
	errno = e;
	return -1;
}
#endif

int goipc__pipe(int fd[2])
{
#ifdef __linux__
	return pipe2(fd, O_CLOEXEC | O_NONBLOCK) == 0 ? 0 : errno;
#else
	if (pipe(fd) != 0)
		return errno;
	if (set_flags(fd[0], true) != 0) {
		int e = errno;
		close(fd[1]);
		return e;
	}
	if (set_flags(fd[1], true) != 0) {
		int e = errno;
		close(fd[0]);
		return e;
	}
	return 0;
#endif
}

int goipc__socket(bool nonblock)
{
#ifdef __linux__
	return socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | (nonblock ? SOCK_NONBLOCK : 0), 0);
#else
	int fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0)
		return -1;
	return set_flags(fd, nonblock) == 0 ? fd : -1;
#endif
}

int goipc__accept(int listen_fd)
{
#ifdef __linux__
	return accept4(listen_fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
	int fd = accept(listen_fd, NULL, NULL);
	if (fd < 0)
		return -1;
	return set_flags(fd, true) == 0 ? fd : -1;
#endif
}

int goipc__dup_cloexec(int from, int to)
{
#ifdef __linux__
	return dup3(from, to, O_CLOEXEC);
#else
	if (dup2(from, to) < 0)
		return -1;
	return fcntl(to, F_SETFD, FD_CLOEXEC) == 0 ? to : -1;
#endif
}

/* ---- a poll bounded by a deadline ---- */

int goipc__poll(struct pollfd *fds, nfds_t nfds, const struct timespec *deadline)
{
	if (deadline == NULL)
		return poll(fds, nfds, -1);
	struct timespec left;
	goipc__remaining(deadline, &left);
#ifdef __linux__
	return ppoll(fds, nfds, &left, NULL);
#else
	/* poll counts milliseconds. The wait rounds up, so a timeout never
	 * fires before the deadline. */
	int64_t ms = (int64_t)left.tv_sec * 1000 + (left.tv_nsec + 999999) / 1000000;
	if (ms > INT_MAX)
		ms = INT_MAX;
	return poll(fds, nfds, (int)ms);
#endif
}
