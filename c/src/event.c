#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "internal.h"

/* An event is a FIFO opened read-write and non-blocking. Waiters poll it
 * together with a close pipe. Close writes one byte to that pipe and never
 * drains it, so every present and future poller wakes. */
struct goipc_event {
	char *path;
	int fd;
	int close_r;
	int close_w;
	struct goipc_gate gate;
};

static const uint8_t signal_tokens[GOIPC_SIGNAL_MAX_TOKENS];

static int open_event(char *path, goipc_event **out)
{
	int fd = open(path, O_RDWR | O_NONBLOCK | O_CLOEXEC);
	if (fd < 0) {
		int rc = goipc__sys();
		free(path);
		return rc;
	}
	struct stat st;
	if (fstat(fd, &st) != 0) {
		int rc = goipc__sys();
		close(fd);
		free(path);
		return rc;
	}
	/* A regular file polls readable forever and reads end-of-file, so a
	 * waiter on it would spin. */
	if (!S_ISFIFO(st.st_mode)) {
		close(fd);
		free(path);
		return GOIPC_EINVAL;
	}
	int pipefd[2];
	int err = goipc__pipe(pipefd);
	if (err != 0) {
		int rc = goipc__sys_errno(err);
		close(fd);
		free(path);
		return rc;
	}
	goipc_event *e = calloc(1, sizeof *e);
	if (e == NULL) {
		close(pipefd[0]);
		close(pipefd[1]);
		close(fd);
		free(path);
		return GOIPC_ENOMEM;
	}
	int rc = goipc__gate_init(&e->gate);
	if (rc != GOIPC_OK) {
		free(e);
		close(pipefd[0]);
		close(pipefd[1]);
		close(fd);
		free(path);
		return rc;
	}
	e->path = path;
	e->fd = fd;
	e->close_r = pipefd[0];
	e->close_w = pipefd[1];
	*out = e;
	return GOIPC_OK;
}

int goipc_event_create(const char *name, goipc_event **out)
{
	int rc = goipc__validate_name(name);
	if (rc != GOIPC_OK)
		return rc;
	char *path = goipc__event_path(name);
	if (path == NULL)
		return GOIPC_ENOMEM;
	if (unlink(path) != 0 && errno != ENOENT) {
		rc = goipc__sys();
		free(path);
		return rc;
	}
	if (mkfifo(path, 0600) != 0 && errno != EEXIST) {
		rc = goipc__sys();
		free(path);
		return rc;
	}
	return open_event(path, out);
}

int goipc_event_open(const char *name, goipc_event **out)
{
	int rc = goipc__validate_name(name);
	if (rc != GOIPC_OK)
		return rc;
	char *path = goipc__event_path(name);
	if (path == NULL)
		return GOIPC_ENOMEM;
	return open_event(path, out);
}

int goipc_event_signal(goipc_event *e, int n)
{
	if (n < 0)
		return GOIPC_EINVAL;
	if (n == 0)
		return GOIPC_OK;
	if (n > GOIPC_SIGNAL_MAX_TOKENS)
		n = GOIPC_SIGNAL_MAX_TOKENS;
	if (!goipc__gate_enter(&e->gate))
		return GOIPC_ECLOSED;
	int rc = GOIPC_OK;
	for (;;) {
		if (write(e->fd, signal_tokens, (size_t)n) >= 0)
			break;
		if (errno == EINTR)
			continue;
		/* A full pipe already holds more wakeups than there are waiters. */
		if (errno != EAGAIN)
			rc = goipc__sys();
		break;
	}
	goipc__gate_leave(&e->gate);
	return rc;
}

static int wait_locked(goipc_event *e, const struct timespec *deadline)
{
	for (;;) {
		struct pollfd pfd[2] = {
			{.fd = e->fd, .events = POLLIN},
			{.fd = e->close_r, .events = POLLIN},
		};
		int n = goipc__poll(pfd, 2, deadline);
		if (n < 0) {
			if (errno == EINTR)
				continue;
			return goipc__sys();
		}
		if (pfd[1].revents != 0)
			return GOIPC_ECLOSED;
		if (n == 0)
			return GOIPC_ETIMEDOUT;
		if ((pfd[0].revents & POLLIN) == 0)
			return goipc__sys_errno(EIO);
		uint8_t b;
		ssize_t got = read(e->fd, &b, 1);
		if (got == 1)
			return GOIPC_OK;
		if (got == 0)
			return goipc__sys_errno(EIO);
		/* EAGAIN means another waiter took the byte, so poll again. */
		if (errno != EAGAIN && errno != EINTR)
			return goipc__sys();
	}
}

int goipc__event_wait_until(goipc_event *e, const struct timespec *deadline)
{
	if (!goipc__gate_enter(&e->gate))
		return GOIPC_ECLOSED;
	int rc = wait_locked(e, deadline);
	goipc__gate_leave(&e->gate);
	return rc;
}

int goipc_event_wait(goipc_event *e, int64_t timeout_ns)
{
	struct timespec ts;
	return goipc__event_wait_until(e, goipc__deadline(timeout_ns, &ts));
}

int goipc_event_close(goipc_event *e)
{
	if (!goipc__gate_close(&e->gate))
		return GOIPC_ECLOSED;
	uint8_t b = 0;
	ssize_t w;
	do
		w = write(e->close_w, &b, 1);
	while (w < 0 && errno == EINTR);
	if (w != 1)
		return goipc__sys();
	goipc__gate_drain(&e->gate);

	int rc = GOIPC_OK;
	if (close(e->fd) != 0)
		rc = goipc__sys();
	if (close(e->close_r) != 0 && rc == GOIPC_OK)
		rc = goipc__sys();
	if (close(e->close_w) != 0 && rc == GOIPC_OK)
		rc = goipc__sys();
	return rc;
}

int goipc__event_unlink_name(const char *name)
{
	char *path = goipc__event_path(name);
	if (path == NULL)
		return GOIPC_ENOMEM;
	int rc = GOIPC_OK;
	if (unlink(path) != 0 && errno != ENOENT)
		rc = goipc__sys();
	free(path);
	return rc;
}

int goipc_event_unlink(goipc_event *e)
{
	if (unlink(e->path) != 0 && errno != ENOENT)
		return goipc__sys();
	return GOIPC_OK;
}

void goipc_event_destroy(goipc_event *e)
{
	if (e == NULL)
		return;
	if (!goipc__gate_closed(&e->gate))
		goipc_event_close(e);
	goipc__gate_destroy(&e->gate);
	free(e->path);
	free(e);
}
