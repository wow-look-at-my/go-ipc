/* The procID of this process and its life socket. A watch on a process is a
 * connection to its life socket, which the kernel ends when that process
 * exits. One thread per process parks in poll() over the listener, the
 * accepted connections and every watch. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include "internal.h"

struct exit_cb {
	uint64_t key;
	goipc_exit_fn fn;
	void *arg;
	struct exit_cb *next;
};

struct watch {
	uint64_t id;
	int fd;
	/* A dead watch has no callbacks left. The life thread closes and frees it. */
	bool dead;
	struct exit_cb *cbs;
	struct watch *next;
};

static struct {
	pthread_mutex_t mu;
	bool init;
	bool atfork;
	uint64_t id;
	int err;
	int listen_fd;
	/* listen_paused drops the listener from the poll set after accept ran out
	 * of descriptors, so a pending connection cannot make poll() spin. */
	bool listen_paused;
	int wake_r, wake_w;
	int *conns;
	size_t nconns, capconns;
	struct watch *watches;
	uint64_t next_key;
} life = {
	.mu = PTHREAD_MUTEX_INITIALIZER,
	.listen_fd = -1,
	.wake_r = -1,
	.wake_w = -1,
};

char *goipc__life_path(uint64_t id)
{
	char hex[17];
	snprintf(hex, sizeof hex, "%016llx", (unsigned long long)id);
	return goipc__join(GOIPC_SHM_DIR "/" GOIPC_LIFE_PREFIX, hex, GOIPC_LIFE_SUFFIX);
}

static bool fill_addr(struct sockaddr_un *addr, const char *path)
{
	memset(addr, 0, sizeof *addr);
	addr->sun_family = AF_UNIX;
	if (strlen(path) >= sizeof addr->sun_path)
		return false;
	strcpy(addr->sun_path, path);
	return true;
}

enum dial_result { DIAL_OK, DIAL_GONE, DIAL_ERR };

/* dial connects to the life socket of id. ENOENT and ECONNREFUSED mean that no
 * process listens there. */
static enum dial_result dial(uint64_t id, int *fd_out, int *err)
{
	char *path = goipc__life_path(id);
	if (path == NULL) {
		*err = ENOMEM;
		return DIAL_ERR;
	}
	struct sockaddr_un addr;
	bool fits = fill_addr(&addr, path);
	free(path);
	if (!fits) {
		*err = ENAMETOOLONG;
		return DIAL_ERR;
	}
	int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (fd < 0) {
		*err = errno;
		return DIAL_ERR;
	}
	if (connect(fd, (struct sockaddr *)&addr, sizeof addr) != 0) {
		int e = errno;
		close(fd);
		if (e == ENOENT || e == ECONNREFUSED)
			return DIAL_GONE;
		*err = e;
		return DIAL_ERR;
	}
	if (fd_out != NULL)
		*fd_out = fd;
	else
		close(fd);
	return DIAL_OK;
}

bool goipc__is_dead(uint64_t id)
{
	if ((id & GOIPC_PROC_WATCHABLE) == 0)
		return false;
	int err;
	return dial(id, NULL, &err) == DIAL_GONE;
}

static int random_u64(uint64_t *out)
{
	uint8_t *p = (uint8_t *)out;
	size_t have = 0;
	while (have < sizeof *out) {
		ssize_t n = getrandom(p + have, sizeof *out - have, 0);
		if (n < 0) {
			if (errno == EINTR)
				continue;
			return errno;
		}
		have += (size_t)n;
	}
	return 0;
}

/* listen_life binds under a temporary name and renames into place. A bound
 * socket refuses a dial until it listens, and the sweep removes a socket
 * that refuses, but never one with a temporary name. */
static int listen_life(uint64_t id, int *out)
{
	char *path = goipc__life_path(id);
	char *tmp = path == NULL ? NULL : goipc__join(path, GOIPC_LIFE_TEMP_SUFFIX, "");
	struct sockaddr_un addr;
	int fd = -1, err = 0;
	if (tmp == NULL) {
		err = ENOMEM;
		goto out;
	}
	if (!fill_addr(&addr, tmp)) {
		err = ENAMETOOLONG;
		goto out;
	}
	fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
	if (fd < 0) {
		err = errno;
		goto out;
	}
	if (bind(fd, (struct sockaddr *)&addr, sizeof addr) != 0) {
		err = errno;
		goto out;
	}
	if (listen(fd, SOMAXCONN) != 0 || chmod(tmp, 0600) != 0 || rename(tmp, path) != 0) {
		err = errno;
		unlink(tmp);
		goto out;
	}
	*out = fd;
	fd = -1;
out:
	if (fd >= 0)
		close(fd);
	free(tmp);
	free(path);
	return err;
}

static void wake_thread(void)
{
	uint8_t b = 0;
	/* A failed write finds a full pipe, which already holds a wakeup. */
	if (life.wake_w >= 0 && write(life.wake_w, &b, 1) < 0)
		return;
}

static void close_conn(size_t i)
{
	close(life.conns[i]);
	life.conns[i] = life.conns[--life.nconns];
	life.listen_paused = false;
}

static void accept_all(void)
{
	for (;;) {
		int fd = accept4(life.listen_fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
		if (fd < 0) {
			if (errno == EINTR || errno == ECONNABORTED)
				continue;
			if (errno == EMFILE || errno == ENFILE || errno == ENOBUFS || errno == ENOMEM)
				life.listen_paused = true;
			return;
		}
		if (life.nconns == life.capconns) {
			size_t cap = life.capconns == 0 ? 16 : life.capconns * 2;
			int *grown = realloc(life.conns, cap * sizeof *grown);
			if (grown == NULL) {
				close(fd);
				life.listen_paused = true;
				return;
			}
			life.conns = grown;
			life.capconns = cap;
		}
		life.conns[life.nconns++] = fd;
	}
}

/* drained reads what fd holds and reports whether its connection ended. */
static bool drained(int fd, int *err)
{
	uint8_t buf[256];
	for (;;) {
		ssize_t n = read(fd, buf, sizeof buf);
		if (n > 0)
			continue;
		if (n == 0) {
			*err = 0;
			return true;
		}
		if (errno == EINTR)
			continue;
		if (errno == EAGAIN)
			return false;
		*err = errno;
		return true;
	}
}

static void fire(struct watch *w, int err)
{
	/* A failed read on a process that is gone is still an exit. */
	if (err != 0 && goipc__is_dead(w->id))
		err = 0;
	struct exit_cb *cb = w->cbs;
	w->cbs = NULL;
	w->dead = true;
	while (cb != NULL) {
		struct exit_cb *next = cb->next;
		cb->fn(cb->arg, w->id, err);
		free(cb);
		cb = next;
	}
}

static void purge_dead(void)
{
	struct watch **pp = &life.watches;
	while (*pp != NULL) {
		struct watch *w = *pp;
		if (w->dead) {
			*pp = w->next;
			close(w->fd);
			free(w);
			life.listen_paused = false;
		} else {
			pp = &w->next;
		}
	}
}

static void *life_thread(void *arg)
{
	(void)arg;
	struct pollfd *pfds = NULL;
	struct watch **polled = NULL;
	size_t cap = 0;

	for (;;) {
		pthread_mutex_lock(&life.mu);
		purge_dead();
		size_t nwatch = 0;
		for (struct watch *w = life.watches; w != NULL; w = w->next)
			nwatch++;
		size_t need = 2 + life.nconns + nwatch;
		if (need > cap) {
			struct pollfd *p = realloc(pfds, need * sizeof *p);
			if (p != NULL)
				pfds = p;
			struct watch **q = realloc(polled, need * sizeof *q);
			if (q != NULL)
				polled = q;
			if (p == NULL || q == NULL)
				need = 1;
			else
				cap = need;
		}
		/* Without memory for the full set, the thread parks on the wake pipe alone. */
		struct pollfd one;
		struct pollfd *set = need == 1 ? &one : pfds;
		size_t n = 0;
		set[n++] = (struct pollfd){.fd = life.wake_r, .events = POLLIN};
		size_t nconns = 0;
		if (need > 1) {
			pfds[n++] = (struct pollfd){.fd = life.listen_paused ? -1 : life.listen_fd, .events = POLLIN};
			for (size_t i = 0; i < life.nconns; i++)
				pfds[n++] = (struct pollfd){.fd = life.conns[i], .events = POLLIN};
			nconns = life.nconns;
			for (struct watch *w = life.watches; w != NULL; w = w->next) {
				polled[n] = w;
				pfds[n++] = (struct pollfd){.fd = w->fd, .events = POLLIN};
			}
		}
		pthread_mutex_unlock(&life.mu);

		if (poll(set, n, -1) < 0)
			continue;

		pthread_mutex_lock(&life.mu);
		if (set[0].revents != 0) {
			uint8_t buf[64];
			while (read(life.wake_r, buf, sizeof buf) > 0)
				;
		}
		if (n > 1) {
			if (pfds[1].revents != 0)
				accept_all();
			/* Only this thread changes conns, and accept_all appends,
			 * so the first nconns entries still match the poll set.
			 * The walk runs backward because close_conn moves the last
			 * entry into the freed place. */
			for (size_t i = nconns; i-- > 0;) {
				int err;
				if (pfds[2 + i].revents != 0 && drained(life.conns[i], &err))
					close_conn(i);
			}
			for (size_t i = 2 + nconns; i < n; i++) {
				struct watch *w = polled[i];
				int err;
				if (!w->dead && pfds[i].revents != 0 && drained(w->fd, &err))
					fire(w, err);
			}
		}
		pthread_mutex_unlock(&life.mu);
	}
	return NULL;
}

/* The child of a fork is another process. It must not keep this process's
 * listener or connections open, or this process would look alive after it
 * exits. The child builds an identity of its own on first use. */
static void atfork_prepare(void)
{
	pthread_mutex_lock(&life.mu);
}

static void atfork_parent(void)
{
	pthread_mutex_unlock(&life.mu);
}

static void atfork_child(void)
{
	pthread_mutex_init(&life.mu, NULL);
	if (!life.init)
		return;
	if (life.listen_fd >= 0)
		close(life.listen_fd);
	if (life.wake_r >= 0)
		close(life.wake_r);
	if (life.wake_w >= 0)
		close(life.wake_w);
	for (size_t i = 0; i < life.nconns; i++)
		close(life.conns[i]);
	for (struct watch *w = life.watches; w != NULL; w = w->next)
		close(w->fd);
	/* The parent owns that memory's other copy, so the child drops it. */
	life.conns = NULL;
	life.nconns = life.capconns = 0;
	life.watches = NULL;
	life.listen_fd = life.wake_r = life.wake_w = -1;
	life.listen_paused = false;
	life.id = 0;
	life.err = 0;
	life.init = false;
}

static int start_thread(void)
{
	pthread_attr_t attr;
	int err = pthread_attr_init(&attr);
	if (err != 0)
		return err;
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	/* Signals belong to the application's own threads. */
	sigset_t all, old;
	sigfillset(&all);
	pthread_sigmask(SIG_SETMASK, &all, &old);
	pthread_t th;
	err = pthread_create(&th, &attr, life_thread, NULL);
	pthread_sigmask(SIG_SETMASK, &old, NULL);
	pthread_attr_destroy(&attr);
	return err;
}

static void init_locked(void)
{
	if (life.init)
		return;
	life.init = true;
	if (!life.atfork) {
		pthread_atfork(atfork_prepare, atfork_parent, atfork_child);
		life.atfork = true;
	}
	uint64_t id = 0;
	int err = random_u64(&id);
	id |= GOIPC_PROC_WATCHABLE | GOIPC_PROC_ANY;
	int fd = -1;
	if (err == 0)
		err = listen_life(id, &fd);
	if (err == 0) {
		int p[2];
		if (pipe2(p, O_CLOEXEC | O_NONBLOCK) != 0) {
			err = errno;
		} else {
			life.wake_r = p[0];
			life.wake_w = p[1];
			life.listen_fd = fd;
			err = start_thread();
		}
		if (err != 0) {
			char *path = goipc__life_path(id);
			if (path != NULL)
				unlink(path);
			free(path);
			close(fd);
			if (life.wake_r >= 0) {
				close(life.wake_r);
				close(life.wake_w);
			}
			life.listen_fd = life.wake_r = life.wake_w = -1;
		}
	}
	if (err != 0)
		id &= ~GOIPC_PROC_WATCHABLE;
	life.id = id;
	life.err = err;
}

uint64_t goipc__self_id(void)
{
	pthread_mutex_lock(&life.mu);
	init_locked();
	uint64_t id = life.id;
	pthread_mutex_unlock(&life.mu);
	return id;
}

int goipc__self_errno(void)
{
	pthread_mutex_lock(&life.mu);
	init_locked();
	int err = life.err;
	pthread_mutex_unlock(&life.mu);
	return err;
}

int goipc__on_exit(uint64_t id, goipc_exit_fn fn, void *arg, uint64_t *key)
{
	if ((id & GOIPC_PROC_WATCHABLE) == 0)
		return GOIPC_EINVAL;
	struct exit_cb *cb = malloc(sizeof *cb);
	if (cb == NULL)
		return GOIPC_ENOMEM;

	pthread_mutex_lock(&life.mu);
	init_locked();
	int rc = GOIPC_OK;
	if (life.wake_w < 0) {
		rc = goipc__sys_errno(life.err != 0 ? life.err : ENOTSUP);
		goto out;
	}
	struct watch *w = life.watches;
	while (w != NULL && (w->dead || w->id != id))
		w = w->next;
	if (w == NULL) {
		int fd, err;
		switch (dial(id, &fd, &err)) {
		case DIAL_GONE:
			rc = GOIPC_EPEERGONE;
			goto out;
		case DIAL_ERR:
			rc = goipc__sys_errno(err);
			goto out;
		case DIAL_OK:
			break;
		}
		w = calloc(1, sizeof *w);
		if (w == NULL || fcntl(fd, F_SETFL, O_NONBLOCK) != 0) {
			rc = w == NULL ? GOIPC_ENOMEM : goipc__sys();
			free(w);
			close(fd);
			goto out;
		}
		w->id = id;
		w->fd = fd;
		w->next = life.watches;
		life.watches = w;
		wake_thread();
	}
	cb->key = ++life.next_key;
	cb->fn = fn;
	cb->arg = arg;
	cb->next = w->cbs;
	w->cbs = cb;
	*key = cb->key;
	cb = NULL;
out:
	pthread_mutex_unlock(&life.mu);
	free(cb);
	return rc;
}

void goipc__cancel_exit(uint64_t key)
{
	pthread_mutex_lock(&life.mu);
	for (struct watch *w = life.watches; w != NULL; w = w->next) {
		for (struct exit_cb **pp = &w->cbs; *pp != NULL; pp = &(*pp)->next) {
			if ((*pp)->key != key)
				continue;
			struct exit_cb *cb = *pp;
			*pp = cb->next;
			free(cb);
			if (w->cbs == NULL) {
				w->dead = true;
				wake_thread();
			}
			pthread_mutex_unlock(&life.mu);
			return;
		}
	}
	pthread_mutex_unlock(&life.mu);
}
