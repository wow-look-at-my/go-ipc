/* Queue names, instances and the sweep of stale names. A name file carries the
 * creator's flock. The .inc file holds the id of the current instance. */
#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include "internal.h"

char *goipc__name_path(const char *name)
{
	return goipc__join(GOIPC_SHM_DIR "/" GOIPC_NAME_PREFIX, name, GOIPC_NAME_SUFFIX);
}

char *goipc__inc_path(const char *name)
{
	return goipc__join(GOIPC_SHM_DIR "/" GOIPC_NAME_PREFIX, name, GOIPC_INC_SUFFIX);
}

char *goipc__instance_name(const char *name, const char *inc)
{
	return goipc__join(name, ".", inc);
}

/* ---- held locks ---- */

/* The fork child gets a copy of every held lock descriptor. Each copy holds
 * the flock until it closes. The child points them at /dev/null, so the lock
 * ends with the process that took it, and the descriptor numbers stay valid. */
static struct {
	pthread_mutex_t mu;
	pthread_once_t once;
	int *fds;
	size_t n, cap;
} held = {.mu = PTHREAD_MUTEX_INITIALIZER, .once = PTHREAD_ONCE_INIT};

static void held_prepare(void)
{
	pthread_mutex_lock(&held.mu);
}

static void held_parent(void)
{
	pthread_mutex_unlock(&held.mu);
}

static void held_child(void)
{
	pthread_mutex_init(&held.mu, NULL);
	if (held.n == 0)
		return;
	int null = open("/dev/null", O_RDONLY | O_CLOEXEC);
	if (null < 0)
		return;
	for (size_t i = 0; i < held.n; i++)
		dup3(null, held.fds[i], O_CLOEXEC);
	close(null);
	held.n = 0;
}

static void held_register_atfork(void)
{
	pthread_atfork(held_prepare, held_parent, held_child);
}

static int hold(int fd)
{
	pthread_once(&held.once, held_register_atfork);
	pthread_mutex_lock(&held.mu);
	int rc = GOIPC_OK;
	if (held.n == held.cap) {
		size_t cap = held.cap == 0 ? 8 : held.cap * 2;
		int *grown = realloc(held.fds, cap * sizeof *grown);
		if (grown == NULL)
			rc = GOIPC_ENOMEM;
		else {
			held.fds = grown;
			held.cap = cap;
		}
	}
	if (rc == GOIPC_OK)
		held.fds[held.n++] = fd;
	pthread_mutex_unlock(&held.mu);
	return rc;
}

static void unhold(int fd)
{
	pthread_mutex_lock(&held.mu);
	for (size_t i = 0; i < held.n; i++) {
		if (held.fds[i] == fd) {
			held.fds[i] = held.fds[--held.n];
			break;
		}
	}
	pthread_mutex_unlock(&held.mu);
}

/* ---- the name lock ---- */

int goipc__lock_name(const char *name, int *out)
{
	char *path = goipc__name_path(name);
	if (path == NULL)
		return GOIPC_ENOMEM;
	int rc;
	for (;;) {
		int fd = open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
		if (fd < 0) {
			rc = goipc__sys();
			break;
		}
		if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
			int e = errno;
			close(fd);
			if (e == EINTR)
				continue;
			rc = e == EWOULDBLOCK ? GOIPC_EINUSE : goipc__sys_errno(e);
			break;
		}
		/* Another process can remove or replace the file between the open and the flock. */
		struct stat held_st, now_st;
		if (fstat(fd, &held_st) == 0 && stat(path, &now_st) == 0 && held_st.st_dev == now_st.st_dev &&
		    held_st.st_ino == now_st.st_ino) {
			rc = hold(fd);
			if (rc != GOIPC_OK) {
				close(fd);
				break;
			}
			*out = fd;
			break;
		}
		close(fd);
	}
	free(path);
	return rc;
}

void goipc__release_name(int fd)
{
	unhold(fd);
	close(fd);
}

/* ---- instance ids ---- */

static bool parse_inc(const char *content, size_t len, char *inc)
{
	if (len != GOIPC_INC_LEN)
		return false;
	for (size_t i = 0; i < len; i++) {
		char c = content[i];
		if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')))
			return false;
	}
	memcpy(inc, content, len);
	inc[len] = '\0';
	return true;
}

/* read_inc reads the .inc file of name. A missing file leaves inc empty and
 * is not an error. */
static int read_inc(const char *name, char *inc)
{
	inc[0] = '\0';
	char *path = goipc__inc_path(name);
	if (path == NULL)
		return GOIPC_ENOMEM;
	int fd = open(path, O_RDONLY | O_CLOEXEC);
	free(path);
	if (fd < 0)
		return errno == ENOENT ? GOIPC_OK : goipc__sys();
	char buf[GOIPC_INC_LEN + 2];
	size_t have = 0;
	int rc = GOIPC_OK;
	while (have < sizeof buf) {
		ssize_t n = read(fd, buf + have, sizeof buf - have);
		if (n < 0) {
			if (errno == EINTR)
				continue;
			rc = goipc__sys();
			break;
		}
		if (n == 0)
			break;
		have += (size_t)n;
	}
	close(fd);
	if (rc == GOIPC_OK)
		parse_inc(buf, have, inc);
	return rc;
}

void goipc__previous_inc(const char *name, char *inc)
{
	if (read_inc(name, inc) != GOIPC_OK)
		inc[0] = '\0';
}

int goipc__new_inc(char *inc)
{
	uint8_t raw[GOIPC_INC_LEN / 2];
	size_t have = 0;
	while (have < sizeof raw) {
		ssize_t n = getrandom(raw + have, sizeof raw - have, 0);
		if (n < 0) {
			if (errno == EINTR)
				continue;
			return goipc__sys();
		}
		have += (size_t)n;
	}
	static const char digits[] = "0123456789abcdef";
	for (size_t i = 0; i < sizeof raw; i++) {
		inc[2 * i] = digits[raw[i] >> 4];
		inc[2 * i + 1] = digits[raw[i] & 15];
	}
	inc[GOIPC_INC_LEN] = '\0';
	return GOIPC_OK;
}

int goipc__publish_inc(const char *name, const char *inc)
{
	char *path = goipc__inc_path(name);
	if (path == NULL)
		return GOIPC_ENOMEM;
	int fd = open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
	free(path);
	if (fd < 0)
		return goipc__sys();
	int rc = GOIPC_OK;
	if (pwrite(fd, inc, GOIPC_INC_LEN, 0) != GOIPC_INC_LEN || ftruncate(fd, GOIPC_INC_LEN) != 0)
		rc = goipc__sys();
	close(fd);
	return rc;
}

/* A name file with no instance id yet is reported as ENOENT: the instance
 * does not exist. */
int goipc__read_name(const char *name, char *inc)
{
	char *path = goipc__name_path(name);
	if (path == NULL)
		return GOIPC_ENOMEM;
	struct stat st;
	int r = stat(path, &st);
	free(path);
	if (r != 0 && errno == ENOENT)
		return goipc__sys();
	int rc = read_inc(name, inc);
	if (rc != GOIPC_OK)
		return rc;
	if (inc[0] == '\0')
		return goipc__sys_errno(ENOENT);
	return GOIPC_OK;
}

static int remove_path(const char *path)
{
	if (path == NULL)
		return GOIPC_ENOMEM;
	if (unlink(path) != 0 && errno != ENOENT)
		return goipc__sys();
	return GOIPC_OK;
}

static int remove_name(const char *name)
{
	char *inc = goipc__inc_path(name);
	char *nm = goipc__name_path(name);
	int rc = remove_path(inc);
	if (rc == GOIPC_OK)
		rc = remove_path(nm);
	free(inc);
	free(nm);
	return rc;
}

int goipc__unlink_name(const char *name, const char *inc)
{
	char current[GOIPC_INC_LEN + 1];
	int rc = read_inc(name, current);
	if (rc != GOIPC_OK)
		return rc;
	/* A newer instance under the same name keeps its files. */
	if (strcmp(current, inc) != 0)
		return GOIPC_OK;
	return remove_name(name);
}

int goipc__remove_instance(const char *name, const char *inc)
{
	char *inst = goipc__instance_name(name, inc);
	if (inst == NULL)
		return GOIPC_ENOMEM;
	char *seg = goipc__segment_path(inst);
	char *ne = goipc__join(inst, GOIPC_NOT_EMPTY_SUFFIX, "");
	char *nf = goipc__join(inst, GOIPC_NOT_FULL_SUFFIX, "");
	int rc = remove_path(seg);
	int r2 = ne == NULL ? GOIPC_ENOMEM : goipc__event_unlink_name(ne);
	int r3 = nf == NULL ? GOIPC_ENOMEM : goipc__event_unlink_name(nf);
	if (rc == GOIPC_OK)
		rc = r2;
	if (rc == GOIPC_OK)
		rc = r3;
	free(seg);
	free(ne);
	free(nf);
	free(inst);
	return rc;
}

/* ---- the sweep ---- */

static bool has_suffix(const char *s, const char *suffix, size_t *base)
{
	size_t ls = strlen(s), lx = strlen(suffix);
	if (ls < lx || strcmp(s + ls - lx, suffix) != 0)
		return false;
	*base = ls - lx;
	return true;
}

/* sweep_life removes the life socket of a process that has exited. Nothing
 * listens there, so a dial is refused. */
static void sweep_life(const char *path)
{
	struct sockaddr_un addr;
	memset(&addr, 0, sizeof addr);
	addr.sun_family = AF_UNIX;
	if (strlen(path) >= sizeof addr.sun_path)
		return;
	strcpy(addr.sun_path, path);
	int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (fd < 0)
		return;
	if (connect(fd, (struct sockaddr *)&addr, sizeof addr) != 0 && errno == ECONNREFUSED)
		unlink(path);
	close(fd);
}

/* sweep_name removes a name that no live process holds. A failure costs this
 * caller nothing, and the next sweep tries again. */
static void sweep_name(const char *name)
{
	int fd;
	if (goipc__lock_name(name, &fd) != GOIPC_OK)
		return;
	char inc[GOIPC_INC_LEN + 1];
	goipc__previous_inc(name, inc);
	if (inc[0] == '\0' || goipc__remove_instance(name, inc) == GOIPC_OK)
		remove_name(name);
	goipc__release_name(fd);
}

void goipc__sweep_dir(const char *dir)
{
	DIR *d = opendir(dir);
	if (d == NULL)
		return;
	/* The sweep removes entries, so it lists the directory first. */
	char **names = NULL;
	size_t n = 0, cap = 0;
	struct dirent *e;
	while ((e = readdir(d)) != NULL) {
		if (strncmp(e->d_name, GOIPC_NAME_PREFIX, strlen(GOIPC_NAME_PREFIX)) != 0)
			continue;
		if (n == cap) {
			size_t c = cap == 0 ? 32 : cap * 2;
			char **grown = realloc(names, c * sizeof *grown);
			if (grown == NULL)
				break;
			names = grown;
			cap = c;
		}
		names[n] = strdup(e->d_name);
		if (names[n] != NULL)
			n++;
	}
	closedir(d);

	for (size_t i = 0; i < n; i++) {
		const char *entry = names[i];
		size_t base;
		if (strncmp(entry, GOIPC_LIFE_PREFIX, strlen(GOIPC_LIFE_PREFIX)) == 0 && has_suffix(entry, GOIPC_LIFE_SUFFIX, &base)) {
			char *path = goipc__join(dir, "/", entry);
			if (path != NULL)
				sweep_life(path);
			free(path);
			continue;
		}
		const char *rest = entry + strlen(GOIPC_NAME_PREFIX);
		if (!has_suffix(rest, GOIPC_NAME_SUFFIX, &base) && !has_suffix(rest, GOIPC_INC_SUFFIX, &base))
			continue;
		char *name = strndup(rest, base);
		if (name != NULL && goipc__validate_name(name) == GOIPC_OK)
			sweep_name(name);
		free(name);
	}
	for (size_t i = 0; i < n; i++)
		free(names[i]);
	free(names);
}

static pthread_once_t sweep_once = PTHREAD_ONCE_INIT;

static void sweep_runtime_dir(void)
{
	goipc__sweep_dir(GOIPC_SHM_DIR);
}

void goipc__sweep(void)
{
	pthread_once(&sweep_once, sweep_runtime_dir);
}
