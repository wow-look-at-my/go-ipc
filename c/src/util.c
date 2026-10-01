#define _GNU_SOURCE
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "internal.h"

static _Thread_local int last_errno;

int goipc_last_errno(void)
{
	return last_errno;
}

int goipc__sys(void)
{
	last_errno = errno;
	return GOIPC_ESYS;
}

int goipc__sys_errno(int e)
{
	last_errno = e;
	return GOIPC_ESYS;
}

const char *goipc_strerror(int err)
{
	switch (err) {
	case GOIPC_OK: return "goipc: success";
	case GOIPC_ECLOSED: return "goipc: endpoint is closed";
	case GOIPC_EFULL: return "goipc: ring is full";
	case GOIPC_EEMPTY: return "goipc: ring is empty";
	case GOIPC_ETOOLARGE: return "goipc: message exceeds the maximum size";
	case GOIPC_ERESERVED: return "goipc: message type is reserved";
	case GOIPC_EINVALNAME: return "goipc: name must not be empty or contain a separator";
	case GOIPC_EINVALCAP: return "goipc: capacity must be a power of two of at least 4096 bytes";
	case GOIPC_ETOOSMALL: return "goipc: buffer is too small to hold a ring";
	case GOIPC_EBADLAYOUT: return "goipc: buffer does not hold a compatible ring";
	case GOIPC_ECORRUPT: return "goipc: ring contents are corrupt";
	case GOIPC_EUNALIGNED: return "goipc: buffer is not 8-byte aligned";
	case GOIPC_ETIMEDOUT: return "goipc: timed out";
	case GOIPC_ESYS: return "goipc: system call failed";
	case GOIPC_ENOMEM: return "goipc: out of memory";
	case GOIPC_EINVAL: return "goipc: invalid argument";
	case GOIPC_EBUFFER: return "goipc: receive buffer is smaller than the next message";
	case GOIPC_EOF: return "goipc: end of stream";
	case GOIPC_EPEERGONE: return "goipc: peer is gone";
	case GOIPC_EINUSE: return "goipc: name is in use";
	case GOIPC_ENOTCONSUMER: return "goipc: handle is not the receiving end";
	case GOIPC_ETOOMANYCLAIMS: return "goipc: too many claims in progress";
	default: return "goipc: unknown error";
	}
}

int goipc__validate_name(const char *name)
{
	if (name == NULL || name[0] == '\0')
		return GOIPC_EINVALNAME;
	if (strchr(name, '/') != NULL || strchr(name, '\\') != NULL)
		return GOIPC_EINVALNAME;
	if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0)
		return GOIPC_EINVALNAME;
	return GOIPC_OK;
}

char *goipc__join(const char *a, const char *b, const char *c)
{
	size_t la = strlen(a), lb = strlen(b), lc = strlen(c);
	char *s = malloc(la + lb + lc + 1);
	if (s == NULL)
		return NULL;
	memcpy(s, a, la);
	memcpy(s + la, b, lb);
	memcpy(s + la + lb, c, lc + 1);
	return s;
}

char *goipc__segment_path(const char *name)
{
	return goipc__join(GOIPC_SHM_DIR "/" GOIPC_SEGMENT_PREFIX, name, "");
}

char *goipc__event_path(const char *name)
{
	return goipc__join(GOIPC_SHM_DIR "/" GOIPC_EVENT_PREFIX, name, GOIPC_EVENT_SUFFIX);
}

const struct timespec *goipc__deadline(int64_t timeout_ns, struct timespec *out)
{
	if (timeout_ns < 0)
		return NULL;
	clock_gettime(CLOCK_MONOTONIC, out);
	int64_t sec = timeout_ns / 1000000000;
	int64_t nsec = timeout_ns % 1000000000 + out->tv_nsec;
	if (nsec >= 1000000000) {
		nsec -= 1000000000;
		sec++;
	}
	out->tv_sec += (time_t)sec;
	out->tv_nsec = (long)nsec;
	return out;
}

void goipc__remaining(const struct timespec *deadline, struct timespec *out)
{
	struct timespec now;
	clock_gettime(CLOCK_MONOTONIC, &now);
	int64_t ns = ((int64_t)deadline->tv_sec - (int64_t)now.tv_sec) * 1000000000 + (deadline->tv_nsec - now.tv_nsec);
	if (ns < 0)
		ns = 0;
	out->tv_sec = (time_t)(ns / 1000000000);
	out->tv_nsec = (long)(ns % 1000000000);
}

int goipc__gate_init(struct goipc_gate *g)
{
	atomic_init(&g->closing, false);
	atomic_init(&g->active, 0);
	int rc = pthread_mutex_init(&g->mu, NULL);
	if (rc != 0)
		return goipc__sys_errno(rc);
	rc = pthread_cond_init(&g->cv, NULL);
	if (rc != 0) {
		pthread_mutex_destroy(&g->mu);
		return goipc__sys_errno(rc);
	}
	return GOIPC_OK;
}

void goipc__gate_destroy(struct goipc_gate *g)
{
	pthread_cond_destroy(&g->cv);
	pthread_mutex_destroy(&g->mu);
}

/* The increment here and the store in gate_close are both seq_cst, so either
 * the close waits for this operation or this operation backs out. */
bool goipc__gate_enter(struct goipc_gate *g)
{
	atomic_fetch_add(&g->active, 1);
	if (atomic_load(&g->closing)) {
		goipc__gate_leave(g);
		return false;
	}
	return true;
}

void goipc__gate_leave(struct goipc_gate *g)
{
	if (atomic_fetch_sub(&g->active, 1) == 1 && atomic_load(&g->closing)) {
		pthread_mutex_lock(&g->mu);
		pthread_cond_broadcast(&g->cv);
		pthread_mutex_unlock(&g->mu);
	}
}

bool goipc__gate_close(struct goipc_gate *g)
{
	bool expected = false;
	return atomic_compare_exchange_strong(&g->closing, &expected, true);
}

void goipc__gate_drain(struct goipc_gate *g)
{
	pthread_mutex_lock(&g->mu);
	while (atomic_load(&g->active) > 0)
		pthread_cond_wait(&g->cv, &g->mu);
	pthread_mutex_unlock(&g->mu);
}

bool goipc__gate_closed(struct goipc_gate *g)
{
	return atomic_load(&g->closing);
}
