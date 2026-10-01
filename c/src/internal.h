/* Shared internals of libgoipc. Not part of the public API. */
#ifndef GOIPC_INTERNAL_H
#define GOIPC_INTERNAL_H

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

/* The library builds with hidden visibility. Only the public API exports. */
#pragma GCC visibility push(default)
#include "goipc.h"
#pragma GCC visibility pop

#define GOIPC_CACHE_LINE 128u
#define GOIPC_RECORD_ALIGNMENT 8u
#define GOIPC_SIGNAL_MAX_TOKENS 4096
#define GOIPC_SHM_DIR "/dev/shm"
#define GOIPC_SEGMENT_PREFIX "go-shm-"
#define GOIPC_EVENT_PREFIX "go-ipc-"
#define GOIPC_EVENT_SUFFIX ".event"
#define GOIPC_NOT_EMPTY_SUFFIX ".ne"
#define GOIPC_NOT_FULL_SUFFIX ".nf"
#define GOIPC_C2O_SUFFIX ".c2o"
#define GOIPC_O2C_SUFFIX ".o2c"

/* The ring control block. Its layout is the wire format of spec/README.md. */
struct goipc_ring_hdr {
	_Atomic uint64_t magic;
	uint32_t version;
	uint32_t flags;
	uint64_t capacity;
	uint8_t pad0[GOIPC_CACHE_LINE - 24];
	_Atomic uint64_t tail;
	uint8_t pad1[GOIPC_CACHE_LINE - 8];
	_Atomic uint64_t head;
	uint8_t pad2[GOIPC_CACHE_LINE - 8];
	_Atomic uint64_t head_cache;
	_Atomic int32_t recv_waiters;
	_Atomic int32_t send_waiters;
	uint8_t pad3[GOIPC_CACHE_LINE - 16];
};

_Static_assert(sizeof(struct goipc_ring_hdr) == GOIPC_HEADER_SIZE, "ring header size drifted");
_Static_assert(sizeof(_Atomic uint64_t) == 8, "atomic u64 must be lock-free size");
_Static_assert(sizeof(_Atomic int32_t) == 4, "atomic i32 must be plain size");

static inline struct goipc_ring_hdr *goipc__hdr(const goipc_ring *r)
{
	return (struct goipc_ring_hdr *)r->hdr;
}

/* goipc__sys saves errno for goipc_last_errno and returns GOIPC_ESYS. */
int goipc__sys(void);
/* goipc__sys_errno saves e as the errno and returns GOIPC_ESYS. */
int goipc__sys_errno(int e);

int goipc__validate_name(const char *name);
/* goipc__join returns a malloc copy of a, b and c joined, or NULL. */
char *goipc__join(const char *a, const char *b, const char *c);
char *goipc__segment_path(const char *name);
char *goipc__event_path(const char *name);

/* goipc__deadline turns a relative timeout into an absolute CLOCK_MONOTONIC
 * time. It returns NULL for a negative timeout, which means no deadline. */
const struct timespec *goipc__deadline(int64_t timeout_ns, struct timespec *out);
/* */
void goipc__remaining(const struct timespec *deadline, struct timespec *out);

/* A gate counts operations in flight so that a close can wait for them. */
struct goipc_gate {
	atomic_bool closing;
	atomic_long active;
	pthread_mutex_t mu;
	pthread_cond_t cv;
};

int goipc__gate_init(struct goipc_gate *g);
void goipc__gate_destroy(struct goipc_gate *g);
bool goipc__gate_enter(struct goipc_gate *g);
void goipc__gate_leave(struct goipc_gate *g);
/* goipc__gate_close returns false when the gate was already closing. */
bool goipc__gate_close(struct goipc_gate *g);
void goipc__gate_drain(struct goipc_gate *g);
bool goipc__gate_closed(struct goipc_gate *g);

/* goipc__ring_read is goipc_ring_read with a payload limit. It stops before a
 * record whose payload exceeds maxlen and returns GOIPC_EBUFFER, with the
 * payload size in *need. That record stays queued. */
int goipc__ring_read(goipc_ring *r, int limit, goipc_read_fn fn, void *ctx, size_t maxlen, size_t *need, uint32_t *need_type);

int goipc__event_wait_until(goipc_event *e, const struct timespec *deadline);
int goipc__event_unlink_name(const char *name);

int goipc__queue_send_until(goipc_queue *q, uint32_t type, const void *payload, size_t len, const struct timespec *deadline);
int goipc__queue_recv_until(goipc_queue *q, void *dst, size_t cap, uint32_t *type, size_t *len, const struct timespec *deadline);

#endif
