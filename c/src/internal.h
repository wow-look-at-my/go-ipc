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
#define GOIPC_SERVICE_REGISTRY_SUFFIX ".svc"
#define GOIPC_SERVICE_CLIENT_PREFIX ".c."

#define GOIPC_NAME_PREFIX "go-ipc-"
#define GOIPC_NAME_SUFFIX ".name"
#define GOIPC_INC_SUFFIX ".inc"
#define GOIPC_LIFE_PREFIX "go-ipc-life-"
#define GOIPC_LIFE_SUFFIX ".sock"
#define GOIPC_LIFE_TEMP_SUFFIX ".tmp"
/* An instance id is this many lowercase hex digits. */
#define GOIPC_INC_LEN 16

/* A procID with this bit has a life socket behind it. */
#define GOIPC_PROC_WATCHABLE (UINT64_C(1) << 63)
/* */
#define GOIPC_PROC_ANY (UINT64_C(1) << 62)
/* */
#define GOIPC_PROC_NONE UINT64_C(0)
#define GOIPC_PROC_PENDING UINT64_C(1)
/* A claim slot at this cursor claims nothing. */
#define GOIPC_NO_INTENT UINT64_MAX

/* A claim slot names the process behind a claim. at and size are the cursor
 * range the owner claims, or is about to. */
struct goipc_claim_slot {
	_Atomic uint64_t owner;
	_Atomic uint64_t at;
	_Atomic uint64_t size;
	uint8_t pad[GOIPC_CLAIM_SLOT_SIZE - 24];
};

/* The ring control block. Its layout is the wire format of spec/README.md. */
struct goipc_ring_hdr {
	_Atomic uint64_t magic;
	uint32_t version;
	uint32_t flags;
	uint64_t capacity;
	/* consumer is the procID of the process that reads the ring. */
	_Atomic uint64_t consumer;
	uint8_t pad0[GOIPC_CACHE_LINE - 32];
	_Atomic uint64_t tail;
	uint8_t pad1[GOIPC_CACHE_LINE - 8];
	_Atomic uint64_t head;
	uint8_t pad2[GOIPC_CACHE_LINE - 8];
	_Atomic uint64_t head_cache;
	_Atomic int32_t recv_waiters;
	_Atomic int32_t send_waiters;
	uint8_t pad3[GOIPC_CACHE_LINE - 16];
	struct goipc_claim_slot slots[GOIPC_CLAIM_SLOTS];
};

_Static_assert(sizeof(struct goipc_claim_slot) == GOIPC_CLAIM_SLOT_SIZE, "claim slot size drifted");
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

/* goipc__ring_init is goipc_ring_init with the consumer recorded before the
 * magic, so a peer never attaches to a ring without its reader. */
int goipc__ring_init(goipc_ring *r, void *buf, size_t len, uint64_t consumer);
/* goipc__ring_try_claim records the claim range in slot before the tail
 * swap. */
int goipc__ring_try_claim(goipc_ring *r, int slot, uint32_t type, size_t len, goipc_claim *out);

/* A stall is a claim that stops the reader at head. slots lists every slot
 * whose intent covered head when the reader looked. */
struct goipc_stall {
	uint64_t at;
	int32_t length;
	int nslots;
	int slots[GOIPC_CLAIM_SLOTS];
};

bool goipc__ring_stalled(goipc_ring *r, struct goipc_stall *st);
/* goipc__ring_reclaim pads the stalled claim up to end. It returns false
 * when the claim changed under the reader. */
bool goipc__ring_reclaim(goipc_ring *r, const struct goipc_stall *st, uint64_t end, const int *dead, int ndead);
/* goipc__ring_acquire_slot takes a free slot for self, or the slot of a dead
 * owner whose claim the reader has passed. dead reports a dead owner. */
int goipc__ring_acquire_slot(goipc_ring *r, uint64_t self, bool (*dead)(void *ctx, uint64_t id), void *ctx, int *slot);
void goipc__ring_drop_slot(goipc_ring *r, uint64_t self, int slot);

int goipc__event_wait_until(goipc_event *e, const struct timespec *deadline);
int goipc__event_unlink_name(const char *name);

int goipc__queue_send_until(goipc_queue *q, uint32_t type, const void *payload, size_t len, const struct timespec *deadline);
int goipc__queue_recv_until(goipc_queue *q, void *dst, size_t cap, uint32_t *type, size_t *len, const struct timespec *deadline);
/* With pending set, the queue's reader is a channel peer that connects later. */
int goipc__queue_create(const char *name, size_t capacity, bool pending, goipc_queue **out);
/* goipc__queue_open is goipc_queue_open without the check of the receiver. */
int goipc__queue_open(const char *name, goipc_queue **out);
/* The peer of a channel is the reader of tx. rx reports when it goes. */
void goipc__channel_link(goipc_queue *tx, goipc_queue *rx);
int goipc__channel_connect(goipc_queue *rx);
/* goipc__queue_wake_receiver signals the not-empty event whether or not a receiver waits. */
int goipc__queue_wake_receiver(goipc_queue *q);

/* ---- identity.c: the procID and the life socket ---- */

/* goipc__self_id returns the procID of this process. The first call starts
 * the life socket. A process without one gets an id without the watchable
 * bit. */
uint64_t goipc__self_id(void);
/* */
int goipc__self_errno(void);
/* goipc__life_path returns a malloc copy of the life socket path of id. */
char *goipc__life_path(uint64_t id);
/* goipc__is_dead reports a process that has exited. A process that cannot be
 * checked counts as alive. */
bool goipc__is_dead(uint64_t id);

/* It runs under the registry lock, so it must not call into the registry or
 * block. */
typedef void (*goipc_exit_fn)(void *arg, uint64_t id, int err);
/* goipc__on_exit registers fn and returns its key in *key. It returns
 * GOIPC_EPEERGONE and registers nothing when the process is already gone. */
int goipc__on_exit(uint64_t id, goipc_exit_fn fn, void *arg, uint64_t *key);
/* goipc__cancel_exit removes a registration. After it returns, the function
 * of key is not running and does not run. */
void goipc__cancel_exit(uint64_t key);

/* ---- name.c: names, instances and the sweep ---- */

char *goipc__name_path(const char *name);
char *goipc__inc_path(const char *name);
/* goipc__instance_name returns a malloc copy of "<name>.<inc>". */
char *goipc__instance_name(const char *name, const char *inc);
/* goipc__lock_name takes the flock on the name file and returns its fd, or
 * returns GOIPC_EINUSE while a live process holds it. */
int goipc__lock_name(const char *name, int *fd);
void goipc__release_name(int fd);
/* */
void goipc__previous_inc(const char *name, char *inc);
int goipc__publish_inc(const char *name, const char *inc);
int goipc__new_inc(char *inc);
/* goipc__read_name returns the instance id of name for an opener. */
int goipc__read_name(const char *name, char *inc);
int goipc__unlink_name(const char *name, const char *inc);
int goipc__remove_instance(const char *name, const char *inc);
/* goipc__sweep runs goipc__sweep_dir on the runtime directory, once per process. */
void goipc__sweep(void);
void goipc__sweep_dir(const char *dir);

#endif
