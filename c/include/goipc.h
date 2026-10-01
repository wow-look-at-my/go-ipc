/*goipc: C implementation of the go-ipc wire protocol (spec/README.md). Every
 * function returns GOIPC_OK or a negative goipc_err unless its comment says
 * otherwise. Send, claim, commit and abort are safe from any number of
 * threads. Receive and read_batch belong to a single thread. Close may run on
 * any thread and releases every waiter. Destroy frees the handle; no other
 * call may run on it at that time or after. */
#ifndef GOIPC_H
#define GOIPC_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define GOIPC_RING_MAGIC UINT64_C(0x676F2D6970632D31)
#define GOIPC_RING_VERSION 1u
#define GOIPC_HEADER_SIZE 512u
#define GOIPC_MIN_CAPACITY 4096u
#define GOIPC_RECORD_HEADER_SIZE 8u
#define GOIPC_TYPE_PADDING UINT32_C(0xFFFFFFFF)
#define GOIPC_DEFAULT_CAPACITY 1048576u
#define GOIPC_CONN_TYPE_DATA 0u
#define GOIPC_CONN_TYPE_EOF 1u

typedef enum goipc_err {
	GOIPC_OK = 0,
	GOIPC_ECLOSED = -1,
	GOIPC_EFULL = -2,
	GOIPC_EEMPTY = -3,
	GOIPC_ETOOLARGE = -4,
	GOIPC_ERESERVED = -5,
	GOIPC_EINVALNAME = -6,
	GOIPC_EINVALCAP = -7,
	GOIPC_ETOOSMALL = -8,
	GOIPC_EBADLAYOUT = -9,
	GOIPC_ECORRUPT = -10,
	GOIPC_EUNALIGNED = -11,
	GOIPC_ETIMEDOUT = -12,
	/* A system call failed. goipc_last_errno() holds the errno value. */
	GOIPC_ESYS = -13,
	GOIPC_ENOMEM = -14,
	GOIPC_EINVAL = -15,
	/* The receive buffer is smaller than the next message, which stays queued. */
	GOIPC_EBUFFER = -16,
	/* The conn peer ended the stream. */
	GOIPC_EOF = -17
} goipc_err;

/* goipc_strerror returns a static description of err. */
const char *goipc_strerror(int err);

/* goipc_last_errno returns the errno of the latest GOIPC_ESYS on this thread. */
int goipc_last_errno(void);

/* ---- Ring: a lock-free MPSC ring over a caller buffer ---- */

typedef struct goipc_ring {
	void *hdr;
	uint8_t *data;
	uint64_t mask;
} goipc_ring;

size_t goipc_ring_size(size_t capacity);
int goipc_ring_init(goipc_ring *r, void *buf, size_t len);
int goipc_ring_attach(goipc_ring *r, void *buf, size_t len);
size_t goipc_ring_capacity(const goipc_ring *r);
size_t goipc_ring_max_message_size(const goipc_ring *r);
size_t goipc_ring_buffered(const goipc_ring *r);
int goipc_ring_empty(const goipc_ring *r);

typedef struct goipc_claim {
	goipc_ring *ring;
	uint64_t index;
	int32_t total;
	/* bytes is the payload region. Fill it, then commit or abort. */
	uint8_t *bytes;
	size_t len;
} goipc_claim;

int goipc_ring_try_claim(goipc_ring *r, uint32_t type, size_t len, goipc_claim *out);
void goipc_claim_commit(goipc_claim *c);
void goipc_claim_abort(goipc_claim *c);
int goipc_ring_try_write(goipc_ring *r, uint32_t type, const void *payload, size_t len);

/* The payload aliases the ring and is valid only during the call. */
typedef void (*goipc_read_fn)(void *ctx, uint32_t type, const uint8_t *payload, size_t len);

/* goipc_ring_read returns the count of records it delivered, or an error. */
int goipc_ring_read(goipc_ring *r, int limit, goipc_read_fn fn, void *ctx);

/* On GOIPC_EBUFFER, *len holds the size the next message needs. */
int goipc_ring_try_recv(goipc_ring *r, void *dst, size_t cap, uint32_t *type, size_t *len);

/* ---- Event: a named cross-process wake channel ---- */

typedef struct goipc_event goipc_event;

int goipc_event_create(const char *name, goipc_event **out);
int goipc_event_open(const char *name, goipc_event **out);
int goipc_event_signal(goipc_event *e, int n);
/* Returns GOIPC_OK on a wakeup, GOIPC_ETIMEDOUT or GOIPC_ECLOSED. */
int goipc_event_wait(goipc_event *e, int64_t timeout_ns);
int goipc_event_close(goipc_event *e);
int goipc_event_unlink(goipc_event *e);
void goipc_event_destroy(goipc_event *e);

/* ---- Queue: a named MPSC message queue ---- */

typedef struct goipc_queue goipc_queue;

/* */
int goipc_queue_create(const char *name, size_t capacity, goipc_queue **out);
int goipc_queue_open(const char *name, goipc_queue **out);
const char *goipc_queue_name(const goipc_queue *q);
size_t goipc_queue_capacity(const goipc_queue *q);
size_t goipc_queue_max_message_size(const goipc_queue *q);
/* The ring has no close guard. Do not use it after close. */
goipc_ring *goipc_queue_ring(goipc_queue *q);

int goipc_queue_try_send(goipc_queue *q, uint32_t type, const void *payload, size_t len);
int goipc_queue_send(goipc_queue *q, uint32_t type, const void *payload, size_t len, int64_t timeout_ns);
int goipc_queue_claim(goipc_queue *q, uint32_t type, size_t len, int64_t timeout_ns, goipc_claim *out);
void goipc_queue_commit(goipc_queue *q, goipc_claim *c);
void goipc_queue_abort(goipc_queue *q, goipc_claim *c);

int goipc_queue_try_recv(goipc_queue *q, void *dst, size_t cap, uint32_t *type, size_t *len);
int goipc_queue_recv(goipc_queue *q, void *dst, size_t cap, uint32_t *type, size_t *len, int64_t timeout_ns);
/* */
int goipc_queue_read_batch(goipc_queue *q, int limit, goipc_read_fn fn, void *ctx, int64_t timeout_ns);

int goipc_queue_close(goipc_queue *q);
int goipc_queue_unlink(goipc_queue *q);
void goipc_queue_destroy(goipc_queue *q);

/* ---- Channel: a pair of queues with opposite directions ---- */

typedef struct goipc_channel goipc_channel;

int goipc_channel_create(const char *name, size_t capacity, goipc_channel **out);
int goipc_channel_open(const char *name, goipc_channel **out);
goipc_queue *goipc_channel_tx(goipc_channel *c);
goipc_queue *goipc_channel_rx(goipc_channel *c);
size_t goipc_channel_max_message_size(const goipc_channel *c);
int goipc_channel_send(goipc_channel *c, uint32_t type, const void *payload, size_t len, int64_t timeout_ns);
int goipc_channel_recv(goipc_channel *c, void *dst, size_t cap, uint32_t *type, size_t *len, int64_t timeout_ns);
int goipc_channel_close(goipc_channel *c);
int goipc_channel_unlink(goipc_channel *c);
void goipc_channel_destroy(goipc_channel *c);

/* ---- Conn: a byte stream over a channel ---- */

typedef struct goipc_conn goipc_conn;

int goipc_conn_listen(const char *name, size_t capacity, goipc_conn **out);
int goipc_conn_dial(const char *name, goipc_conn **out);
goipc_channel *goipc_conn_channel(goipc_conn *c);
/* Reads up to cap bytes. Returns GOIPC_EOF once the peer has closed and every byte is consumed. */
int goipc_conn_read(goipc_conn *c, void *buf, size_t cap, int64_t timeout_ns, size_t *nread);
/* *written holds the bytes sent, which is less than len only on an error. */
int goipc_conn_write(goipc_conn *c, const void *buf, size_t len, int64_t timeout_ns, size_t *written);
/* Sends end-of-stream with a blocking send. Later writes return GOIPC_ECLOSED; reads keep working. */
int goipc_conn_close_write(goipc_conn *c, int64_t timeout_ns);
/* Sends end-of-stream, best effort, unless close_write sent it. Then closes the channel. */
int goipc_conn_close(goipc_conn *c);
int goipc_conn_unlink(goipc_conn *c);
void goipc_conn_destroy(goipc_conn *c);

#ifdef __cplusplus
}
#endif

#endif
