/* The service layer of spec/service.md: a server answers calls from any
 * number of client processes. Each client owns a channel; a queue the server
 * owns carries the knock that announces a new channel. */
#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "internal.h"

#define SERVICE_FIRST_SEQ UINT64_C(1)

struct goipc_reply {
	uint32_t type;
	/* buf holds the sequence number, then the payload. len counts both. */
	uint8_t *buf;
	size_t len;
	size_t cap;
	bool set;
	int rc;
};

struct goipc_session {
	goipc_service *svc;
	goipc_channel *ch;
	uint64_t ordinal;
	struct goipc_reply reply;
	struct goipc_session *next;
	struct goipc_session *prev;
};

struct goipc_service {
	char *name;
	goipc_service_fn fn;
	goipc_gone_fn gone;
	void *ctx;
	goipc_queue *reg;
	pthread_t thread;
	bool running;

	pthread_mutex_t mu;
	/* idle signals when the last session thread has left. */
	pthread_cond_t idle;
	bool closed;
	uint64_t next;
	size_t active;
	struct goipc_session *sessions;
};

static void put_u64(uint8_t *b, uint64_t v)
{
	for (int i = 0; i < 8; i++)
		b[i] = (uint8_t)(v >> (8 * i));
}

static uint64_t get_u64(const uint8_t *b)
{
	uint64_t v = 0;
	for (int i = 0; i < 8; i++)
		v |= (uint64_t)b[i] << (8 * i);
	return v;
}

static bool is_client_id(const char *s, size_t len)
{
	if (len != GOIPC_INC_LEN)
		return false;
	for (size_t i = 0; i < len; i++) {
		char c = s[i];
		if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
			return false;
	}
	return true;
}

/* ---- replies ---- */

static int reply_reserve(struct goipc_reply *r, size_t len)
{
	size_t need = GOIPC_SERVICE_SEQUENCE_SIZE + len;
	if (need <= r->cap)
		return GOIPC_OK;
	uint8_t *grown = realloc(r->buf, need);
	if (grown == NULL)
		return GOIPC_ENOMEM;
	r->buf = grown;
	r->cap = need;
	return GOIPC_OK;
}

static int reply_fill(struct goipc_reply *r, uint32_t type, const void *payload, size_t len)
{
	int rc = reply_reserve(r, len);
	if (rc != GOIPC_OK) {
		r->rc = rc;
		return rc;
	}
	if (len > 0)
		memcpy(r->buf + GOIPC_SERVICE_SEQUENCE_SIZE, payload, len);
	r->type = type;
	r->len = GOIPC_SERVICE_SEQUENCE_SIZE + len;
	r->set = true;
	r->rc = GOIPC_OK;
	return GOIPC_OK;
}

int goipc_reply_set(goipc_reply *r, uint32_t type, const void *payload, size_t len)
{
	if (type >= GOIPC_SERVICE_RESERVED_TYPE_MIN)
		return GOIPC_ERESERVED;
	return reply_fill(r, type, payload, len);
}

int goipc_reply_error(goipc_reply *r, const char *message)
{
	if (message == NULL)
		message = "";
	return reply_fill(r, GOIPC_SERVICE_TYPE_ERROR, message, strlen(message));
}

uint64_t goipc_session_ordinal(const goipc_session *s)
{
	return s->ordinal;
}

/* ---- sessions ---- */

static int send_reply(struct goipc_session *sess, uint64_t seq)
{
	put_u64(sess->reply.buf, seq);
	return goipc_channel_send(sess->ch, sess->reply.type, sess->reply.buf, sess->reply.len, -1);
}

static int send_error(struct goipc_session *sess, uint64_t seq, const char *message)
{
	int rc = goipc_reply_error(&sess->reply, message);
	if (rc != GOIPC_OK)
		return rc;
	return send_reply(sess, seq);
}

/* answer runs the handler on one request and sends its reply. */
static int answer(struct goipc_session *sess, uint32_t type, const uint8_t *msg, size_t len)
{
	if (len < GOIPC_SERVICE_SEQUENCE_SIZE)
		return send_error(sess, 0, "goipc: request is shorter than its sequence number");
	uint64_t seq = get_u64(msg);
	char text[96];
	if (type >= GOIPC_SERVICE_RESERVED_TYPE_MIN) {
		snprintf(text, sizeof text, "goipc: request type %lu is reserved", (unsigned long)type);
		return send_error(sess, seq, text);
	}
	struct goipc_reply *r = &sess->reply;
	r->set = false;
	r->rc = GOIPC_OK;
	sess->svc->fn(sess->svc->ctx, sess, type, msg + GOIPC_SERVICE_SEQUENCE_SIZE, len - GOIPC_SERVICE_SEQUENCE_SIZE, r);
	if (r->rc != GOIPC_OK)
		return send_error(sess, seq, goipc_strerror(r->rc));
	if (!r->set)
		return send_error(sess, seq, "goipc: the handler set no reply");
	if (r->len > goipc_channel_max_message_size(sess->ch))
		return send_error(sess, seq, "goipc: reply exceeds the maximum size");
	return send_reply(sess, seq);
}

/* forget drops a session from its service and frees it. The channel is
 * destroyed outside the lock, after the close has stopped touching it. */
static void forget(struct goipc_session *sess)
{
	goipc_service *s = sess->svc;
	pthread_mutex_lock(&s->mu);
	if (sess->prev != NULL)
		sess->prev->next = sess->next;
	else
		s->sessions = sess->next;
	if (sess->next != NULL)
		sess->next->prev = sess->prev;
	s->active--;
	if (s->active == 0)
		pthread_cond_broadcast(&s->idle);
	pthread_mutex_unlock(&s->mu);
	goipc_channel_destroy(sess->ch);
	free(sess->reply.buf);
	free(sess);
}

/* serve answers one client until it goes or the service closes. A client
 * that went has its channel removed before the handler hears of it. */
static void *serve(void *arg)
{
	struct goipc_session *sess = arg;
	goipc_service *s = sess->svc;
	size_t cap = goipc_channel_max_message_size(sess->ch);
	uint8_t *buf = malloc(cap > 0 ? cap : 1);
	while (buf != NULL) {
		uint32_t type;
		size_t len;
		int rc = goipc_channel_recv(sess->ch, buf, cap, &type, &len, -1);
		if (rc == GOIPC_EPEERGONE) {
			goipc_channel_close(sess->ch);
			goipc_channel_unlink(sess->ch);
			if (s->gone != NULL)
				s->gone(s->ctx, sess);
			break;
		}
		if (rc != GOIPC_OK)
			break;
		if (answer(sess, type, buf, len) != GOIPC_OK)
			break;
	}
	free(buf);
	forget(sess);
	return NULL;
}

/* adopt opens a client's channel and serves it. An open that fails means the
 * channel was adopted already, its client has gone. Otherwise, the client
 * has not finished creating it and knocks when it has. */
static void adopt(goipc_service *s, const char *id)
{
	char *name = goipc__join(s->name, GOIPC_SERVICE_CLIENT_PREFIX, id);
	if (name == NULL)
		return;
	goipc_channel *ch;
	int rc = goipc_channel_open(name, &ch);
	free(name);
	if (rc != GOIPC_OK)
		return;
	struct goipc_session *sess = calloc(1, sizeof *sess);
	if (sess == NULL) {
		goipc_channel_destroy(ch);
		return;
	}
	sess->svc = s;
	sess->ch = ch;

	pthread_mutex_lock(&s->mu);
	if (s->closed) {
		pthread_mutex_unlock(&s->mu);
		goipc_channel_destroy(ch);
		free(sess);
		return;
	}
	sess->ordinal = s->next++;
	sess->next = s->sessions;
	if (s->sessions != NULL)
		s->sessions->prev = sess;
	s->sessions = sess;
	s->active++;
	pthread_mutex_unlock(&s->mu);

	uint8_t hello[GOIPC_SERVICE_SEQUENCE_SIZE];
	put_u64(hello, sess->ordinal);
	if (goipc_channel_send(ch, GOIPC_SERVICE_TYPE_HELLO, hello, sizeof hello, -1) != GOIPC_OK) {
		forget(sess);
		return;
	}
	pthread_t th;
	pthread_attr_t attr;
	if (pthread_attr_init(&attr) != 0) {
		forget(sess);
		return;
	}
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	int err = pthread_create(&th, &attr, serve, sess);
	pthread_attr_destroy(&attr);
	if (err != 0)
		forget(sess);
}

/* scan_clients adopts every client channel that exists under the service
 * name. A client that created its channel before the service existed is
 * found here. */
static void scan_clients(goipc_service *s)
{
	char *prefix = goipc__join(GOIPC_NAME_PREFIX, s->name, GOIPC_SERVICE_CLIENT_PREFIX);
	if (prefix == NULL)
		return;
	size_t plen = strlen(prefix);
	static const char suffix[] = GOIPC_O2C_SUFFIX GOIPC_NAME_SUFFIX;
	size_t slen = sizeof suffix - 1;
	DIR *d = opendir(GOIPC_SHM_DIR);
	if (d == NULL) {
		free(prefix);
		return;
	}
	struct dirent *e;
	while ((e = readdir(d)) != NULL) {
		const char *n = e->d_name;
		size_t len = strlen(n);
		if (len < plen + slen || memcmp(n, prefix, plen) != 0 || memcmp(n + len - slen, suffix, slen) != 0)
			continue;
		if (!is_client_id(n + plen, len - plen - slen))
			continue;
		char id[GOIPC_INC_LEN + 1];
		memcpy(id, n + plen, GOIPC_INC_LEN);
		id[GOIPC_INC_LEN] = '\0';
		adopt(s, id);
	}
	closedir(d);
	free(prefix);
}

/* run adopts each client that knocks until the queue closes. */
static void *run(void *arg)
{
	goipc_service *s = arg;
	size_t cap = goipc_queue_max_message_size(s->reg);
	char *buf = malloc(cap > 0 ? cap : 1);
	while (buf != NULL) {
		uint32_t type;
		size_t len;
		int rc = goipc_queue_recv(s->reg, buf, cap, &type, &len, -1);
		if (rc != GOIPC_OK)
			break;
		if (type != GOIPC_SERVICE_TYPE_KNOCK || !is_client_id(buf, len))
			continue;
		char id[GOIPC_INC_LEN + 1];
		memcpy(id, buf, GOIPC_INC_LEN);
		id[GOIPC_INC_LEN] = '\0';
		adopt(s, id);
	}
	free(buf);
	return NULL;
}

static void free_service(goipc_service *s)
{
	goipc_queue_destroy(s->reg);
	pthread_cond_destroy(&s->idle);
	pthread_mutex_destroy(&s->mu);
	free(s->name);
	free(s);
}

int goipc_service_serve(const char *name, size_t capacity, goipc_service_fn fn, goipc_gone_fn gone, void *ctx, goipc_service **out)
{
	int rc = goipc__validate_name(name);
	if (rc != GOIPC_OK)
		return rc;
	if (fn == NULL)
		return GOIPC_EINVAL;
	goipc_service *s = calloc(1, sizeof *s);
	if (s == NULL)
		return GOIPC_ENOMEM;
	s->fn = fn;
	s->gone = gone;
	s->ctx = ctx;
	s->name = strdup(name);
	char *reg = goipc__join(name, GOIPC_SERVICE_REGISTRY_SUFFIX, "");
	if (s->name == NULL || reg == NULL) {
		free(reg);
		free(s->name);
		free(s);
		return GOIPC_ENOMEM;
	}
	int err = pthread_mutex_init(&s->mu, NULL);
	if (err == 0 && (err = pthread_cond_init(&s->idle, NULL)) != 0)
		pthread_mutex_destroy(&s->mu);
	if (err != 0) {
		free(reg);
		free(s->name);
		free(s);
		return goipc__sys_errno(err);
	}
	rc = goipc_queue_create(reg, capacity, &s->reg);
	free(reg);
	if (rc != GOIPC_OK) {
		free_service(s);
		return rc;
	}
	/* The queue is published, so a client whose channel the scan misses opens the queue and knocks. */
	scan_clients(s);
	err = pthread_create(&s->thread, NULL, run, s);
	if (err != 0) {
		goipc_service_close(s);
		free_service(s);
		return goipc__sys_errno(err);
	}
	s->running = true;
	*out = s;
	return GOIPC_OK;
}

int goipc_service_close(goipc_service *s)
{
	pthread_mutex_lock(&s->mu);
	if (s->closed) {
		pthread_mutex_unlock(&s->mu);
		return GOIPC_ECLOSED;
	}
	s->closed = true;
	pthread_mutex_unlock(&s->mu);

	int rc = goipc_queue_close(s->reg);
	if (s->running) {
		pthread_join(s->thread, NULL);
		s->running = false;
	}
	/* Each session thread wakes from its receive, finds the channel closed
	 * and leaves. A handler that is running finishes its call first. */
	pthread_mutex_lock(&s->mu);
	for (struct goipc_session *sess = s->sessions; sess != NULL; sess = sess->next)
		goipc_channel_close(sess->ch);
	while (s->active > 0)
		pthread_cond_wait(&s->idle, &s->mu);
	pthread_mutex_unlock(&s->mu);

	int rc2 = goipc_queue_unlink(s->reg);
	return rc != GOIPC_OK ? rc : rc2;
}

void goipc_service_destroy(goipc_service *s)
{
	if (s == NULL)
		return;
	goipc_service_close(s);
	free_service(s);
}

/* ---- clients ---- */

struct goipc_client {
	char *name;
	uint64_t ordinal;
	goipc_channel *ch;
	pthread_mutex_t mu;
	uint64_t seq;
	/* buf receives replies; req carries the framed request. */
	uint8_t *buf;
	size_t cap;
	uint8_t *req;
	size_t req_cap;
};

/* knock tells a running service about a new client. A service that is not
 * up yet finds the client's channel when it starts, so a failure here costs
 * nothing. */
static void knock(const char *name, const char *id, const struct timespec *deadline)
{
	char *reg = goipc__join(name, GOIPC_SERVICE_REGISTRY_SUFFIX, "");
	if (reg == NULL)
		return;
	goipc_queue *q;
	int rc = goipc_queue_open(reg, &q);
	free(reg);
	if (rc != GOIPC_OK)
		return;
	goipc__queue_send_until(q, GOIPC_SERVICE_TYPE_KNOCK, id, GOIPC_INC_LEN, deadline);
	goipc_queue_destroy(q);
}

static void free_client(goipc_client *c)
{
	goipc_channel_destroy(c->ch);
	pthread_mutex_destroy(&c->mu);
	free(c->req);
	free(c->buf);
	free(c->name);
	free(c);
}

int goipc_client_connect(const char *name, size_t capacity, int64_t timeout_ns, goipc_client **out)
{
	int rc = goipc__validate_name(name);
	if (rc != GOIPC_OK)
		return rc;
	char id[GOIPC_INC_LEN + 1];
	if ((rc = goipc__new_inc(id)) != GOIPC_OK)
		return rc;
	goipc_client *c = calloc(1, sizeof *c);
	if (c == NULL)
		return GOIPC_ENOMEM;
	c->name = strdup(name);
	char *chname = goipc__join(name, GOIPC_SERVICE_CLIENT_PREFIX, id);
	if (c->name == NULL || chname == NULL) {
		free(chname);
		free(c->name);
		free(c);
		return GOIPC_ENOMEM;
	}
	int err = pthread_mutex_init(&c->mu, NULL);
	if (err != 0) {
		free(chname);
		free(c->name);
		free(c);
		return goipc__sys_errno(err);
	}
	rc = goipc_channel_create(chname, capacity, &c->ch);
	free(chname);
	if (rc != GOIPC_OK) {
		free_client(c);
		return rc;
	}
	c->cap = goipc_channel_max_message_size(c->ch);
	c->buf = malloc(c->cap > 0 ? c->cap : 1);
	if (c->buf == NULL) {
		goipc_channel_unlink(c->ch);
		free_client(c);
		return GOIPC_ENOMEM;
	}

	struct timespec ts;
	const struct timespec *deadline = goipc__deadline(timeout_ns, &ts);
	knock(name, id, deadline);
	uint32_t type;
	size_t len;
	rc = goipc__queue_recv_until(goipc_channel_rx(c->ch), c->buf, c->cap, &type, &len, deadline);
	if (rc == GOIPC_OK && (type != GOIPC_SERVICE_TYPE_HELLO || len != GOIPC_SERVICE_SEQUENCE_SIZE))
		rc = GOIPC_ECORRUPT;
	if (rc != GOIPC_OK) {
		goipc_channel_close(c->ch);
		goipc_channel_unlink(c->ch);
		free_client(c);
		return rc;
	}
	c->ordinal = get_u64(c->buf);
	c->seq = SERVICE_FIRST_SEQ - 1;
	*out = c;
	return GOIPC_OK;
}

uint64_t goipc_client_ordinal(const goipc_client *c)
{
	return c->ordinal;
}

size_t goipc_client_max_payload_size(const goipc_client *c)
{
	return c->cap - GOIPC_SERVICE_SEQUENCE_SIZE;
}

/* copy_out copies a reply payload into the caller's buffer and returns rc. A
 * reply that does not fit is dropped, and an error message is cut to fit. */
static int copy_out(const uint8_t *msg, size_t len, void *buf, size_t cap, size_t *reply_len, int rc)
{
	size_t n = len - GOIPC_SERVICE_SEQUENCE_SIZE;
	if (reply_len != NULL)
		*reply_len = n;
	if (n > cap) {
		if (rc == GOIPC_OK)
			return GOIPC_EBUFFER;
		n = cap;
	}
	if (n > 0)
		memcpy(buf, msg + GOIPC_SERVICE_SEQUENCE_SIZE, n);
	return rc;
}

int goipc_client_call(goipc_client *c, uint32_t type, const void *payload, size_t len, int64_t timeout_ns,
		      uint32_t *reply_type, void *buf, size_t cap, size_t *reply_len)
{
	if (type >= GOIPC_SERVICE_RESERVED_TYPE_MIN)
		return GOIPC_ERESERVED;
	if (len > goipc_client_max_payload_size(c))
		return GOIPC_ETOOLARGE;
	struct timespec ts;
	const struct timespec *deadline = goipc__deadline(timeout_ns, &ts);
	pthread_mutex_lock(&c->mu);
	size_t need = GOIPC_SERVICE_SEQUENCE_SIZE + len;
	if (need > c->req_cap) {
		uint8_t *grown = realloc(c->req, need);
		if (grown == NULL) {
			pthread_mutex_unlock(&c->mu);
			return GOIPC_ENOMEM;
		}
		c->req = grown;
		c->req_cap = need;
	}
	uint64_t seq = ++c->seq;
	put_u64(c->req, seq);
	if (len > 0)
		memcpy(c->req + GOIPC_SERVICE_SEQUENCE_SIZE, payload, len);
	int rc = goipc__queue_send_until(goipc_channel_tx(c->ch), type, c->req, need, deadline);
	while (rc == GOIPC_OK) {
		uint32_t rtype;
		size_t rlen;
		rc = goipc__queue_recv_until(goipc_channel_rx(c->ch), c->buf, c->cap, &rtype, &rlen, deadline);
		if (rc != GOIPC_OK)
			break;
		if (rlen < GOIPC_SERVICE_SEQUENCE_SIZE) {
			rc = GOIPC_ECORRUPT;
			break;
		}
		uint64_t rseq = get_u64(c->buf);
		/* A reply below the sequence answers a call this client gave up on. */
		if (rseq < seq)
			continue;
		if (rseq > seq) {
			rc = GOIPC_ECORRUPT;
			break;
		}
		if (rtype == GOIPC_SERVICE_TYPE_ERROR) {
			rc = copy_out(c->buf, rlen, buf, cap, reply_len, GOIPC_ECALL);
			break;
		}
		if (rtype >= GOIPC_SERVICE_RESERVED_TYPE_MIN) {
			rc = GOIPC_ECORRUPT;
			break;
		}
		if (reply_type != NULL)
			*reply_type = rtype;
		rc = copy_out(c->buf, rlen, buf, cap, reply_len, GOIPC_OK);
		break;
	}
	pthread_mutex_unlock(&c->mu);
	return rc;
}

int goipc_client_close(goipc_client *c)
{
	int rc = goipc_channel_close(c->ch);
	int rc2 = goipc_channel_unlink(c->ch);
	return rc != GOIPC_OK ? rc : rc2;
}

void goipc_client_destroy(goipc_client *c)
{
	if (c == NULL)
		return;
	free_client(c);
}
