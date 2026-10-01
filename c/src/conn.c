#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#include "internal.h"

struct goipc_conn {
	goipc_channel *ch;
	atomic_bool closed;

	pthread_mutex_t read_mu;
	uint8_t *buf;
	size_t buf_len;
	size_t pending_off;
	size_t pending_len;
	bool eof;

	pthread_mutex_t write_mu;
	/* eof_sent is set once close_write delivered end-of-stream. */
	atomic_bool eof_sent;
};

static int wrap(goipc_channel *ch, goipc_conn **out)
{
	goipc_conn *c = calloc(1, sizeof *c);
	if (c == NULL)
		return GOIPC_ENOMEM;
	c->buf_len = goipc_queue_max_message_size(goipc_channel_rx(ch));
	c->buf = malloc(c->buf_len);
	if (c->buf == NULL) {
		free(c);
		return GOIPC_ENOMEM;
	}
	int rc = pthread_mutex_init(&c->read_mu, NULL);
	if (rc != 0) {
		free(c->buf);
		free(c);
		return goipc__sys_errno(rc);
	}
	rc = pthread_mutex_init(&c->write_mu, NULL);
	if (rc != 0) {
		pthread_mutex_destroy(&c->read_mu);
		free(c->buf);
		free(c);
		return goipc__sys_errno(rc);
	}
	atomic_init(&c->closed, false);
	atomic_init(&c->eof_sent, false);
	c->ch = ch;
	*out = c;
	return GOIPC_OK;
}

int goipc_conn_listen(const char *name, size_t capacity, goipc_conn **out)
{
	goipc_channel *ch;
	int rc = goipc_channel_create(name, capacity, &ch);
	if (rc != GOIPC_OK)
		return rc;
	rc = wrap(ch, out);
	if (rc != GOIPC_OK) {
		goipc_channel_close(ch);
		goipc_channel_unlink(ch);
		goipc_channel_destroy(ch);
	}
	return rc;
}

int goipc_conn_dial(const char *name, goipc_conn **out)
{
	goipc_channel *ch;
	int rc = goipc_channel_open(name, &ch);
	if (rc != GOIPC_OK)
		return rc;
	rc = wrap(ch, out);
	if (rc != GOIPC_OK)
		goipc_channel_destroy(ch);
	return rc;
}

goipc_channel *goipc_conn_channel(goipc_conn *c)
{
	return c->ch;
}

int goipc_conn_read(goipc_conn *c, void *buf, size_t cap, int64_t timeout_ns, size_t *nread)
{
	struct timespec ts;
	const struct timespec *deadline = goipc__deadline(timeout_ns, &ts);
	int rc = GOIPC_OK;
	*nread = 0;

	pthread_mutex_lock(&c->read_mu);
	while (c->pending_len == 0) {
		if (c->eof) {
			rc = GOIPC_EOF;
			goto out;
		}
		uint32_t type;
		size_t len;
		rc = goipc__queue_recv_until(goipc_channel_rx(c->ch), c->buf, c->buf_len, &type, &len, deadline);
		if (rc != GOIPC_OK)
			goto out;
		if (type == GOIPC_CONN_TYPE_EOF) {
			c->eof = true;
			rc = GOIPC_EOF;
			goto out;
		}
		c->pending_off = 0;
		c->pending_len = len;
	}
	size_t n = cap < c->pending_len ? cap : c->pending_len;
	if (n > 0)
		memcpy(buf, c->buf + c->pending_off, n);
	c->pending_off += n;
	c->pending_len -= n;
	*nread = n;
out:
	pthread_mutex_unlock(&c->read_mu);
	return rc;
}

int goipc_conn_write(goipc_conn *c, const void *buf, size_t len, int64_t timeout_ns, size_t *written)
{
	struct timespec ts;
	const struct timespec *deadline = goipc__deadline(timeout_ns, &ts);
	goipc_queue *tx = goipc_channel_tx(c->ch);
	size_t limit = goipc_queue_max_message_size(tx);
	const uint8_t *p = buf;
	size_t done = 0;
	int rc = GOIPC_OK;

	pthread_mutex_lock(&c->write_mu);
	if (atomic_load(&c->eof_sent))
		rc = GOIPC_ECLOSED;
	while (rc == GOIPC_OK && done < len) {
		size_t chunk = len - done < limit ? len - done : limit;
		rc = goipc__queue_send_until(tx, GOIPC_CONN_TYPE_DATA, p + done, chunk, deadline);
		if (rc != GOIPC_OK)
			break;
		done += chunk;
	}
	pthread_mutex_unlock(&c->write_mu);
	*written = done;
	return rc;
}

int goipc_conn_close_write(goipc_conn *c, int64_t timeout_ns)
{
	struct timespec ts;
	const struct timespec *deadline = goipc__deadline(timeout_ns, &ts);
	int rc = GOIPC_ECLOSED;
	pthread_mutex_lock(&c->write_mu);
	if (!atomic_load(&c->eof_sent)) {
		rc = goipc__queue_send_until(goipc_channel_tx(c->ch), GOIPC_CONN_TYPE_EOF, NULL, 0, deadline);
		if (rc == GOIPC_OK)
			atomic_store(&c->eof_sent, true);
	}
	pthread_mutex_unlock(&c->write_mu);
	return rc;
}

int goipc_conn_close(goipc_conn *c)
{
	if (atomic_exchange(&c->closed, true))
		return GOIPC_ECLOSED;
	/* A peer that already left has nobody to deliver end-of-stream to. */
	if (!atomic_load(&c->eof_sent))
		goipc_queue_try_send(goipc_channel_tx(c->ch), GOIPC_CONN_TYPE_EOF, NULL, 0);
	return goipc_channel_close(c->ch);
}

int goipc_conn_unlink(goipc_conn *c)
{
	return goipc_channel_unlink(c->ch);
}

void goipc_conn_destroy(goipc_conn *c)
{
	if (c == NULL)
		return;
	if (!atomic_load(&c->closed))
		goipc_conn_close(c);
	goipc_channel_destroy(c->ch);
	pthread_mutex_destroy(&c->read_mu);
	pthread_mutex_destroy(&c->write_mu);
	free(c->buf);
	free(c);
}
