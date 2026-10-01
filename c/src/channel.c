#include <stdlib.h>

#include "internal.h"

struct goipc_channel {
	goipc_queue *tx;
	goipc_queue *rx;
};

static int alloc_channel(goipc_channel **out)
{
	*out = calloc(1, sizeof **out);
	return *out == NULL ? GOIPC_ENOMEM : GOIPC_OK;
}

int goipc_channel_create(const char *name, size_t capacity, goipc_channel **out)
{
	int rc = goipc__validate_name(name);
	if (rc != GOIPC_OK)
		return rc;
	char *c2o = goipc__join(name, GOIPC_C2O_SUFFIX, "");
	char *o2c = goipc__join(name, GOIPC_O2C_SUFFIX, "");
	goipc_channel *c = NULL;
	if (c2o == NULL || o2c == NULL || alloc_channel(&c) != GOIPC_OK) {
		rc = GOIPC_ENOMEM;
		goto out;
	}
	/* The direction to the peer has no reader until a peer connects. */
	if ((rc = goipc__queue_create(c2o, capacity, true, &c->tx)) != GOIPC_OK)
		goto out;
	if ((rc = goipc__queue_create(o2c, capacity, false, &c->rx)) != GOIPC_OK) {
		goipc_queue_close(c->tx);
		goipc_queue_unlink(c->tx);
		goipc_queue_destroy(c->tx);
		goto out;
	}
	goipc__channel_link(c->tx, c->rx);
	*out = c;
	c = NULL;
out:
	free(c);
	free(c2o);
	free(o2c);
	return rc;
}

int goipc_channel_open(const char *name, goipc_channel **out)
{
	int rc = goipc__validate_name(name);
	if (rc != GOIPC_OK)
		return rc;
	char *c2o = goipc__join(name, GOIPC_C2O_SUFFIX, "");
	char *o2c = goipc__join(name, GOIPC_O2C_SUFFIX, "");
	goipc_channel *c = NULL;
	if (c2o == NULL || o2c == NULL || alloc_channel(&c) != GOIPC_OK) {
		rc = GOIPC_ENOMEM;
		goto out;
	}
	if ((rc = goipc_queue_open(o2c, &c->tx)) != GOIPC_OK)
		goto out;
	if ((rc = goipc__queue_open(c2o, &c->rx)) != GOIPC_OK) {
		goipc_queue_destroy(c->tx);
		goto out;
	}
	if ((rc = goipc__channel_connect(c->rx)) != GOIPC_OK) {
		goipc_queue_destroy(c->rx);
		goipc_queue_destroy(c->tx);
		goto out;
	}
	goipc__channel_link(c->tx, c->rx);
	/* The creator may already be parked. It wakes to find its peer, and to
	 * start the watch on the peer's process. */
	goipc__queue_wake_receiver(c->tx);
	*out = c;
	c = NULL;
out:
	free(c);
	free(c2o);
	free(o2c);
	return rc;
}

goipc_queue *goipc_channel_tx(goipc_channel *c)
{
	return c->tx;
}

goipc_queue *goipc_channel_rx(goipc_channel *c)
{
	return c->rx;
}

size_t goipc_channel_max_message_size(const goipc_channel *c)
{
	return goipc_queue_max_message_size(c->tx);
}

int goipc_channel_send(goipc_channel *c, uint32_t type, const void *payload, size_t len, int64_t timeout_ns)
{
	return goipc_queue_send(c->tx, type, payload, len, timeout_ns);
}

int goipc_channel_recv(goipc_channel *c, void *dst, size_t cap, uint32_t *type, size_t *len, int64_t timeout_ns)
{
	return goipc_queue_recv(c->rx, dst, cap, type, len, timeout_ns);
}

/* The receiving end closes first. The signal after it wakes a peer parked in
 * a receive, which then finds this side gone. */
int goipc_channel_close(goipc_channel *c)
{
	int rc = goipc_queue_close(c->rx);
	goipc__queue_wake_receiver(c->tx);
	int rc2 = goipc_queue_close(c->tx);
	return rc != GOIPC_OK ? rc : rc2;
}

int goipc_channel_unlink(goipc_channel *c)
{
	int rc = goipc_queue_unlink(c->tx);
	int rc2 = goipc_queue_unlink(c->rx);
	return rc != GOIPC_OK ? rc : rc2;
}

void goipc_channel_destroy(goipc_channel *c)
{
	if (c == NULL)
		return;
	/* A close of a closed channel changes nothing. */
	goipc_channel_close(c);
	goipc_queue_destroy(c->tx);
	goipc_queue_destroy(c->rx);
	free(c);
}
