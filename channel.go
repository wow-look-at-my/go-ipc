package ipc

import (
	"context"
	"fmt"
)

// A Channel is a bidirectional named endpoint between a pair of processes.
type Channel struct {
	name  string
	tx    *Queue
	rx    *Queue
	owner bool
}

const (
	chanCreatorToOpener = ".c2o"
	chanOpenerToCreator = ".o2c"
)

// CreateChannel creates both directions of the named channel. It returns
// ErrInUse while a live process holds the name. The creator should Unlink the
// name when finished.
func CreateChannel(name string, opts ...Option) (*Channel, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	// The direction to the peer has no reader until a peer connects.
	tx, err := createQueue(name+chanCreatorToOpener, opts, true)
	if err != nil {
		return nil, err
	}
	rx, err := createQueue(name+chanOpenerToCreator, opts, false)
	if err != nil {
		tx.Close()
		tx.Unlink()
		return nil, err
	}
	return newChannel(name, tx, rx, true), nil
}

// OpenChannel attaches to a channel the peer created. The directions are
// swapped relative to the creator, so both sides use Send and Recv alike. It
// returns ErrPeerGone when the creator has gone, and ErrInUse when another
// peer holds the channel.
func OpenChannel(name string, opts ...Option) (*Channel, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	tx, err := OpenQueue(name+chanOpenerToCreator, opts...)
	if err != nil {
		return nil, err
	}
	rx, err := openQueue(name+chanCreatorToOpener, opts)
	if err != nil {
		tx.Close()
		return nil, err
	}
	if !rx.ring.hdr.consumer.CompareAndSwap(uint64(pendingProc), uint64(rx.self)) {
		rx.Close()
		tx.Close()
		return nil, fmt.Errorf("ipc: open channel %q: %w", name, ErrInUse)
	}
	rx.reader = true
	ch := newChannel(name, tx, rx, false)
	// The creator may already be parked. It wakes to find its peer, and to start watching the peer's process.
	tx.notEmpty.Signal()
	rx.wakeSenders()
	return ch, nil
}

// newChannel links the directions. The peer is the receiver of tx, so rx
// reports that receiver's end, and the end wakes whatever waits on rx.
func newChannel(name string, tx, rx *Queue, owner bool) *Channel {
	rx.peerErr = tx.checkPeer
	tx.onPeerGone = append(tx.onPeerGone, func() { rx.notEmpty.Signal() })
	return &Channel{name: name, tx: tx, rx: rx, owner: owner}
}

// Name returns the name the channel was created or opened with.
func (c *Channel) Name() string { return c.name }

// Tx returns the outbound queue, for callers that want its non-blocking
// operations or its statistics.
func (c *Channel) Tx() *Queue { return c.tx }

// Rx returns the inbound queue.
func (c *Channel) Rx() *Queue { return c.rx }

// MaxMessageSize returns the largest payload a single message may carry.
func (c *Channel) MaxMessageSize() int { return c.tx.MaxMessageSize() }

// Send copies payload to the peer, waiting for room if the channel is full.
func (c *Channel) Send(ctx context.Context, payload []byte) error {
	return c.tx.SendTyped(ctx, 0, payload)
}

// SendTyped is Send with an explicit message type.
func (c *Channel) SendTyped(ctx context.Context, typ uint32, payload []byte) error {
	return c.tx.SendTyped(ctx, typ, payload)
}

// TrySend copies payload to the peer without blocking.
func (c *Channel) TrySend(payload []byte) error { return c.tx.TrySendTyped(0, payload) }

// TrySendTyped is TrySend with an explicit message type.
func (c *Channel) TrySendTyped(typ uint32, payload []byte) error {
	return c.tx.TrySendTyped(typ, payload)
}

// Claim reserves room for an outbound message the caller fills in place.
func (c *Channel) Claim(ctx context.Context, typ uint32, length int) (Claim, error) {
	return c.tx.Claim(ctx, typ, length)
}

// Commit publishes a claim and wakes the peer.
func (c *Channel) Commit(cl Claim) { c.tx.Commit(cl) }

// Abort discards a claim.
func (c *Channel) Abort(cl Claim) { c.tx.Abort(cl) }

// Recv waits for the next message from the peer.
func (c *Channel) Recv(ctx context.Context) (uint32, []byte, error) {
	return c.rx.RecvInto(ctx, nil)
}

// RecvInto is Recv with a caller-supplied buffer, which makes receiving
// allocation free when the buffer is reused and large enough.
func (c *Channel) RecvInto(ctx context.Context, dst []byte) (uint32, []byte, error) {
	return c.rx.RecvInto(ctx, dst)
}

// TryRecv copies the next message into dst without blocking.
func (c *Channel) TryRecv(dst []byte) (uint32, []byte, error) { return c.rx.TryRecv(dst) }

// ReadBatch drains up to limit ready messages in a single call.
func (c *Channel) ReadBatch(ctx context.Context, limit int, fn ReadFunc) (int, error) {
	return c.rx.ReadBatch(ctx, limit, fn)
}

// Close releases this process's handles on both directions.
func (c *Channel) Close() error {
	// The receiving end closes first. The signal after it wakes a peer parked in Recv, which then finds this side gone.
	err := c.rx.Close()
	c.tx.notEmpty.Signal()
	if cerr := c.tx.Close(); err == nil {
		err = cerr
	}
	return err
}

// Unlink removes both directions' names so no further process can open them.
func (c *Channel) Unlink() error {
	err := c.tx.Unlink()
	if uerr := c.rx.Unlink(); err == nil {
		err = uerr
	}
	return err
}
