package ipc

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/wow-look-at-my/go-shm"
)

const DefaultCapacity = 1 << 20

// An Option configures a queue, channel or connection at creation.
type Option func(*config)

type config struct {
	capacity int
}

func defaultConfig() config {
	return config{capacity: DefaultCapacity}
}

func (c *config) apply(opts []Option) error {
	for _, o := range opts {
		o(c)
	}
	if c.capacity < MinCapacity || c.capacity&(c.capacity-1) != 0 {
		return ErrInvalidCapacity
	}
	return nil
}

// WithCapacity sets the data region of each underlying ring, in bytes. It must
// be a power of of at least MinCapacity. Only the creating side decides it; an
// opener reads the value out of the segment.
func WithCapacity(bytes int) Option {
	return func(c *config) { c.capacity = bytes }
}

// A Queue is a named multi-producer single-consumer message queue in shared
// memory.
//
// Any number of processes may Send. Exactly a single goroutine, in a single
// process, may Recv. A side that cannot proceed parks on a kernel wait; it
// never spins and never sleeps for a guessed interval. A side that can proceed
// makes no system call at all.
type Queue struct {
	name     string
	seg      *shm.SharedMemory
	ring     *Ring
	notEmpty *Event
	notFull  *Event
	cfg      config
	owner    bool
	closed   bool
}

// CreateQueue creates the named queue, replacing any stale instance of it. The
// creator owns the name and should Unlink it when finished.
func CreateQueue(name string, opts ...Option) (*Queue, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	cfg := defaultConfig()
	if err := cfg.apply(opts); err != nil {
		return nil, err
	}

	q := &Queue{name: name, cfg: cfg, owner: true}

	// The events come earliest and the segment last, so a peer that finds
	// the segment also finds the events. The reverse order would hand an
	// opener a segment whose wakeup channels do not exist yet.
	var err error
	if q.notEmpty, err = CreateEvent(name + ".ne"); err != nil {
		q.unwind()
		return nil, fmt.Errorf("ipc: create queue %q: %w", name, err)
	}
	if q.notFull, err = CreateEvent(name + ".nf"); err != nil {
		q.unwind()
		return nil, fmt.Errorf("ipc: create queue %q: %w", name, err)
	}
	if q.seg, err = shm.Create(name, RingSize(cfg.capacity)); err != nil {
		q.unwind()
		return nil, fmt.Errorf("ipc: create queue %q: %w", name, err)
	}
	if q.ring, err = InitRing(q.seg.Data()); err != nil {
		q.unwind()
		return nil, fmt.Errorf("ipc: create queue %q: %w", name, err)
	}
	return q, nil
}

// OpenQueue attaches to a queue another process created. Capacity comes from
// the segment, so WithCapacity has no effect here.
func OpenQueue(name string, opts ...Option) (*Queue, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	cfg := defaultConfig()
	if err := cfg.apply(opts); err != nil {
		return nil, err
	}

	seg, err := shm.Open(name)
	if err != nil {
		return nil, fmt.Errorf("ipc: open queue %q: %w", name, err)
	}
	q := &Queue{name: name, seg: seg, cfg: cfg}

	if q.ring, err = AttachRing(seg.Data()); err != nil {
		q.unwind()
		return nil, fmt.Errorf("ipc: open queue %q: %w", name, err)
	}
	if q.notEmpty, err = OpenEvent(name + ".ne"); err != nil {
		q.unwind()
		return nil, fmt.Errorf("ipc: open queue %q: %w", name, err)
	}
	if q.notFull, err = OpenEvent(name + ".nf"); err != nil {
		q.unwind()
		return nil, fmt.Errorf("ipc: open queue %q: %w", name, err)
	}
	q.cfg.capacity = q.ring.Capacity()
	return q, nil
}

// unwind releases whatever a failed constructor managed to acquire.
func (q *Queue) unwind() {
	if q.notEmpty != nil {
		q.notEmpty.Close()
	}
	if q.notFull != nil {
		q.notFull.Close()
	}
	if q.seg != nil {
		q.seg.Close()
	}
	if q.owner {
		unlinkEventImpl(q.name + ".ne")
		unlinkEventImpl(q.name + ".nf")
		if q.seg != nil {
			q.seg.Unlink()
		}
	}
}

// Name returns the name the queue was created or opened with.
func (q *Queue) Name() string { return q.name }

// Capacity returns the data region of the queue in bytes.
func (q *Queue) Capacity() int { return q.ring.Capacity() }

// MaxMessageSize returns the largest payload a single message may carry.
func (q *Queue) MaxMessageSize() int { return q.ring.MaxMessageSize() }

// Ring exposes the underlying buffer for callers that want the non-blocking
// primitives directly.
func (q *Queue) Ring() *Ring { return q.ring }

// wakeReceiver signals only when a receiver is parked, so an active queue
// makes no system call at all.
func (q *Queue) wakeReceiver() {
	if q.ring.hdr.recvWaiters.Load() > 0 {
		q.notEmpty.Signal()
	}
}

// wakeSenders releases every parked sender. Freed space fits a different
// number of senders than it fits messages, so the count cannot be derived;
// waking all of them lets each re-check its own size and park again.
func (q *Queue) wakeSenders() {
	if w := q.ring.hdr.sendWaiters.Load(); w > 0 {
		q.notFull.SignalN(int(w))
	}
}

// park runs attempt, and while it reports blocked, waits on ev for a peer to
// change the ring.
//
// The waiter count is published before the next attempt, and a peer reads it
// after it publishes its own change. Both are sequentially consistent, so any
// of both sees the other: the wait below cannot begin after the wakeup it
// needs has already been decided against.
func park(ctx context.Context, ev *Event, waiters *atomic.Int32, blocked error, attempt func() error) error {
	for {
		err := attempt()
		if err != blocked {
			return err
		}

		waiters.Add(1)
		err = attempt()
		if err != blocked {
			waiters.Add(-1)
			return err
		}
		err = ev.Wait(ctx)
		waiters.Add(-1)
		if err != nil {
			return err
		}
	}
}

// TrySend copies payload into the queue without blocking. It returns ErrFull
// when the queue has no room.
func (q *Queue) TrySend(payload []byte) error {
	return q.TrySendTyped(0, payload)
}

// TrySendTyped is TrySend with an explicit message type.
func (q *Queue) TrySendTyped(typ uint32, payload []byte) error {
	if q.closed {
		return ErrClosed
	}
	if err := q.ring.TryWrite(typ, payload); err != nil {
		return err
	}
	q.wakeReceiver()
	return nil
}

// Send copies payload into the queue, waiting for room if the queue is full.
// It returns the context error if ctx ends earliest.
func (q *Queue) Send(ctx context.Context, payload []byte) error {
	return q.SendTyped(ctx, 0, payload)
}

// SendTyped is Send with an explicit message type.
func (q *Queue) SendTyped(ctx context.Context, typ uint32, payload []byte) error {
	if q.closed {
		return ErrClosed
	}
	err := park(ctx, q.notFull, &q.ring.hdr.sendWaiters, ErrFull, func() error {
		return q.ring.TryWrite(typ, payload)
	})
	if err != nil {
		return err
	}
	q.wakeReceiver()
	return nil
}

// Claim reserves room for length bytes in the queue and returns the region to
// fill in place, waiting for room if the queue is full.
//
// The returned claim must be committed or aborted; until then the receiver
// stops at it. This is the allocation-free send path.
func (q *Queue) Claim(ctx context.Context, typ uint32, length int) (Claim, error) {
	if q.closed {
		return Claim{}, ErrClosed
	}
	var c Claim
	err := park(ctx, q.notFull, &q.ring.hdr.sendWaiters, ErrFull, func() error {
		var err error
		c, err = q.ring.TryClaim(typ, length)
		return err
	})
	if err != nil {
		return Claim{}, err
	}
	return c, nil
}

// Commit publishes a claim and wakes a parked receiver.
func (q *Queue) Commit(c Claim) {
	c.Commit()
	q.wakeReceiver()
}

// TryRecv copies the next message into dst and returns its type and payload.
// It returns ErrEmpty when the queue holds nothing.
//
// When dst has room the payload is written into it and no allocation happens.
func (q *Queue) TryRecv(dst []byte) (uint32, []byte, error) {
	if q.closed {
		return 0, nil, ErrClosed
	}
	typ, msg, err := q.ring.TryRecv(dst)
	if err != nil {
		return 0, nil, err
	}
	q.wakeSenders()
	return typ, msg, nil
}

// Recv waits for the next message and returns its type and a fresh copy of the
// payload. It returns the context error if ctx ends earliest.
func (q *Queue) Recv(ctx context.Context) (uint32, []byte, error) {
	return q.RecvInto(ctx, nil)
}

// RecvInto is Recv with a caller-supplied buffer. The returned slice aliases
// dst when dst is large enough, so a reused buffer makes receiving allocation
// free.
func (q *Queue) RecvInto(ctx context.Context, dst []byte) (uint32, []byte, error) {
	if q.closed {
		return 0, nil, ErrClosed
	}
	var (
		typ uint32
		msg []byte
	)
	err := park(ctx, q.notEmpty, &q.ring.hdr.recvWaiters, ErrEmpty, func() error {
		var err error
		typ, msg, err = q.ring.TryRecv(dst)
		return err
	})
	if err != nil {
		return 0, nil, err
	}
	q.wakeSenders()
	return typ, msg, nil
}

// Each payload aliases shared memory and is valid only for the duration of the
// call, so a caller that keeps a single copies it.
//
// Draining a burst in a single call amortizes the cursor update and the
// sender wakeup across the whole batch.
func (q *Queue) ReadBatch(ctx context.Context, limit int, fn ReadFunc) (int, error) {
	if q.closed {
		return 0, ErrClosed
	}
	var count int
	err := park(ctx, q.notEmpty, &q.ring.hdr.recvWaiters, ErrEmpty, func() error {
		n, err := q.ring.Read(limit, fn)
		count = n
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrEmpty
		}
		return nil
	})
	if err != nil {
		return count, err
	}
	q.wakeSenders()
	return count, nil
}

// Close releases this process's handles on the queue. Other processes keep
// theirs. Close does not remove the name; see Unlink.
func (q *Queue) Close() error {
	if q.closed {
		return ErrClosed
	}
	q.closed = true

	err := q.notEmpty.Close()
	if cerr := q.notFull.Close(); err == nil {
		err = cerr
	}
	if cerr := q.seg.Close(); err == nil {
		err = cerr
	}
	return err
}

// Unlink removes the queue's name so no further process can open it. Handles
// already open stay usable until they close.
func (q *Queue) Unlink() error {
	err := q.seg.Unlink()
	if uerr := unlinkEventImpl(q.name + ".ne"); err == nil {
		err = uerr
	}
	if uerr := unlinkEventImpl(q.name + ".nf"); err == nil {
		err = uerr
	}
	return err
}
