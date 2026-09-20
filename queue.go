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
// be a power of 2, and at least MinCapacity. Only the creating side decides
// it. An opener reads the value out of the segment.
func WithCapacity(bytes int) Option {
	return func(c *config) { c.capacity = bytes }
}

// A Queue is a named multi-producer single-consumer message queue in shared
// memory.
//
// Any number of processes may Send. Recv has a single caller, in a single
// process. A side that cannot proceed parks on a kernel wait. It never spins,
// and it never sleeps for a guessed interval. A side that can proceed makes no
// system call at all.
type Queue struct {
	name     string
	seg      *shm.SharedMemory
	ring     *Ring
	notEmpty *Event
	notFull  *Event
	cfg      config
	owner    bool

	// closing and active gate the unmap. An operation in flight holds a
	// pointer into the segment, so Close must wait for it to leave rather
	// than pull the mapping out from under it.
	closing atomic.Bool
	active  atomic.Int64
	drained chan struct{}
}

// enter registers an operation against the mapping. A false return means the
// queue is closing, and the caller must then leave shared memory alone.
func (q *Queue) enter() bool {
	q.active.Add(1)
	// Both this load and the store in Close are sequentially consistent, so
	// each side observes the other. Either Close waits for this operation,
	// or this operation backs out.
	if q.closing.Load() {
		q.leave()
		return false
	}
	return true
}

func (q *Queue) leave() {
	if q.active.Add(-1) == 0 && q.closing.Load() {
		select {
		case q.drained <- struct{}{}:
		default:
		}
	}
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

	q := &Queue{name: name, cfg: cfg, owner: true, drained: make(chan struct{}, 1)}

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
	q := &Queue{name: name, seg: seg, cfg: cfg, drained: make(chan struct{}, 1)}

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
//
// The ring points into the mapping and carries no close guard of its own, so a
// caller that keeps it must not use it after Close.
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
// after it publishes its own change. Both are sequentially consistent, so each
// side sees the other. The wait below cannot begin after a peer has already
// decided against the wakeup it needs.
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
	if !q.enter() {
		return ErrClosed
	}
	defer q.leave()

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
	if !q.enter() {
		return ErrClosed
	}
	defer q.leave()

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
	if !q.enter() {
		return Claim{}, ErrClosed
	}
	defer q.leave()

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
//
// A claim points into the mapping, so it must be committed or aborted before
// the queue is closed.
func (q *Queue) Commit(c Claim) {
	if !q.enter() {
		return
	}
	defer q.leave()

	c.Commit()
	q.wakeReceiver()
}

// Abort discards a claim. The reader skips the region and reclaims it.
func (q *Queue) Abort(c Claim) {
	if !q.enter() {
		return
	}
	defer q.leave()

	c.Abort()
	q.wakeSenders()
}

// TryRecv copies the next message into dst and returns its type and payload.
// It returns ErrEmpty when the queue holds nothing.
//
// When dst has room the payload is written into it and no allocation happens.
func (q *Queue) TryRecv(dst []byte) (uint32, []byte, error) {
	if !q.enter() {
		return 0, nil, ErrClosed
	}
	defer q.leave()

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
	if !q.enter() {
		return 0, nil, ErrClosed
	}
	defer q.leave()

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

// ReadBatch passes up to limit ready messages to fn and returns how many it
// passed. It waits for at least a message to arrive.
//
// Each payload aliases shared memory and stays valid only for the duration of
// the call. A caller that keeps a payload must copy it.
//
// A batch amortizes the cursor update and the sender wakeup across every
// message it drains.
func (q *Queue) ReadBatch(ctx context.Context, limit int, fn ReadFunc) (int, error) {
	if !q.enter() {
		return 0, ErrClosed
	}
	defer q.leave()

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
//
// An operation blocked in Send or Recv returns ErrClosed, and Close waits for
// every operation to let go before it unmaps the segment. Calling it from
// another goroutine is therefore safe.
func (q *Queue) Close() error {
	if !q.closing.CompareAndSwap(false, true) {
		return ErrClosed
	}

	// Closing the events releases whatever is parked on them, which is what
	// lets the in-flight count fall to empty.
	err := q.notEmpty.Close()
	if cerr := q.notFull.Close(); err == nil {
		err = cerr
	}
	if q.active.Load() > 0 {
		<-q.drained
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
