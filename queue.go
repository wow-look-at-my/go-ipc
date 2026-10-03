package ipc

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

// WithCapacity sets the data region of each underlying ring, in bytes. Only
// the creating side decides it.
func WithCapacity(bytes int) Option {
	return func(c *config) { c.capacity = bytes }
}

// A Queue is a named multi-producer single-consumer message queue in shared memory.
//
// Any number of processes may Send. Only the handle CreateQueue returns may receive, and its receives run one at a time. A side that cannot proceed parks on a kernel wait. It never spins, and it never sleeps for a guessed interval. A side that can proceed makes no system call at all.
type Queue struct {
	name     string
	inc      string
	seg      *shm.SharedMemory
	ring     *Ring
	notEmpty *Event
	notFull  *Event
	cfg      config
	self     procID
	// lock holds the name. Only the creator has one.
	lock *nameLock
	// reader reports whether this handle owns the receiving end.
	reader bool
	recvMu sync.Mutex

	slotMu sync.Mutex
	idle   []int
	owned  []int

	peer      peerWatch
	producers producerWatch
	// peerErr reports why the far side of a channel is gone.
	peerErr func() error
	// onPeerGone runs after the receiver of this queue exits or closes.
	onPeerGone []func()

	// closing and active gate the unmap.
	closing atomic.Bool
	active  atomic.Int64
	drained chan struct{}
}

// enter registers an operation against the mapping.
func (q *Queue) enter() bool {
	q.active.Add(1)
	// Both this load and the store in Close are sequentially consistent, so each
	// side observes the other.
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

// CreateQueue creates the named queue and returns its receiving end. It
// returns ErrInUse while a live process holds the name.
func CreateQueue(name string, opts ...Option) (*Queue, error) {
	return createQueue(name, opts, false)
}

// createQueue makes a new instance of the named queue. This process reads
// it, unless pending leaves the reader to a channel peer that connects later.
func createQueue(name string, opts []Option, pending bool) (*Queue, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	cfg := defaultConfig()
	if err := cfg.apply(opts); err != nil {
		return nil, err
	}
	sweepStale()

	lock, err := lockName(name)
	if err != nil {
		return nil, fmt.Errorf("ipc: create queue %q: %w", name, err)
	}
	q := newQueue(name, cfg)
	q.lock = lock
	reader := q.self
	if pending {
		reader = pendingProc
	}
	q.reader = !pending
	if err := q.build(reader); err != nil {
		q.unwind()
		return nil, fmt.Errorf("ipc: create queue %q: %w", name, err)
	}
	return q, nil
}

func newQueue(name string, cfg config) *Queue {
	return &Queue{name: name, cfg: cfg, self: selfID(), drained: make(chan struct{}, 1)}
}

// build makes the instance while this handle holds the name. The name file
// points at the instance only after the instance is complete.
func (q *Queue) build(reader procID) error {
	if old := q.lock.previous(); old != "" {
		if err := removeInstance(q.name, old); err != nil {
			return err
		}
	}
	var err error
	if q.inc, err = newIncarnation(); err != nil {
		return err
	}
	inst := instanceName(q.name, q.inc)
	if q.notEmpty, err = CreateEvent(inst + ".ne"); err != nil {
		return err
	}
	if q.notFull, err = CreateEvent(inst + ".nf"); err != nil {
		return err
	}
	if q.seg, err = shm.Create(inst, RingSize(q.cfg.capacity)); err != nil {
		return err
	}
	if q.ring, err = initRing(q.seg.Data(), reader); err != nil {
		return err
	}
	return q.lock.publish(q.inc)
}

// OpenQueue attaches to a queue another process created, as a sender. It
// returns ErrPeerGone when the receiver has already exited or closed.
// Capacity comes from the segment, so WithCapacity has no effect here.
func OpenQueue(name string, opts ...Option) (*Queue, error) {
	q, err := openQueue(name, opts)
	if err != nil {
		return nil, err
	}
	if err := q.checkPeer(); err != nil {
		q.Close()
		return nil, fmt.Errorf("ipc: open queue %q: %w", name, err)
	}
	return q, nil
}

func openQueue(name string, opts []Option) (*Queue, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	cfg := defaultConfig()
	if err := cfg.apply(opts); err != nil {
		return nil, err
	}
	q := newQueue(name, cfg)
	var err error
	if q.inc, err = readName(name); err != nil {
		return nil, fmt.Errorf("ipc: open queue %q: %w", name, err)
	}
	if err := q.attach(); err != nil {
		q.unwind()
		return nil, fmt.Errorf("ipc: open queue %q: %w", name, err)
	}
	q.cfg.capacity = q.ring.Capacity()
	return q, nil
}

func (q *Queue) attach() error {
	inst := instanceName(q.name, q.inc)
	var err error
	if q.seg, err = shm.Open(inst); err != nil {
		return err
	}
	if q.ring, err = AttachRing(q.seg.Data()); err != nil {
		return err
	}
	if q.notEmpty, err = OpenEvent(inst + ".ne"); err != nil {
		return err
	}
	q.notFull, err = OpenEvent(inst + ".nf")
	return err
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
	if q.lock != nil {
		if q.inc != "" {
			removeInstance(q.name, q.inc)
		}
		q.lock.release()
	}
}

// Name returns the name the queue was created or opened with.
func (q *Queue) Name() string { return q.name }

// Capacity returns the data region of the queue in bytes.
func (q *Queue) Capacity() int { return q.ring.Capacity() }

// MaxMessageSize returns the largest payload a single message may carry.
func (q *Queue) MaxMessageSize() int { return q.ring.MaxMessageSize() }

// Ring exposes the underlying buffer for callers that want the non-blocking primitives directly.
func (q *Queue) Ring() *Ring { return q.ring }

// wakeReceiver signals only when a receiver is parked, so an active queue
// makes no system call at all.
func (q *Queue) wakeReceiver() {
	if q.ring.hdr.recvWaiters.Load() > 0 {
		q.notEmpty.Signal()
	}
}

// wakeSenders releases every parked sender.
func (q *Queue) wakeSenders() {
	if w := q.ring.hdr.sendWaiters.Load(); w > 0 {
		q.notFull.SignalN(int(w))
	}
}

// park runs attempt, and while it reports blocked, waits on ev for a peer to
// change the ring.
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
		if errors.Is(err, ErrPeerGone) {
			// The event creator closed after its last write. That write is in the ring, so read it before peer-gone.
			if aerr := attempt(); aerr != blocked {
				return aerr
			}
			return err
		}
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

	if err := q.write(typ, payload); err != nil {
		return err
	}
	q.wakeReceiver()
	return nil
}

// write is a single attempt to copy payload into the ring under a claim slot
// of this process.
func (q *Queue) write(typ uint32, payload []byte) error {
	if err := q.checkPeer(); err != nil {
		return err
	}
	slot, err := q.takeSlot()
	if err != nil {
		return err
	}
	err = q.ring.tryWrite(slot, typ, payload)
	q.putSlot(slot)
	return err
}

// claim is a single attempt to reserve length bytes under a claim slot of
// this process. The slot stays with the claim until its commit or abort.
func (q *Queue) claim(typ uint32, length int) (Claim, error) {
	if err := q.checkPeer(); err != nil {
		return Claim{}, err
	}
	slot, err := q.takeSlot()
	if err != nil {
		return Claim{}, err
	}
	c, err := q.ring.tryClaim(slot, typ, length)
	if err != nil {
		q.putSlot(slot)
	}
	return c, err
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
		return q.write(typ, payload)
	})
	if err != nil {
		return q.sawPeerGone(err)
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
		c, err = q.claim(typ, length)
		return err
	})
	if err != nil {
		return Claim{}, q.sawPeerGone(err)
	}
	return c, nil
}

// Commit publishes a claim and wakes a parked receiver.
func (q *Queue) Commit(c Claim) {
	if !q.enter() {
		return
	}
	defer q.leave()

	c.Commit()
	q.putSlot(c.slot)
	q.wakeReceiver()
}

// Abort discards a claim. The reader skips the region and reclaims it.
func (q *Queue) Abort(c Claim) {
	if !q.enter() {
		return
	}
	defer q.leave()

	// An abort frees nothing by itself: it commits a padding record that only the receiver can step over.
	c.Abort()
	q.putSlot(c.slot)
	q.wakeReceiver()
}

// receive runs a single read and reports whether it freed ring space.
//
// Padding that a sender aborted frees space without producing a message, so the space has to be announced even when the read delivers nothing. Without that, a sender parked behind an aborted claim never wakes.
//
// A read that finds a claim in its way checks the claim's producer. A dead producer's claim becomes padding, and the read runs again.
func (q *Queue) receive(limit int, fn ReadFunc) (int, error) {
	// The peer check comes before the read.
	var gone error
	if q.peerErr != nil {
		gone = q.peerErr()
	}
	for {
		before := q.ring.hdr.head.Load()
		n, err := q.ring.Read(limit, fn)
		if q.ring.hdr.head.Load() != before {
			q.wakeSenders()
		}
		if err != nil || n > 0 {
			return n, err
		}
		reclaimed, err := q.unstall()
		if err != nil {
			return 0, err
		}
		if !reclaimed {
			break
		}
	}
	if gone != nil {
		return 0, gone
	}
	return 0, ErrEmpty
}

// recvOne copies the next message into dst through the shared read path.
func (q *Queue) recvOne(dst []byte) (uint32, []byte, error) {
	var (
		typ uint32
		out []byte
	)
	_, err := q.receive(1, func(t uint32, payload []byte) {
		typ = t
		if cap(dst) >= len(payload) {
			out = dst[:len(payload)]
		} else {
			out = make([]byte, len(payload))
		}
		copy(out, payload)
	})
	if err != nil {
		return 0, nil, err
	}
	return typ, out, nil
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
	if !q.reader {
		return 0, nil, ErrNotConsumer
	}
	q.recvMu.Lock()
	defer q.recvMu.Unlock()

	return q.recvOne(dst)
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
	if !q.reader {
		return 0, nil, ErrNotConsumer
	}
	q.recvMu.Lock()
	defer q.recvMu.Unlock()

	var (
		typ uint32
		msg []byte
	)
	err := park(ctx, q.notEmpty, &q.ring.hdr.recvWaiters, ErrEmpty, func() error {
		var err error
		typ, msg, err = q.recvOne(dst)
		return err
	})
	if err != nil {
		return 0, nil, err
	}
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
	if !q.reader {
		return 0, ErrNotConsumer
	}
	q.recvMu.Lock()
	defer q.recvMu.Unlock()

	var count int
	err := park(ctx, q.notEmpty, &q.ring.hdr.recvWaiters, ErrEmpty, func() error {
		var err error
		count, err = q.receive(limit, fn)
		return err
	})
	if err != nil {
		return count, err
	}
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

	// The receiving end goes first, so a sender parked on a full queue
	// wakes and finds nobody left to drain it.
	if q.reader {
		q.ring.hdr.consumer.CompareAndSwap(uint64(q.self), uint64(noProc))
		q.wakeSenders()
	}
	q.peer.stop()
	q.producers.stop()

	// Closing the events releases whatever is parked on them, which is what lets the in-flight count fall to empty.
	err := q.notEmpty.Close()
	if cerr := q.notFull.Close(); err == nil {
		err = cerr
	}
	if q.active.Load() > 0 {
		<-q.drained
	}

	q.slotMu.Lock()
	for _, slot := range q.owned {
		q.ring.dropSlot(q.self, slot)
	}
	q.owned, q.idle = nil, nil
	q.slotMu.Unlock()

	if cerr := q.seg.Close(); err == nil {
		err = cerr
	}
	if q.lock != nil {
		if cerr := q.lock.release(); err == nil {
			err = cerr
		}
	}
	return err
}

// Unlink removes the queue's name so no further process can open it. Handles
// already open stay usable until they close.
func (q *Queue) Unlink() error {
	err := removeInstance(q.name, q.inc)
	if uerr := unlinkName(q.name, q.inc); err == nil {
		err = uerr
	}
	return err
}
