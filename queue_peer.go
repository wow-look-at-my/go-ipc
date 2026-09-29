package ipc

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// A peerWatch follows the receiver of a queue, for a handle that sends. It
// learns of an exit from the kernel. It does not poll.
type peerWatch struct {
	mu     sync.Mutex
	id     atomic.Uint64
	gone   atomic.Bool
	failed atomic.Pointer[error]
	cancel func()
}

func (w *peerWatch) state() error {
	if !w.gone.Load() {
		return nil
	}
	if failed := w.failed.Load(); failed != nil {
		return fmt.Errorf("ipc: cannot watch the receiver: %w", *failed)
	}
	return ErrPeerGone
}

func (w *peerWatch) stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cancel != nil {
		w.cancel()
		w.cancel = nil
	}
}

// checkPeer reports ErrPeerGone once the receiver of the queue has closed or
// exited. The first check of a new receiver starts a watch on its process.
// Every later check is an atomic load.
func (q *Queue) checkPeer() error {
	id := procID(q.ring.hdr.consumer.Load())
	switch id {
	case noProc:
		return ErrPeerGone
	case pendingProc, q.self:
		return nil
	}
	// A receiver in another pid namespace cannot be watched from here. Its
	// Close still reaches this sender through the consumer field.
	if q.ring.hdr.consumerNS.Load() != q.ns {
		return nil
	}
	if procID(q.peer.id.Load()) == id {
		return q.peer.state()
	}
	return q.watchPeer(id)
}

func (q *Queue) watchPeer(id procID) error {
	w := &q.peer
	w.mu.Lock()
	defer w.mu.Unlock()
	if procID(w.id.Load()) == id {
		return w.state()
	}
	if w.cancel != nil {
		w.cancel()
		w.cancel = nil
	}
	w.failed.Store(nil)
	// A receiver that is already dead is reported now. The watch would report it too, but only after this call returns.
	w.gone.Store(isDead(id))
	w.id.Store(uint64(id))
	if w.gone.Load() {
		return ErrPeerGone
	}
	cancel, err := onExit(id, func(err error) { q.peerExited(id, err) })
	if err != nil {
		w.id.Store(0)
		return fmt.Errorf("ipc: cannot watch the receiver: %w", err)
	}
	w.cancel = cancel
	return nil
}

// peerExited marks the receiver gone and wakes everything in this process
// that waits on it.
func (q *Queue) peerExited(id procID, err error) {
	if !q.enter() {
		return
	}
	defer q.leave()
	if procID(q.peer.id.Load()) != id {
		return
	}
	if err != nil {
		q.peer.failed.Store(&err)
	}
	q.peer.gone.Store(true)
	q.wakeSenders()
	for _, fn := range q.onPeerGone {
		fn()
	}
}

// A producerWatch follows the producers whose claims stop the receiver.
type producerWatch struct {
	mu      sync.Mutex
	cancels map[procID]func()
	closed  bool
	// failed holds a kernel wait that failed. A receive reports it rather than watch the same producer again.
	failed error
}

func (w *producerWatch) stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	for _, cancel := range w.cancels {
		cancel()
	}
	w.cancels = nil
}

func (q *Queue) watchProducer(id procID) error {
	w := &q.producers
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	if w.failed != nil {
		return w.failed
	}
	if _, ok := w.cancels[id]; ok {
		return nil
	}
	cancel, err := onExit(id, func(err error) { q.producerExited(id, err) })
	if err != nil {
		return fmt.Errorf("ipc: cannot watch a producer: %w", err)
	}
	if w.cancels == nil {
		w.cancels = make(map[procID]func())
	}
	w.cancels[id] = cancel
	return nil
}

// producerExited wakes the receiver, which then reclaims the claim.
func (q *Queue) producerExited(id procID, err error) {
	if !q.enter() {
		return
	}
	defer q.leave()
	q.producers.mu.Lock()
	delete(q.producers.cancels, id)
	if err != nil && q.producers.failed == nil {
		q.producers.failed = fmt.Errorf("ipc: cannot watch a producer: %w", err)
	}
	q.producers.mu.Unlock()
	q.notEmpty.Signal()
}

// unstall looks at the claim that stops the receiver, if one does. It returns
// true after it turns the claim of a dead producer into padding. A claim
// whose producer lives gets a watch on that producer instead.
func (q *Queue) unstall() (bool, error) {
	st, ok := q.ring.stalled()
	if !ok {
		return false, nil
	}
	var (
		dead []int
		end  uint64
	)
	for _, idx := range st.slots {
		slot := &q.ring.hdr.slots[idx]
		owner := procID(slot.owner.Load())
		at, size := slot.at.Load(), slot.size.Load()
		// The slot may have changed since stalled read it. Only a slot that
		// still covers the stall counts.
		if owner == noProc || at == noIntent || at > st.at || st.at >= at+size {
			continue
		}
		// A claim of this process, or of a process in another pid namespace, is
		// left alone.
		if owner == q.self || slot.ns.Load() != q.ns {
			return false, nil
		}
		if !isDead(owner) {
			return false, q.watchProducer(owner)
		}
		// Dead producers that claim different ranges here leave no way to
		// tell which claim is real.
		if len(dead) > 0 && at+size != end {
			return false, fmt.Errorf("%w: dead producers disagree on a claim", ErrCorrupt)
		}
		end = at + size
		dead = append(dead, idx)
	}
	if len(dead) == 0 {
		return false, nil
	}
	return q.ring.reclaim(st, end, dead), nil
}

// takeSlot hands out a claim slot this handle owns, and takes a new one from
// the ring when none is idle.
func (q *Queue) takeSlot() (int, error) {
	q.slotMu.Lock()
	if n := len(q.idle); n > 0 {
		slot := q.idle[n-1]
		q.idle = q.idle[:n-1]
		q.slotMu.Unlock()
		return slot, nil
	}
	q.slotMu.Unlock()

	known := make(map[procID]bool)
	slot, err := q.ring.acquireSlot(q.self, q.ns, func(id procID) bool {
		dead, ok := known[id]
		if !ok {
			dead = isDead(id)
			known[id] = dead
		}
		return dead
	})
	if err != nil {
		return -1, err
	}
	q.slotMu.Lock()
	q.owned = append(q.owned, slot)
	q.slotMu.Unlock()
	return slot, nil
}

func (q *Queue) putSlot(slot int) {
	q.slotMu.Lock()
	q.idle = append(q.idle, slot)
	q.slotMu.Unlock()
}
