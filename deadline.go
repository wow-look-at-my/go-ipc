package ipc

import (
	"context"
	"os"
	"sync"
	"time"
)

// A deadline is one direction's deadline on a Conn. A new deadline also
// reaches the operations already blocked, as net.Conn requires.
type deadline struct {
	mu      sync.Mutex
	timer   *time.Timer
	gen     uint64
	expired bool
	next    uint64
	waiting map[uint64]context.CancelCauseFunc
}

// set replaces the deadline. The zero time means none.
func (d *deadline) set(at time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.gen++
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	d.expired = false
	if at.IsZero() {
		return
	}
	wait := time.Until(at)
	if wait <= 0 {
		d.expireLocked()
		return
	}
	gen := d.gen
	d.timer = time.AfterFunc(wait, func() { d.fire(gen) })
}

// fire expires the deadline, unless a later set replaced it.
func (d *deadline) fire(gen uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if gen == d.gen {
		d.expireLocked()
	}
}

func (d *deadline) expireLocked() {
	d.expired = true
	for _, cancel := range d.waiting {
		cancel(os.ErrDeadlineExceeded)
	}
}

// begin starts an operation under the deadline. The context ends when the
// deadline passes, or when base ends. done must follow.
func (d *deadline) begin(base context.Context) (ctx context.Context, done func(), err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.expired {
		return nil, nil, os.ErrDeadlineExceeded
	}
	ctx, cancel := context.WithCancelCause(base)
	d.next++
	key := d.next
	if d.waiting == nil {
		d.waiting = make(map[uint64]context.CancelCauseFunc)
	}
	d.waiting[key] = cancel
	return ctx, func() {
		d.mu.Lock()
		delete(d.waiting, key)
		d.mu.Unlock()
		cancel(nil)
	}, nil
}
