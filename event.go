package ipc

import (
	"context"
	"sync"
	"sync/atomic"
)

// An Event is a named wake channel between processes.
//
// Signal releases a single waiter; SignalN releases n. A waiter parks the
// calling goroutine and releases its thread, so thousands of waiters cost no
// more than thousands of idle goroutines.
//
// An Event carries no state beyond pending wakeups. A signal delivered while
// nobody waits is kept and released to the next waiter, so a caller that
// checks its own condition before waiting never loses a wakeup. Wait may also
// return early with no matching signal, so callers must re-check the
// condition in a loop.
type Event struct {
	impl  eventImpl
	name  string
	owner bool

	// tokens is unbuffered on purpose: a wakeup the pump has taken but not
	// handed on is still owned by the pump, so an early signal is held
	// rather than dropped.
	tokens   chan struct{}
	closing  chan struct{}
	pumpDone chan struct{}

	// waiting counts the goroutines inside Wait. The pump reads the handle
	// only while that count is above empty, so a process with nobody
	// waiting never takes a wakeup meant for a peer process.
	waiting  atomic.Int32
	wakePump chan struct{}
	idle     chan struct{}

	pumpOnce  sync.Once
	shutOnce  sync.Once
	closeOnce sync.Once
}

// poke delivers a notification over a single-slot channel and never blocks.
// A slot that is already full carries the same meaning as the dropped send.
func poke(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// shut publishes the close exactly a single time, from Close or from the pump.
func (e *Event) shut() {
	e.shutOnce.Do(func() { close(e.closing) })
}

// CreateEvent creates the named event, replacing any stale instance of it.
// The caller owns the name and should call Unlink when finished with it.
func CreateEvent(name string) (*Event, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	impl, err := createEventImpl(name)
	if err != nil {
		return nil, err
	}
	return newEvent(impl, name, true), nil
}

// OpenEvent attaches to an event another process created.
func OpenEvent(name string) (*Event, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	impl, err := openEventImpl(name)
	if err != nil {
		return nil, err
	}
	return newEvent(impl, name, false), nil
}

func newEvent(impl eventImpl, name string, owner bool) *Event {
	return &Event{
		impl:     impl,
		name:     name,
		owner:    owner,
		tokens:   make(chan struct{}),
		closing:  make(chan struct{}),
		pumpDone: make(chan struct{}),
		wakePump: make(chan struct{}, 1),
		idle:     make(chan struct{}, 1),
	}
}

// Name returns the name the event was created or opened with.
func (e *Event) Name() string { return e.name }

// Signal releases a single waiter. It is safe from any goroutine or process
// that holds the event open, and it never blocks.
func (e *Event) Signal() error { return e.SignalN(1) }

// SignalN releases up to n waiters. Delivery is capped at the capacity of the
// underlying handle; a waiter that misses a token still re-checks its
// condition after any other waiter wakes.
func (e *Event) SignalN(n int) error {
	if n <= 0 {
		return nil
	}
	select {
	case <-e.closing:
		return ErrClosed
	default:
	}
	return e.impl.signal(n)
}

// Wait blocks until a signal arrives, ctx ends, or the event closes.
//
// The goroutine parks; it does not spin and does not hold an OS thread. A
// return of nil means a wakeup arrived, not that any particular condition now
// holds, so callers re-check their own state and call Wait again.
func (e *Event) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.pumpOnce.Do(func() { go e.pump() })

	// Announce this waiter before the select. The pump reads the handle only
	// while the count is above empty, so the announcement is what authorizes
	// this process to consume a wakeup at all.
	e.waiting.Add(1)
	poke(e.wakePump)
	defer func() {
		if e.waiting.Add(-1) == 0 {
			poke(e.idle)
		}
	}()

	select {
	case <-e.tokens:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-e.closing:
		return ErrClosed
	}
}

// pump moves wakeups off the operating system handle and onto e.tokens. A
// single pump serves every waiter in this process, so a kernel wait that does
// block a thread blocks no more than any of them per event.
//
// The pump is demand driven. A handle is shared between processes, so a pump
// that read it with nobody waiting here would take a wakeup that a waiter in
// another process needs, and that waiter would sleep on forever.
func (e *Event) pump() {
	defer close(e.pumpDone)
	for {
		if e.waiting.Load() == 0 {
			select {
			case <-e.wakePump:
			case <-e.closing:
				return
			}
			continue
		}

		if err := e.impl.wait(); err != nil {
			e.shut()
			return
		}

		// The waiter this token was fetched for can leave before the handoff
		// completes. Put the token back rather than hold it: another process
		// is entitled to it.
		select {
		case e.tokens <- struct{}{}:
		case <-e.idle:
			e.impl.signal(1)
		case <-e.closing:
			e.impl.signal(1)
			return
		}
	}
}

// Close releases this process's handle on the event. Other processes keep
// theirs. Waiters in this process return ErrClosed.
func (e *Event) Close() error {
	var err error
	e.closeOnce.Do(func() {
		e.shut()
		err = e.impl.stop()
		// Claiming pumpOnce stops a pump that has not started yet, so the
		// wait below cannot outlast a Close that raced a Wait.
		e.pumpOnce.Do(func() { close(e.pumpDone) })
		<-e.pumpDone
		if rerr := e.impl.release(); err == nil {
			err = rerr
		}
	})
	return err
}

// Unlink removes the event's name so no further process can open it. Handles
// already open stay usable until they are closed.
func (e *Event) Unlink() error {
	return unlinkEventImpl(e.name)
}
