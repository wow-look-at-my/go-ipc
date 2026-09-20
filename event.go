package ipc

import (
	"context"
	"sync"
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

	pumpOnce  sync.Once
	shutOnce  sync.Once
	closeOnce sync.Once
}

// shut makes closing observable exactly a single time, from Close or from the pump.
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

	select {
	case <-e.tokens:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-e.closing:
		return ErrClosed
	}
}

// pump moves wakeups off the operating system handle and onto e.tokens. a
// single pump serves every waiter in this process, so a kernel wait that does
// block a thread blocks at most a single per event.
func (e *Event) pump() {
	defer close(e.pumpDone)
	for {
		if err := e.impl.wait(); err != nil {
			e.shut()
			return
		}
		select {
		case e.tokens <- struct{}{}:
		case <-e.closing:
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
		// wait below cannot outlast a Close that raced a earliest Wait.
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
