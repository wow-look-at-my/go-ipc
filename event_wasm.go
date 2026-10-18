//go:build js || wasip1

package ipc

import (
	"context"
	"fmt"
	"io/fs"
	"math"
	"sync"
	"sync/atomic"
)

// WebAssembly runs one process and has no FIFO and no named semaphore. An
// event is a counting semaphore in a registry of this process, as a named
// semaphore is on Windows. Every open of a name shares its tokens.

// semaphore holds a token for each signal no waiter took yet. A full channel
// holds more tokens than there can be waiters, so a lost send costs nothing.
type semaphore struct {
	tokens chan struct{}
}

var events struct {
	mu     sync.Mutex
	byName map[string]*semaphore
}

type eventImpl struct {
	sem     *semaphore
	closing chan struct{}
	closed  atomic.Bool
}

// createEventImpl replaces any instance of the name. Handles to the old one
// keep it.
func createEventImpl(name string) (*eventImpl, error) {
	sem := &semaphore{tokens: make(chan struct{}, math.MaxInt32)}
	events.mu.Lock()
	if events.byName == nil {
		events.byName = make(map[string]*semaphore)
	}
	events.byName[name] = sem
	events.mu.Unlock()
	return &eventImpl{sem: sem, closing: make(chan struct{})}, nil
}

func openEventImpl(name string) (*eventImpl, error) {
	events.mu.Lock()
	sem := events.byName[name]
	events.mu.Unlock()
	if sem == nil {
		return nil, fmt.Errorf("ipc: no event named %q: %w", name, fs.ErrNotExist)
	}
	return &eventImpl{sem: sem, closing: make(chan struct{})}, nil
}

func unlinkEventImpl(name string) error {
	events.mu.Lock()
	delete(events.byName, name)
	events.mu.Unlock()
	return nil
}

func (e *eventImpl) signal(n int) error {
	if e.closed.Load() {
		return ErrClosed
	}
	for range n {
		select {
		case e.sem.tokens <- struct{}{}:
		default:
			return nil
		}
	}
	return nil
}

// wait blocks until it takes a token, ctx ends, or this handle closes.
func (e *eventImpl) wait(ctx context.Context) error {
	if e.closed.Load() {
		return ErrClosed
	}
	select {
	case <-e.sem.tokens:
		return nil
	case <-e.closing:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *eventImpl) close() error {
	if e.closed.Swap(true) {
		return ErrClosed
	}
	close(e.closing)
	return nil
}
