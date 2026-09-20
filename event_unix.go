//go:build unix

package ipc

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// pipeBuf is the largest write a pipe guarantees to deliver atomically. A
// SignalN above it is truncated rather than torn.
const pipeBuf = 4096

var signalTokens = make([]byte, pipeBuf)

// eventImpl backs an Event with a FIFO.
//
// A FIFO has a name, so a peer opens it with no handshake. The Go runtime
// polls a FIFO, so a read parks the goroutine and hands back its thread. Every
// handle is opened read-write: that never blocks on open, and it keeps a
// writer attached so a reader never sees end-of-file.
//
// A waiter reads through a handle of its own, taken from idle. That is what
// makes a read deadline usable for cancellation: a deadline set on a shared
// handle would abort every reader of it, not just the thing that cancelled.
type eventImpl struct {
	path  string
	write *os.File

	// gone lets signal reject a closed event without the mutex, and without
	// a guess at which internal error a closed handle reports.
	gone atomic.Bool

	mu     sync.Mutex
	idle   []*reader
	open   []*reader
	closed bool
}

// A reader is a handle a single waiter reads through, plus the cancel
// function for it. The function is built with the handle so that a wait
// hands context.AfterFunc an existing value rather than a fresh closure.
type reader struct {
	f      *os.File
	cancel func()
}

func newReader(f *os.File) *reader {
	r := &reader{f: f}
	r.cancel = func() { r.f.SetReadDeadline(deadlinePast) }
	return r
}

func createEventImpl(name string) (*eventImpl, error) {
	path := eventPath(name)
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	if err := unix.Mkfifo(path, 0o600); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, &os.PathError{Op: "mkfifo", Path: path, Err: err}
	}
	return newEventImpl(path)
}

func openEventImpl(name string) (*eventImpl, error) {
	return newEventImpl(eventPath(name))
}

func newEventImpl(path string) (*eventImpl, error) {
	w, err := openFIFO(path)
	if err != nil {
		return nil, err
	}
	return &eventImpl{path: path, write: w}, nil
}

func openFIFO(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	// A pollable handle is the whole point, so prove it rather than assume
	// it. Deadline support is the observable consequence of registration
	// with the runtime poller.
	if err := f.SetReadDeadline(time.Time{}); err != nil {
		f.Close()
		return nil, errors.Join(ErrNotPollable, err)
	}
	return f, nil
}

// acquire hands out a reader, and opens a handle when none is idle. The pool
// therefore grows to the peak number of waiters this process ever had.
func (e *eventImpl) acquire() (*reader, error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, ErrClosed
	}
	if n := len(e.idle); n > 0 {
		r := e.idle[n-1]
		e.idle = e.idle[:n-1]
		e.mu.Unlock()
		return r, nil
	}
	e.mu.Unlock()

	f, err := openFIFO(e.path)
	if err != nil {
		return nil, err
	}
	r := newReader(f)

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		f.Close()
		return nil, ErrClosed
	}
	e.open = append(e.open, r)
	e.mu.Unlock()
	return r, nil
}

// releaseReader returns a handle for the next waiter to use.
func (e *eventImpl) releaseReader(r *reader) {
	// A cancellation that fired late can leave a deadline in the past. The
	// reset here keeps the next waiter from an immediate spurious return,
	// and its own retry covers the case where both race.
	r.f.SetReadDeadline(time.Time{})

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	e.idle = append(e.idle, r)
}

func unlinkEventImpl(name string) error {
	err := os.Remove(eventPath(name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// signal writes n tokens without ever blocking the caller.
//
// A full pipe already holds more pending wakeups than there are waiters, so
// dropping the write loses nothing.
func (e *eventImpl) signal(n int) error {
	if n > pipeBuf {
		n = pipeBuf
	}
	if e.gone.Load() {
		return ErrClosed
	}
	rc, err := e.write.SyscallConn()
	if err != nil {
		return err
	}
	var werr error
	cerr := rc.Write(func(fd uintptr) bool {
		_, werr = unix.Write(int(fd), signalTokens[:n])
		if errors.Is(werr, syscall.EAGAIN) || errors.Is(werr, syscall.EWOULDBLOCK) {
			werr = nil
		}
		// Reporting completion unconditionally keeps this call off the
		// poller: a signaller must never wait for a waiter.
		return true
	})
	if cerr != nil {
		return cerr
	}
	return werr
}

// deadlinePast is any instant already gone. Setting it aborts a blocked read.
var deadlinePast = time.Unix(1, 0)

// wait parks the calling goroutine until a token arrives, ctx ends, or the
// event closes.
//
// The read runs on this waiter's own handle, so cancellation through a read
// deadline touches nothing else. A token is consumed only by a read that
// returns it, so a cancelled wait never swallows a wakeup.
func (e *eventImpl) wait(ctx context.Context) error {
	r, err := e.acquire()
	if err != nil {
		return err
	}
	defer e.releaseReader(r)

	if ctx.Done() != nil {
		stop := context.AfterFunc(ctx, r.cancel)
		defer stop()
	}

	var buf [1]byte
	for {
		n, rerr := r.f.Read(buf[:])
		if n == 1 {
			return nil
		}
		switch {
		case rerr == nil:
			continue
		case errors.Is(rerr, os.ErrDeadlineExceeded):
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			// A deadline with no live cancellation behind it came from an
			// earlier waiter whose cancellation landed after it let go.
			r.f.SetReadDeadline(time.Time{})
		case errors.Is(rerr, os.ErrClosed), e.gone.Load():
			// Close aborts an in-flight read by closing the handle under
			// it. The flag covers the same case without a guess at which
			// internal error the runtime reports for it.
			return ErrClosed
		default:
			return rerr
		}
	}
}

// close releases every handle. A read in flight fails, because the Go poller
// aborts a single on a closed file, which is what releases a parked waiter.
func (e *eventImpl) close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	e.closed = true
	e.gone.Store(true)
	readers := e.open
	e.open = nil
	e.idle = nil
	e.mu.Unlock()

	var err error
	for _, r := range readers {
		if cerr := r.f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	if cerr := e.write.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}
