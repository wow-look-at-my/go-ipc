//go:build unix

package ipc

import (
	"errors"
	"io/fs"
	"os"
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
// A FIFO has a name, so a peer opens it without any handshake, and the Go
// runtime polls it, so a read parks the goroutine instead of a thread. The
// handle is opened read-write: that never blocks on open, and it keeps a
// writer attached so a reader never sees end-of-file.
type eventImpl struct {
	f *os.File
}

func createEventImpl(name string) (eventImpl, error) {
	path := eventPath(name)
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return eventImpl{}, err
	}
	if err := unix.Mkfifo(path, 0o600); err != nil && !errors.Is(err, fs.ErrExist) {
		return eventImpl{}, &os.PathError{Op: "mkfifo", Path: path, Err: err}
	}
	return openFIFO(path)
}

func openEventImpl(name string) (eventImpl, error) {
	return openFIFO(eventPath(name))
}

func openFIFO(path string) (eventImpl, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return eventImpl{}, err
	}
	// A pollable handle is the whole point, so prove it rather than assume
	// it. Deadline support is the observable consequence of registration
	// with the runtime poller.
	if err := f.SetReadDeadline(time.Time{}); err != nil {
		f.Close()
		return eventImpl{}, errors.Join(ErrNotPollable, err)
	}
	return eventImpl{f: f}, nil
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
func (e eventImpl) signal(n int) error {
	if n > pipeBuf {
		n = pipeBuf
	}
	rc, err := e.f.SyscallConn()
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

// wait blocks the calling goroutine until one token arrives.
func (e eventImpl) wait() error {
	var buf [1]byte
	for {
		n, err := e.f.Read(buf[:])
		if n == 1 {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// stop closes the handle, which is also what releases a goroutine blocked in
// wait: the Go poller fails an in-flight read on a closed file.
func (e eventImpl) stop() error {
	return e.f.Close()
}

// release has nothing left to free; stop already closed the only handle.
func (e eventImpl) release() error { return nil }
