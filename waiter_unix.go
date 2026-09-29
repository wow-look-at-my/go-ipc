//go:build linux || darwin

package ipc

import (
	"errors"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// A pollWaiter parks on a descriptor the Go poller watches, so a wait for an
// exit holds no thread.
type pollWaiter struct {
	f     *os.File
	ready func(fd int) (bool, error)
}

func newPollWaiter(fd int, name string, ready func(int) (bool, error)) (*pollWaiter, error) {
	if err := unix.SetNonblock(fd, true); err != nil {
		unix.Close(fd)
		return nil, err
	}
	f := os.NewFile(uintptr(fd), name)
	// Deadline support is the observable sign that the poller took the
	// descriptor. Without it a wait would hold a thread.
	if err := f.SetReadDeadline(time.Time{}); err != nil {
		f.Close()
		return nil, errors.Join(ErrNotPollable, err)
	}
	return &pollWaiter{f: f, ready: ready}, nil
}

// wait closes the descriptor when it returns. A close from another goroutine
// during the wait aborts it.
func (w *pollWaiter) wait() error {
	defer w.f.Close()
	rc, err := w.f.SyscallConn()
	if err != nil {
		return err
	}
	var readyErr error
	err = rc.Read(func(fd uintptr) bool {
		ok, err := w.ready(int(fd))
		if err != nil {
			readyErr = err
			return true
		}
		return ok
	})
	if err != nil {
		return err
	}
	return readyErr
}

func (w *pollWaiter) close() error { return w.f.Close() }
