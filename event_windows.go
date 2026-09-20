package ipc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	semaphoreAllAccess = 0x1F0003
	waitObject0        = 0
	waitFailed         = 0xFFFFFFFF
	infinite           = 0xFFFFFFFF
	maxSemaphoreCount  = 0x7FFFFFFF
)

var (
	kernel32            = windows.NewLazySystemDLL("kernel32.dll")
	procCreateSemaphore = kernel32.NewProc("CreateSemaphoreW")
	procOpenSemaphore   = kernel32.NewProc("OpenSemaphoreW")
	procReleaseSema     = kernel32.NewProc("ReleaseSemaphore")
)

// eventImpl backs an Event with a named semaphore.
//
// The semaphore counts pending wakeups the way the Unix FIFO counts unread
// tokens, so both platforms deliver the same semantics. A separate unnamed
// event lets Close release a waiter blocked inside the kernel.
//
// A kernel wait here does occupy a thread, which the Unix poller avoids. The
// Go runtime hands the processor to another thread for the duration, so other
// goroutines keep running.
type eventImpl struct {
	sem     windows.Handle
	closing windows.Handle

	// waiters keeps the handles alive until the last wait leaves. Closing a
	// handle that a thread is blocked on is not safe, so close signals first
	// and frees only after the count reaches zero.
	mu      sync.Mutex
	waiters atomic.Int64
	drained chan struct{}
	closed  bool
}

// objectName puts the semaphore in the caller's logon session. A cross-session
// name would need a privilege that an ordinary program does not hold.
func objectName(name string) string { return `Local\go-ipc-` + name + ".event" }

func createEventImpl(name string) (*eventImpl, error) {
	wide, err := syscall.UTF16PtrFromString(objectName(name))
	if err != nil {
		return nil, err
	}
	h, _, callErr := procCreateSemaphore.Call(0, 0, maxSemaphoreCount, uintptr(unsafe.Pointer(wide)))
	if h == 0 {
		return nil, callErr
	}
	return finishEvent(windows.Handle(h))
}

func openEventImpl(name string) (*eventImpl, error) {
	wide, err := syscall.UTF16PtrFromString(objectName(name))
	if err != nil {
		return nil, err
	}
	h, _, callErr := procOpenSemaphore.Call(semaphoreAllAccess, 0, uintptr(unsafe.Pointer(wide)))
	if h == 0 {
		return nil, callErr
	}
	return finishEvent(windows.Handle(h))
}

func finishEvent(sem windows.Handle) (*eventImpl, error) {
	closing, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		windows.CloseHandle(sem)
		return nil, err
	}
	return &eventImpl{sem: sem, closing: closing, drained: make(chan struct{}, 1)}, nil
}

// unlinkEventImpl has nothing to remove. Windows drops a named object once
// the last handle to it closes.
func unlinkEventImpl(string) error { return nil }

func (e *eventImpl) signal(n int) error {
	ok, _, callErr := procReleaseSema.Call(uintptr(e.sem), uintptr(n), 0)
	if ok == 0 {
		// A saturated semaphore already holds more wakeups than there
		// are waiters, so the lost release costs nothing.
		if errors.Is(callErr, windows.ERROR_TOO_MANY_POSTS) {
			return nil
		}
		return callErr
	}
	return nil
}

// wait blocks until the semaphore is released, ctx ends, or the event closes.
//
// Cancellation gets an unnamed event of its own per call, so it wakes this
// waiter and leaves every other waiter on the object alone.
func (e *eventImpl) wait(ctx context.Context) error {
	if !e.enter() {
		return ErrClosed
	}
	defer e.leave()

	handles := []windows.Handle{e.sem, e.closing}
	if ctx.Done() != nil {
		cancel, err := windows.CreateEvent(nil, 1, 0, nil)
		if err != nil {
			return err
		}
		defer windows.CloseHandle(cancel)
		stop := context.AfterFunc(ctx, func() { windows.SetEvent(cancel) })
		defer stop()
		handles = append(handles, cancel)
	}

	res, err := windows.WaitForMultipleObjects(handles, false, infinite)
	switch {
	case res == waitObject0:
		return nil
	case res == waitFailed:
		return err
	}
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	return ErrClosed
}

// enter keeps the handles alive for the duration of a wait.
func (e *eventImpl) enter() bool {
	e.waiters.Add(1)
	e.mu.Lock()
	closed := e.closed
	e.mu.Unlock()
	if closed {
		e.leave()
		return false
	}
	return true
}

func (e *eventImpl) leave() {
	if e.waiters.Add(-1) != 0 {
		return
	}
	e.mu.Lock()
	closed := e.closed
	e.mu.Unlock()
	if closed {
		select {
		case e.drained <- struct{}{}:
		default:
		}
	}
}

// close releases every waiter and then frees the handles. Closing a handle
// that a thread still waits on is not safe, so the free comes last.
func (e *eventImpl) close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	e.closed = true
	e.mu.Unlock()

	if err := windows.SetEvent(e.closing); err != nil {
		return err
	}
	if e.waiters.Load() > 0 {
		<-e.drained
	}

	err := windows.CloseHandle(e.sem)
	if cerr := windows.CloseHandle(e.closing); err == nil {
		err = cerr
	}
	return err
}
