package ipc

import (
	"errors"
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
// tokens, so the two platforms deliver the same semantics. A second, unnamed
// event lets Close release the waiter blocked inside the kernel.
type eventImpl struct {
	sem     windows.Handle
	closing windows.Handle
}

// objectName puts the semaphore in the caller's logon session. A cross-session
// name would need a privilege that an ordinary program does not hold.
func objectName(name string) string { return `Local\go-ipc-` + name + ".event" }

func createEventImpl(name string) (eventImpl, error) {
	wide, err := syscall.UTF16PtrFromString(objectName(name))
	if err != nil {
		return eventImpl{}, err
	}
	h, _, callErr := procCreateSemaphore.Call(0, 0, maxSemaphoreCount, uintptr(unsafe.Pointer(wide)))
	if h == 0 {
		return eventImpl{}, callErr
	}
	return finishEvent(windows.Handle(h))
}

func openEventImpl(name string) (eventImpl, error) {
	wide, err := syscall.UTF16PtrFromString(objectName(name))
	if err != nil {
		return eventImpl{}, err
	}
	h, _, callErr := procOpenSemaphore.Call(semaphoreAllAccess, 0, uintptr(unsafe.Pointer(wide)))
	if h == 0 {
		return eventImpl{}, callErr
	}
	return finishEvent(windows.Handle(h))
}

func finishEvent(sem windows.Handle) (eventImpl, error) {
	closing, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		windows.CloseHandle(sem)
		return eventImpl{}, err
	}
	return eventImpl{sem: sem, closing: closing}, nil
}

// unlinkEventImpl has nothing to remove. Windows drops a named object once
// the last handle to it closes.
func unlinkEventImpl(string) error { return nil }

func (e eventImpl) signal(n int) error {
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

func (e eventImpl) wait() error {
	res, err := windows.WaitForMultipleObjects([]windows.Handle{e.sem, e.closing}, false, infinite)
	switch {
	case res == waitObject0:
		return nil
	case res == waitFailed:
		return err
	default:
		return ErrClosed
	}
}

// stop releases a waiter blocked in the kernel. The handles stay valid until
// release, because closing a handle another thread is waiting on is not safe.
func (e eventImpl) stop() error {
	return windows.SetEvent(e.closing)
}

// release frees both handles. The caller guarantees no wait is in flight.
func (e eventImpl) release() error {
	err := windows.CloseHandle(e.sem)
	if cerr := windows.CloseHandle(e.closing); err == nil {
		err = cerr
	}
	return err
}
