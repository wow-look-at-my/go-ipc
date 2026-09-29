package ipc

import (
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sys/windows"
)

const processAccess = windows.PROCESS_QUERY_LIMITED_INFORMATION | windows.SYNCHRONIZE

// openProcess opens pid and checks that it has not exited. A handle keeps
// the process object, and its creation time, after the process exits.
func openProcess(pid int) (windows.Handle, uint64, error) {
	h, err := windows.OpenProcess(processAccess, false, uint32(pid))
	if errors.Is(err, windows.ERROR_INVALID_PARAMETER) {
		return 0, 0, errProcGone
	}
	if err != nil {
		return 0, 0, err
	}
	var created, exited, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(h, &created, &exited, &kernel, &user); err != nil {
		windows.CloseHandle(h)
		return 0, 0, err
	}
	if ev, _ := windows.WaitForSingleObject(h, 0); ev == windows.WAIT_OBJECT_0 {
		windows.CloseHandle(h)
		return 0, 0, errProcGone
	}
	return h, uint64(created.HighDateTime)<<32 | uint64(created.LowDateTime), nil
}

func startTime(pid int) (uint64, error) {
	h, start, err := openProcess(pid)
	if err != nil {
		return 0, err
	}
	windows.CloseHandle(h)
	return start, nil
}

func procNS() (uint64, error) { return 1, nil }

// handleWaiter waits on a process handle. The wait holds a thread, as every
// Windows wait in this package does.
type handleWaiter struct {
	process windows.Handle
	cancel  windows.Handle

	// done guards cancel.
	mu   sync.Mutex
	done bool
}

func openExit(id procID) (exitWaiter, error) {
	h, start, err := openProcess(id.pid())
	if err != nil {
		return nil, err
	}
	if !id.matches(start) {
		windows.CloseHandle(h)
		return nil, errProcGone
	}
	cancel, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		windows.CloseHandle(h)
		return nil, err
	}
	return &handleWaiter{process: h, cancel: cancel}, nil
}

// wait blocks until the process exits or close is called. It frees both
// handles, because only this goroutine knows when no wait uses them.
func (w *handleWaiter) wait() error {
	res, err := windows.WaitForMultipleObjects([]windows.Handle{w.process, w.cancel}, false, windows.INFINITE)
	w.mu.Lock()
	w.done = true
	windows.CloseHandle(w.process)
	windows.CloseHandle(w.cancel)
	w.mu.Unlock()
	switch res {
	case windows.WAIT_OBJECT_0:
		return nil
	case windows.WAIT_OBJECT_0 + 1:
		return ErrClosed
	}
	return fmt.Errorf("ipc: wait for process exit: %w", err)
}

func (w *handleWaiter) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return nil
	}
	return windows.SetEvent(w.cancel)
}
