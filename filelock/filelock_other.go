//go:build !unix && !windows

package filelock

import (
	"os"
	"sync"
	"syscall"
)

// This platform has no OS file lock, and a program here starts no other
// process. So the lock is held inside this process, one slot per path.
var (
	slotsMu sync.Mutex
	slots   = map[string]chan struct{}{}
)

func slot(f *os.File) chan struct{} {
	slotsMu.Lock()
	defer slotsMu.Unlock()
	ch, ok := slots[f.Name()]
	if !ok {
		ch = make(chan struct{}, 1)
		slots[f.Name()] = ch
	}
	return ch
}

// Take takes the lock on f's path. With wait it blocks until the holder lets
// go. Without it a held lock fails at once with EAGAIN.
func Take(f *os.File, wait bool) error {
	ch := slot(f)
	if wait {
		ch <- struct{}{}
		return nil
	}
	select {
	case ch <- struct{}{}:
		return nil
	default:
		return syscall.EAGAIN
	}
}

// Release lets go of the lock Take took on f.
func Release(f *os.File) {
	select {
	case <-slot(f):
	default:
	}
}
