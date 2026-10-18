//go:build js || wasip1

package filelock

import (
	"os"
	"sync"
	"syscall"
)

// WebAssembly runs one process and has no flock. The lock is a token per file
// in this process, so two opens of one file exclude each other as flock makes
// them. The process holds every lock, so none outlives it.

// fileID names a file the way the kernel does: its device and inode.
type fileID struct {
	dev uint64
	ino uint64
}

type lockState struct {
	token  chan struct{}
	holder *os.File
}

var (
	locksMu sync.Mutex
	locks   = make(map[fileID]*lockState)
)

func stateOf(f *os.File) (*lockState, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, syscall.ENOTSUP
	}
	id := fileID{dev: uint64(stat.Dev), ino: stat.Ino}
	locksMu.Lock()
	defer locksMu.Unlock()
	state := locks[id]
	if state == nil {
		state = &lockState{token: make(chan struct{}, 1)}
		locks[id] = state
	}
	return state, nil
}

// Take takes f's exclusive lock. With wait it blocks until the lock is
// granted. Without it a held lock fails at once with EAGAIN, which is
// EWOULDBLOCK.
func Take(f *os.File, wait bool) error {
	state, err := stateOf(f)
	if err != nil {
		return err
	}
	if wait {
		state.token <- struct{}{}
	} else {
		select {
		case state.token <- struct{}{}:
		default:
			return syscall.EAGAIN
		}
	}
	locksMu.Lock()
	state.holder = f
	locksMu.Unlock()
	return nil
}

// Release lets go of the lock Take took on f.
func Release(f *os.File) {
	state, err := stateOf(f)
	if err != nil {
		return
	}
	locksMu.Lock()
	held := state.holder == f
	if held {
		state.holder = nil
	}
	locksMu.Unlock()
	if held {
		<-state.token
	}
}
