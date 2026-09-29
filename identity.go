package ipc

import (
	"errors"
	"os"
	"sync"
)

// A procID names a single process for its whole life.
type procID uint64

const (
	noProc procID = 0
	// pendingProc marks a channel direction whose reader has not connected.
	pendingProc procID = 1
)

// errProcGone reports a process that has exited.
var errProcGone = errors.New("ipc: process has exited")

func makeProcID(pid int, start uint64) procID {
	return procID(uint64(uint32(pid))<<32 | uint64(uint32(start)))
}

func (id procID) pid() int { return int(uint32(id >> 32)) }

// matches reports whether a start time belongs to the process id names.
func (id procID) matches(start uint64) bool { return uint32(start) == uint32(id) }

var self struct {
	once sync.Once
	id   procID
	ns   uint64
	err  error
}

// selfID returns the procID of this process, and its pid namespace.
//
// A pid names a process only inside its own pid namespace. Queues can join processes in different namespaces that share /dev/shm. A process judges the liveness of another only when the other is in its own namespace.
func selfID() (procID, uint64, error) {
	self.once.Do(func() {
		pid := os.Getpid()
		start, err := startTime(pid)
		if err != nil {
			self.err = err
			return
		}
		if self.ns, err = procNS(); err != nil {
			self.err = err
			return
		}
		self.id = makeProcID(pid, start)
	})
	return self.id, self.ns, self.err
}

// isDead reports whether the process id names has exited.
func isDead(id procID) bool {
	start, err := startTime(id.pid())
	if errors.Is(err, errProcGone) {
		return true
	}
	return err == nil && !id.matches(start)
}

// An exitWaiter is a kernel handle that becomes ready when a process exits.
type exitWaiter interface {
	// wait blocks until the process exits or close is called.
	wait() error
	close() error
}

// A watch runs callbacks when its process exits. There is one per watched
// process, however many callers watch it.
type watch struct {
	waiter  exitWaiter
	fns     map[uint64]func(error)
	stopped bool
}

var watches struct {
	mu   sync.Mutex
	byID map[procID]*watch
	next uint64
}

// onExit calls fn once, on a goroutine of its own, after the process id
// names has exited. A process that is already gone gets its call at once.
// The returned function cancels a call that has not started.
//
// fn gets nil for an exit. It gets the error when the kernel wait fails while
// the process lives, since no exit can reach it after that.
func onExit(id procID, fn func(error)) (func(), error) {
	watches.mu.Lock()
	defer watches.mu.Unlock()

	w := watches.byID[id]
	if w == nil {
		waiter, err := openExit(id)
		if errors.Is(err, errProcGone) {
			go fn(nil)
			return func() {}, nil
		}
		if err != nil {
			return nil, err
		}
		w = &watch{waiter: waiter, fns: make(map[uint64]func(error))}
		if watches.byID == nil {
			watches.byID = make(map[procID]*watch)
		}
		watches.byID[id] = w
		go w.run(id)
	}
	watches.next++
	key := watches.next
	w.fns[key] = fn

	return func() {
		watches.mu.Lock()
		defer watches.mu.Unlock()
		delete(w.fns, key)
		if len(w.fns) == 0 && !w.stopped {
			w.stopped = true
			delete(watches.byID, id)
			w.waiter.close()
		}
	}, nil
}

func (w *watch) run(id procID) {
	err := w.waiter.wait()

	watches.mu.Lock()
	if w.stopped {
		watches.mu.Unlock()
		return
	}
	w.stopped = true
	delete(watches.byID, id)
	fns := make([]func(error), 0, len(w.fns))
	for _, fn := range w.fns {
		fns = append(fns, fn)
	}
	watches.mu.Unlock()

	if err != nil && isDead(id) {
		err = nil
	}
	for _, fn := range fns {
		fn(err)
	}
}
