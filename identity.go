package ipc

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"path/filepath"
	"sync"
)

// A procID names a single process for its whole life. It is random, so no later process reuses it.
type procID uint64

const (
	noProc procID = 0
	// pendingProc marks a channel direction whose reader has not connected.
	pendingProc procID = 1
	// watchable marks a procID with a life socket behind it.
	watchable procID = 1 << 63
	// anyProc keeps a random procID well away from the values above.
	anyProc procID = 1 << 62
)

func (id procID) watchable() bool { return id&watchable != 0 }

// errProcGone reports a process that has exited.
var errProcGone = errors.New("ipc: process has exited")

// errNotWatchable reports a process that has no life socket.
var errNotWatchable = errors.New("ipc: process cannot be watched")

func lifePath(id procID) string {
	return filepath.Join(lifeDir(), fmt.Sprintf("go-ipc-life-%016x.sock", uint64(id)))
}

var self struct {
	once sync.Once
	id   procID
	err  error
}

// selfID returns the procID of this process, and starts its life socket.
//
// A host that has no Unix sockets still gets a procID, without the watchable
// bit. The queues of that process work. Its peers do not detect its death,
// and it reports why through selfErr.
func selfID() procID {
	self.once.Do(func() {
		id := randomID()
		ln, err := listenLife(id)
		if err != nil {
			self.id, self.err = id&^watchable, err
			return
		}
		self.id = id
		go serveLife(ln)
	})
	return self.id
}

// selfErr is why this process has no life socket, or nil.
func selfErr() error {
	selfID()
	return self.err
}

func randomID() procID {
	var raw [8]byte
	rand.Read(raw[:])
	return procID(binary.LittleEndian.Uint64(raw[:])) | watchable | anyProc
}

// serveLife holds every connection to the life socket open until its peer
// closes it, or this process exits.
func serveLife(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			// Connections that wait in the backlog still end with this process, so a watch works without an accept.
			return
		}
		go func() {
			io.Copy(io.Discard, conn)
			conn.Close()
		}()
	}
}

// isDead reports whether the process id names has exited. A process that
// cannot be checked counts as alive. The only use of a false answer is to
// leave its claims alone.
func isDead(id procID) bool {
	if !id.watchable() {
		return false
	}
	conn, err := net.Dial("unix", lifePath(id))
	if err == nil {
		conn.Close()
		return false
	}
	return lifeGone(err)
}

// lifeGone reports whether a dial error means that no process listens.
func lifeGone(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || isRefused(err)
}

// openExit connects to the life socket of id. The connection ends when that
// process exits.
func openExit(id procID) (exitWaiter, error) {
	if !id.watchable() {
		return nil, errNotWatchable
	}
	conn, err := net.Dial("unix", lifePath(id))
	if err != nil {
		if lifeGone(err) {
			return nil, errProcGone
		}
		return nil, err
	}
	return connWaiter{conn}, nil
}

// An exitWaiter becomes ready when a process exits.
type exitWaiter interface {
	// wait blocks until the process exits or close is called.
	wait() error
	close() error
}

type connWaiter struct{ conn net.Conn }

func (w connWaiter) wait() error {
	_, err := io.Copy(io.Discard, w.conn)
	w.conn.Close()
	return err
}

func (w connWaiter) close() error { return w.conn.Close() }

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
