//go:build unix

package ipc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"sync"
)

// A sockEvent is an event for a host with Unix sockets but no FIFOs: a cosmo
// binary on Windows.
type sockEvent struct {
	path string
	srv  *sockServer
	// conn is nil when the creator was gone at open.
	conn net.Conn
	// wmu keeps each request whole on conn.
	wmu sync.Mutex

	mu sync.Mutex
	// waiters get tokens in the order they asked. Each sent a W.
	waiters []chan error
	// lostErr is set once conn breaks. Every later wait reports it.
	lostErr error
	closed  bool
}

type sockServer struct {
	ln      net.Listener
	mu      sync.Mutex
	tokens  int
	waiting []net.Conn
	clients map[net.Conn]struct{}
	closed  bool
}

func sockPath(name string) string {
	sum := sha256.Sum256([]byte(eventPath(name)))
	return filepath.Join(filepath.Dir(eventPath(name)), "go-ipc-"+hex.EncodeToString(sum[:8])+".esock")
}

func createSockEvent(name string) (*sockEvent, error) {
	path := sockPath(name)
	os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	srv := &sockServer{ln: ln, clients: make(map[net.Conn]struct{})}
	go srv.serve()
	e := &sockEvent{path: path, srv: srv}
	conn, err := net.Dial("unix", path)
	if err != nil {
		srv.shutdown()
		os.Remove(path)
		return nil, err
	}
	e.start(conn)
	return e, nil
}

// openSockEvent fails only for a missing path. A socket whose creator died
// opens, as a FIFO the creator left does. Windows refuses a dial to a missing
// path too, so a dial cannot tell both apart.
func openSockEvent(name string) (*sockEvent, error) {
	path := sockPath(name)
	if _, err := os.Lstat(path); errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	e := &sockEvent{path: path}
	conn, err := net.Dial("unix", path)
	switch {
	case err == nil:
		e.start(conn)
	case isRefused(err) || errors.Is(err, fs.ErrNotExist):
		e.lostErr = ErrPeerGone
	default:
		return nil, err
	}
	return e, nil
}

func (e *sockEvent) start(conn net.Conn) {
	e.conn = conn
	go e.read()
}

func (s *sockServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			conn.Close()
			continue
		}
		s.clients[conn] = struct{}{}
		s.mu.Unlock()
		go s.client(conn)
	}
}

// shutdown hangs up every client, so a waiter in another process learns that the creator closed.
func (s *sockServer) shutdown() {
	s.ln.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for conn := range s.clients {
		conn.Close()
	}
	s.clients = nil
	s.waiting = nil
}

func (s *sockServer) client(conn net.Conn) {
	defer conn.Close()
	var msg [2]byte
	for {
		if _, err := io.ReadFull(conn, msg[:1]); err != nil {
			s.drop(conn)
			return
		}
		s.mu.Lock()
		switch msg[0] {
		case 'W':
			s.waiting = append(s.waiting, conn)
		case 'C':
			if s.remove(conn) {
				conn.Write([]byte{'X'})
			}
		case 'S':
			s.mu.Unlock()
			if _, err := io.ReadFull(conn, msg[1:]); err != nil {
				return
			}
			s.mu.Lock()
			s.tokens += int(msg[1])
		}
		for s.tokens > 0 && len(s.waiting) > 0 {
			next := s.waiting[0]
			s.waiting = s.waiting[1:]
			if _, err := next.Write([]byte{'T'}); err == nil {
				s.tokens--
			}
		}
		s.mu.Unlock()
	}
}

func (s *sockServer) remove(conn net.Conn) bool {
	for idx, waiter := range s.waiting {
		if waiter == conn {
			s.waiting = append(s.waiting[:idx], s.waiting[idx+1:]...)
			return true
		}
	}
	return false
}

func (s *sockServer) drop(conn net.Conn) {
	s.mu.Lock()
	s.remove(conn)
	delete(s.clients, conn)
	s.mu.Unlock()
}

func (e *sockEvent) send(msg []byte) error {
	e.wmu.Lock()
	defer e.wmu.Unlock()
	_, err := e.conn.Write(msg)
	return err
}

// read hands each token to the oldest waiter. A token with no waiter left
// goes back to the creator, so no other waiter loses it. The creator sends
// such a token when a waiter cancelled after its token was on the way.
func (e *sockEvent) read() {
	var b [1]byte
	for {
		if _, err := e.conn.Read(b[:]); err != nil {
			e.lose()
			return
		}
		if b[0] != 'T' {
			continue
		}
		e.mu.Lock()
		if len(e.waiters) > 0 {
			ch := e.waiters[0]
			e.waiters = e.waiters[1:]
			e.mu.Unlock()
			ch <- nil
			continue
		}
		e.mu.Unlock()
		e.send([]byte{'S', 1})
	}
}

// lose ends every wait once conn breaks. A local close broke it, or the
// creator went away.
func (e *sockEvent) lose() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lostErr = ErrPeerGone
	if e.closed {
		e.lostErr = ErrClosed
	}
	for _, ch := range e.waiters {
		ch <- e.lostErr
	}
	e.waiters = nil
}

// signal to a dead creator is dropped. Nobody waits on it, as with a FIFO nobody reads.
func (e *sockEvent) signal(n int) error {
	e.mu.Lock()
	closed, gone := e.closed, e.lostErr != nil
	e.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if gone {
		return nil
	}
	for n > 0 {
		step := min(n, 255)
		if err := e.send([]byte{'S', byte(step)}); err != nil {
			return e.dropped()
		}
		n -= step
	}
	return nil
}

// dropped is the result of a signal whose connection broke.
func (e *sockEvent) dropped() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	return nil
}

// wait reports ErrPeerGone when the creator dies, because nobody can signal the event after that.
func (e *sockEvent) wait(ctx context.Context) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	if e.lostErr != nil {
		e.mu.Unlock()
		return e.lostErr
	}
	ch := make(chan error, 1)
	e.waiters = append(e.waiters, ch)
	e.mu.Unlock()

	// A failed write means conn broke. Read then fails too, and lose ends this wait.
	if err := e.send([]byte{'W'}); err != nil {
		return <-ch
	}
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
	}
	if !e.withdraw(ch) {
		return <-ch
	}
	// The creator drops a queued W of this connection. When none is queued, the token is on its way, and read sends it back.
	e.send([]byte{'C'})
	return ctx.Err()
}

// withdraw removes ch from the waiters. It fails when ch already has a result.
func (e *sockEvent) withdraw(ch chan error) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, w := range e.waiters {
		if w == ch {
			e.waiters = append(e.waiters[:i], e.waiters[i+1:]...)
			return true
		}
	}
	return false
}

func (e *sockEvent) close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	e.closed = true
	e.mu.Unlock()
	if e.conn != nil {
		e.conn.Close()
	}
	if e.srv != nil {
		e.srv.shutdown()
		os.Remove(e.path)
	}
	return nil
}
