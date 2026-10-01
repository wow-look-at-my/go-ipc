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

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
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
	return &sockEvent{path: path, srv: srv, conns: make(map[net.Conn]struct{})}, nil
}

// openSockEvent fails only for a missing path. A socket whose creator died
// opens, as a FIFO the creator left does. Windows refuses a dial to a missing
// path too, so a dial cannot tell both apart.
func openSockEvent(name string) (*sockEvent, error) {
	path := sockPath(name)
	if _, err := os.Lstat(path); errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	return &sockEvent{path: path, conns: make(map[net.Conn]struct{})}, nil
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

func (e *sockEvent) dial() (net.Conn, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, ErrClosed
	}
	conn, err := net.Dial("unix", e.path)
	if err != nil {
		return nil, err
	}
	e.conns[conn] = struct{}{}
	return conn, nil
}

func (e *sockEvent) hangUp(conn net.Conn) {
	e.mu.Lock()
	delete(e.conns, conn)
	e.mu.Unlock()
	conn.Close()
}

// signal to a dead creator is dropped. Nobody waits on it, as with a FIFO nobody reads.
func (e *sockEvent) signal(n int) error {
	conn, err := e.dial()
	if isRefused(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer e.hangUp(conn)
	for n > 0 {
		step := min(n, 255)
		if _, err := conn.Write([]byte{'S', byte(step)}); err != nil {
			return err
		}
		n -= step
	}
	return nil
}

// wait reports ErrPeerGone when the creator dies, because nobody can signal the event after that.
func (e *sockEvent) wait(ctx context.Context) error {
	conn, err := e.dial()
	if isRefused(err) {
		return ErrPeerGone
	}
	if err != nil {
		return err
	}
	defer e.hangUp(conn)
	if _, err := conn.Write([]byte{'W'}); err != nil {
		return e.lost()
	}
	stop := context.AfterFunc(ctx, func() { conn.Write([]byte{'C'}) })
	defer stop()
	var reply [1]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		return e.lost()
	}
	if reply[0] == 'T' {
		return nil
	}
	return ctx.Err()
}

// lost names a broken connection. A local close broke it, or the creator went away.
func (e *sockEvent) lost() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	return ErrPeerGone
}

func (e *sockEvent) close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	e.closed = true
	for conn := range e.conns {
		conn.Close()
	}
	e.mu.Unlock()
	if e.srv != nil {
		e.srv.shutdown()
		os.Remove(e.path)
	}
	return nil
}
