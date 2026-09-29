//go:build unix

package ipc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
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
	srv := &sockServer{ln: ln}
	go srv.serve()
	return &sockEvent{path: path, srv: srv, conns: make(map[net.Conn]struct{})}, nil
}

// openSockEvent dials once, so a name with no live creator fails here rather than at the first wait.
func openSockEvent(name string) (*sockEvent, error) {
	path := sockPath(name)
	conn, err := net.Dial("unix", path)
	if err != nil {
		return nil, err
	}
	conn.Close()
	return &sockEvent{path: path, conns: make(map[net.Conn]struct{})}, nil
}

func (s *sockServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.client(conn)
	}
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

func (e *sockEvent) signal(n int) error {
	conn, err := e.dial()
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

func (e *sockEvent) wait(ctx context.Context) error {
	conn, err := e.dial()
	if err != nil {
		return err
	}
	defer e.hangUp(conn)
	if _, err := conn.Write([]byte{'W'}); err != nil {
		return ErrClosed
	}
	stop := context.AfterFunc(ctx, func() { conn.Write([]byte{'C'}) })
	defer stop()
	var reply [1]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		return ErrClosed
	}
	if reply[0] == 'T' {
		return nil
	}
	return ctx.Err()
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
		e.srv.ln.Close()
		os.Remove(e.path)
	}
	return nil
}
