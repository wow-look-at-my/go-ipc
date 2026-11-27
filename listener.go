//go:build cosmo || darwin || linux || windows

// listener.go holds Listener.

package ipc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
)

// The stream listener is the handshake of spec/service.md under names of its
// own.
const (
	listenerRegistrySuffix = ".lsn"
	listenerClientPrefix   = ".s."
)

// A Listener accepts a stream from each process that dials its name.
type Listener struct {
	name   string
	reg    *Queue
	ctx    context.Context
	cancel context.CancelFunc
	// done ends when the goroutine that adopts dialers has returned.
	done chan struct{}
	// ready holds one token while the backlog may hold a stream.
	ready chan struct{}
	next  int

	mu sync.Mutex
	// backlog holds the streams that have their hello and no Accept yet.
	backlog []*Conn
	err     error
	closing sync.Once
}

var _ net.Listener = (*Listener)(nil)

// ListenStream creates the listener. It returns ErrInUse while another
// process listens on the name. A dialer that came first is accepted too.
func ListenStream(name string, opts ...Option) (*Listener, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	reg, err := CreateQueue(name+listenerRegistrySuffix, opts...)
	if err != nil {
		return nil, fmt.Errorf("ipc: listen %q: %w", name, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	l := &Listener{
		name: name, reg: reg, ctx: ctx, cancel: cancel,
		done:  make(chan struct{}),
		ready: make(chan struct{}, 1),
	}
	// The queue is published, so a dialer the scan misses opens the queue and knocks.
	go l.run(scanChannels(name, listenerClientPrefix))
	return l, nil
}

// DialStream connects to the named listener. A listener that does not exist
// yet is waited for, parked in the kernel, until ctx ends. Close removes the
// names of the connection.
func DialStream(ctx context.Context, name string, opts ...Option) (*Conn, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	ch, _, err := handshake(ctx, name, listenerRegistrySuffix, listenerClientPrefix, opts, nil)
	if err != nil {
		return nil, err
	}
	c := newConn(ch, name)
	c.reap = reapAlways
	return c, nil
}

// run adopts the dialers that wait, then each that knocks.
func (l *Listener) run(waiting []string) {
	defer close(l.done)
	for _, id := range waiting {
		l.adopt(id)
	}
	l.stop(readKnocks(l.ctx, l.reg, l.adopt))
}

// adopt opens a dialer's channel, sends its hello and queues its stream for
// Accept. A dialer therefore never waits for an Accept.
func (l *Listener) adopt(id string) {
	ch, err := OpenChannel(l.name + listenerClientPrefix + id)
	if err != nil {
		return
	}
	if err := sendHello(l.ctx, ch, l.next); err != nil {
		ch.Close()
		return
	}
	l.next++
	c := newConn(ch, l.name)
	c.reap = reapPeerGone
	l.mu.Lock()
	l.backlog = append(l.backlog, c)
	l.mu.Unlock()
	l.signal()
}

// signal leaves the token that wakes one Accept.
func (l *Listener) signal() {
	select {
	case l.ready <- struct{}{}:
	default:
	}
}

// take removes the oldest stream of the backlog.
func (l *Listener) take() *Conn {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.backlog) == 0 {
		return nil
	}
	c := l.backlog[0]
	l.backlog = l.backlog[1:]
	if len(l.backlog) > 0 {
		l.signal()
	}
	return c
}

// stop records why the listener ended, unless Close ended it.
func (l *Listener) stop(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err == nil && !errors.Is(err, ErrClosed) && !errors.Is(err, context.Canceled) {
		l.err = err
	}
	l.cancel()
}

// Accept waits for the next dialer. It returns net.ErrClosed after Close.
func (l *Listener) Accept() (net.Conn, error) {
	for l.ctx.Err() == nil {
		if c := l.take(); c != nil {
			return c, nil
		}
		select {
		case <-l.ready:
		case <-l.ctx.Done():
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return nil, l.err
	}
	return nil, net.ErrClosed
}

// Close stops the listener and removes its name. An accepted stream stays
// open. A stream that no Accept took is closed.
func (l *Listener) Close() error {
	var err error
	l.closing.Do(func() {
		l.cancel()
		err = l.reg.Close()
		<-l.done
		for c := l.take(); c != nil; c = l.take() {
			c.Close()
		}
		if uerr := l.reg.Unlink(); err == nil {
			err = uerr
		}
	})
	return err
}

// Addr returns the listener name.
func (l *Listener) Addr() net.Addr { return addr{name: l.name} }
