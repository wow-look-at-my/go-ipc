package ipc

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Message types reserved by the stream layer. A Conn and a raw Channel must
// not share a name, because a raw Send would land here as stream data.
const (
	typeStreamData = uint32(0)
	typeStreamEOF  = uint32(1)
)

// A Conn is a net.Conn over a Channel.
//
// Write splits its input across as many messages as it needs. Read
// reassembles the stream, so the byte boundaries a caller sees are its own.
// This is the layer an encoder goes on, such as gob, protobuf or JSON.
type Conn struct {
	ch   *Channel
	addr addr

	readMu  sync.Mutex
	pending []byte
	buf     []byte
	eof     bool

	writeMu sync.Mutex
	// eofSent is set once this side has sent the end-of-stream message.
	eofSent atomic.Bool

	readDeadline  deadline
	writeDeadline deadline

	// base is cancelled by Close, so every in-flight Read and Write returns without a watcher goroutine of its own.
	base       context.Context
	cancelBase context.CancelFunc
	closeOnce  sync.Once
}

var _ net.Conn = (*Conn)(nil)

// addr names a Conn endpoint for the net.Conn interface.
type addr struct{ name string }

func (a addr) Network() string { return "ipc" }
func (a addr) String() string  { return a.name }

// Listen creates the named endpoint and returns a connection on it. It is the
// creating half of a pair; the peer calls Dial with the same name.
func Listen(name string, opts ...Option) (*Conn, error) {
	ch, err := CreateChannel(name, opts...)
	if err != nil {
		return nil, err
	}
	return newConn(ch, name), nil
}

// Dial attaches to an endpoint the peer created with Listen.
func Dial(name string, opts ...Option) (*Conn, error) {
	ch, err := OpenChannel(name, opts...)
	if err != nil {
		return nil, err
	}
	return newConn(ch, name), nil
}

// NewConn wraps an existing channel as a stream. The channel then belongs to the connection, and Close closes it.
func NewConn(ch *Channel) *Conn { return newConn(ch, ch.Name()) }

func newConn(ch *Channel, name string) *Conn {
	base, cancel := context.WithCancel(context.Background())
	return &Conn{
		ch:         ch,
		addr:       addr{name: name},
		buf:        make([]byte, ch.MaxMessageSize()),
		base:       base,
		cancelBase: cancel,
	}
}

// Channel returns the underlying channel, for a caller that wants message boundaries back.
func (c *Conn) Channel() *Channel { return c.ch }

// translate maps a context error onto the error a net.Conn caller expects.
func (c *Conn) translate(ctx context.Context, err error) error {
	switch {
	case errors.Is(context.Cause(ctx), os.ErrDeadlineExceeded):
		return os.ErrDeadlineExceeded
	case errors.Is(err, context.Canceled), errors.Is(err, ErrClosed):
		return net.ErrClosed
	default:
		return err
	}
}

// Read implements io.Reader. It returns io.EOF after the peer has
// closed and every byte it sent has been consumed. A peer that exits without
// a Close ends the stream with ErrPeerGone instead.
func (c *Conn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	for len(c.pending) == 0 {
		if c.eof {
			return 0, io.EOF
		}
		ctx, done, err := c.readDeadline.begin(c.base)
		if err != nil {
			return 0, err
		}
		typ, msg, err := c.ch.RecvInto(ctx, c.buf[:0])
		err = c.translate(ctx, err)
		done()
		if err != nil {
			return 0, err
		}
		if typ == typeStreamEOF {
			c.eof = true
			return 0, io.EOF
		}
		c.pending = msg
	}

	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

// Write implements io.Writer. It splits p into as many messages as the
// channel's maximum message size requires.
func (c *Conn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.eofSent.Load() {
		return 0, net.ErrClosed
	}

	limit := c.ch.MaxMessageSize()
	written := 0
	for written < len(p) {
		end := written + limit
		if end > len(p) {
			end = len(p)
		}
		ctx, done, err := c.writeDeadline.begin(c.base)
		if err != nil {
			return written, err
		}
		err = c.ch.SendTyped(ctx, typeStreamData, p[written:end])
		err = c.translate(ctx, err)
		done()
		if err != nil {
			return written, err
		}
		written = end
	}
	return written, nil
}

// CloseWrite ends this side's stream and keeps the read side open, like
// TCPConn.CloseWrite. It waits for room to send the end-of-stream message,
// within the write deadline. Later writes return net.ErrClosed.
func (c *Conn) CloseWrite() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.eofSent.Load() {
		return net.ErrClosed
	}
	ctx, done, err := c.writeDeadline.begin(c.base)
	if err != nil {
		return err
	}
	err = c.translate(ctx, c.ch.SendTyped(ctx, typeStreamEOF, nil))
	done()
	if err != nil {
		return err
	}
	c.eofSent.Store(true)
	return nil
}

// Close tells the peer the stream ended and releases this side's handles.
func (c *Conn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		if c.eofSent.CompareAndSwap(false, true) {
			c.ch.TrySendTyped(typeStreamEOF, nil)
		}
		c.cancelBase()
		err = c.ch.Close()
	})
	return err
}

// Unlink removes the endpoint's names. Only the side that called Listen owns them.
func (c *Conn) Unlink() error { return c.ch.Unlink() }

// LocalAddr returns the endpoint name.
func (c *Conn) LocalAddr() net.Addr { return c.addr }

// RemoteAddr returns the endpoint name, which both sides share.
func (c *Conn) RemoteAddr() net.Addr { return c.addr }

// SetDeadline sets both the read and the write deadline.
func (c *Conn) SetDeadline(t time.Time) error {
	c.readDeadline.set(t)
	c.writeDeadline.set(t)
	return nil
}

// SetReadDeadline sets the deadline for Read calls, including a Read that is
// already blocked. The zero time means no deadline.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.readDeadline.set(t)
	return nil
}

// SetWriteDeadline sets the deadline for Write calls, including a Write that
// is already blocked. The zero time means no deadline.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline.set(t)
	return nil
}
