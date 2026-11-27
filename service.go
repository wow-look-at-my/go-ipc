//go:build cosmo || darwin || linux || windows

// service.go holds serviceRegistrySuffix.

package ipc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync"
)

// The service layer of spec/service.md: a server answers calls from any number
// of client processes. Each client owns a channel; a queue the server owns
// carries the knock that announces a new channel.
const (
	serviceRegistrySuffix = ".svc"
	serviceClientPrefix   = ".c."

	// serviceReservedMin starts the record types the service layer keeps for itself.
	serviceReservedMin = uint32(0xFFFFFFF0)
	typeServiceKnock   = uint32(0xFFFFFFF0)
	typeServiceHello   = uint32(0xFFFFFFF1)
	typeServiceError   = uint32(0xFFFFFFF2)

	// serviceSeqSize is the sequence number that starts every request and reply payload.
	serviceSeqSize   = 8
	serviceFirstSeq  = uint64(1)
	serviceClientIDs = incarnationLen
)

// serviceCapacity sizes a client channel for a whole call payload, such as a prompt's token array.
const serviceCapacity = 1 << 24

// A CallError is the error a service handler returned, carried to the client.
type CallError struct {
	Message string
}

func (e *CallError) Error() string { return "ipc: call failed: " + e.Message }

// A Message is a typed message, as ipcgen generates it: it knows its own type
// ID and encodes itself.
type Message interface {
	TypeID() uint32
	MarshalBinary() ([]byte, error)
	UnmarshalBinary([]byte) error
}

// A Handler answers the calls of a service's clients.
type Handler interface {
	Call(s *Session, typ uint32, payload []byte) (replyTyp uint32, reply []byte, err error)
	Gone(s *Session)
}

// A HandlerFunc is a Handler that ignores clients going away.
type HandlerFunc func(s *Session, typ uint32, payload []byte) (uint32, []byte, error)

func (f HandlerFunc) Call(s *Session, typ uint32, payload []byte) (uint32, []byte, error) {
	return f(s, typ, payload)
}

func (HandlerFunc) Gone(*Session) {}

// A TypedHandler is a Handler over ipcgen messages. New is the generated
// NewMessage of the schema.
type TypedHandler struct {
	New  func(typeID uint32) Message
	Call func(s *Session, req Message) (Message, error)
	Gone func(s *Session)
}

// Handler returns the Handler form of h.
func (h TypedHandler) Handler() Handler { return typedHandler{h} }

type typedHandler struct{ h TypedHandler }

func (t typedHandler) Call(s *Session, typ uint32, payload []byte) (uint32, []byte, error) {
	req := t.h.New(typ)
	if req == nil {
		return 0, nil, fmt.Errorf("ipc: no message has type ID %d", typ)
	}
	if err := req.UnmarshalBinary(payload); err != nil {
		return 0, nil, err
	}
	reply, err := t.h.Call(s, req)
	if err != nil {
		return 0, nil, err
	}
	if reply == nil {
		return 0, nil, errors.New("ipc: the handler returned no reply")
	}
	b, err := reply.MarshalBinary()
	if err != nil {
		return 0, nil, err
	}
	return reply.TypeID(), b, nil
}

func (t typedHandler) Gone(s *Session) {
	if t.h.Gone != nil {
		t.h.Gone(s)
	}
}

// A Session is one client of a Service, as the Handler sees it.
type Session struct {
	ordinal int
	id      string
	ch      *Channel
}

// Ordinal is the number the service gave this client.
func (s *Session) Ordinal() int { return s.ordinal }

// A Service answers calls under a name. Serve starts it, and it runs until
// Close or until its queue fails.
type Service struct {
	name    string
	handler Handler
	reg     *Queue
	ctx     context.Context
	cancel  context.CancelFunc
	// knocks ends when the goroutine that reads the queue has returned.
	knocks chan struct{}

	mu       sync.Mutex
	next     int
	sessions map[*Session]bool
	err      error
	closing  sync.Once
}

// Serve creates the service and returns once a client can reach it. It
// returns ErrInUse while another process serves the name. Every client that
// created its connection before this call is adopted now; every later one is
// adopted when it knocks.
func Serve(name string, handler Handler, opts ...Option) (*Service, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("ipc: serve needs a handler")
	}
	reg, err := CreateQueue(name+serviceRegistrySuffix, opts...)
	if err != nil {
		return nil, fmt.Errorf("ipc: serve %q: %w", name, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		name: name, handler: handler, reg: reg, ctx: ctx, cancel: cancel,
		knocks:   make(chan struct{}),
		sessions: make(map[*Session]bool),
	}
	// The queue is published, so a client whose channel the scan misses opens the queue and knocks.
	for _, id := range scanClients(name) {
		s.adopt(id)
	}
	go s.run()
	return s, nil
}

// Name returns the service name.
func (s *Service) Name() string { return s.name }

// run adopts each client that knocks until the queue closes.
func (s *Service) run() {
	defer close(s.knocks)
	s.stop(readKnocks(s.ctx, s.reg, s.adopt))
}

// readKnocks hands adopt the id of each client that knocks on reg. It returns the error that ended the queue.
func readKnocks(ctx context.Context, reg *Queue, adopt func(id string)) error {
	buf := make([]byte, serviceClientIDs)
	for {
		typ, payload, err := reg.RecvInto(ctx, buf)
		if err != nil {
			return err
		}
		if typ != typeServiceKnock {
			continue
		}
		if id, ok := parseIncarnation(payload); ok {
			adopt(id)
		}
	}
}

// stop records why the service ended, unless Close ended it.
func (s *Service) stop(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil && !errors.Is(err, ErrClosed) && !errors.Is(err, context.Canceled) {
		s.err = err
	}
	s.cancel()
}

// adopt opens a client's channel and serves it. An open that fails means the
// channel was adopted already, its client has gone. Otherwise, the client
// has not finished creating it and knocks when it has.
func (s *Service) adopt(id string) {
	ch, err := OpenChannel(s.name + serviceClientPrefix + id)
	if err != nil {
		return
	}
	s.mu.Lock()
	sess := &Session{ordinal: s.next, id: id, ch: ch}
	s.next++
	s.sessions[sess] = true
	s.mu.Unlock()
	if err := sendHello(s.ctx, ch, sess.ordinal); err != nil {
		s.forget(sess)
		return
	}
	go s.serve(sess)
}

// forget closes a session's channel and drops it from the service.
func (s *Service) forget(sess *Session) {
	s.mu.Lock()
	delete(s.sessions, sess)
	s.mu.Unlock()
	sess.ch.Close()
}

// serve answers one client until it goes or the service closes. A client
// that went has its channel removed before the handler hears of it.
func (s *Service) serve(sess *Session) {
	defer s.forget(sess)
	ch := sess.ch
	// The buffer grows to the largest request this client sent.
	var buf []byte
	for {
		typ, msg, err := ch.RecvInto(s.ctx, buf)
		if errors.Is(err, ErrPeerGone) {
			ch.Close()
			ch.Unlink()
			s.handler.Gone(sess)
			return
		}
		if err != nil {
			return
		}
		if err := s.answer(sess, typ, msg); err != nil {
			return
		}
		buf = msg[:0]
	}
}

// answer runs the handler on one request and sends its reply.
func (s *Service) answer(sess *Session, typ uint32, msg []byte) error {
	if len(msg) < serviceSeqSize {
		return s.reply(sess, typeServiceError, 0, []byte("ipc: request is shorter than its sequence number"))
	}
	seq := binary.LittleEndian.Uint64(msg)
	if typ >= serviceReservedMin {
		return s.reply(sess, typeServiceError, seq, []byte(fmt.Sprintf("ipc: request type %d is reserved", typ)))
	}
	replyTyp, reply, err := s.handler.Call(sess, typ, msg[serviceSeqSize:])
	if err != nil {
		return s.reply(sess, typeServiceError, seq, []byte(err.Error()))
	}
	if replyTyp >= serviceReservedMin {
		return s.reply(sess, typeServiceError, seq, []byte(fmt.Sprintf("ipc: reply type %d is reserved", replyTyp)))
	}
	return s.reply(sess, replyTyp, seq, reply)
}

func (s *Service) reply(sess *Session, typ uint32, seq uint64, payload []byte) error {
	return sess.ch.SendTyped(s.ctx, typ, frame(seq, payload))
}

// frame prefixes payload with its sequence number.
func frame(seq uint64, payload []byte) []byte {
	b := make([]byte, serviceSeqSize+len(payload))
	binary.LittleEndian.PutUint64(b, seq)
	copy(b[serviceSeqSize:], payload)
	return b
}

// Close stops the service. Every client parked in a call finds the service
// gone. It then removes the service's name. A handler that is still running
// is not waited for; its reply fails when it returns.
func (s *Service) Close() error {
	var err error
	s.closing.Do(func() {
		s.cancel()
		err = s.reg.Close()
		<-s.knocks
		s.mu.Lock()
		sessions := make([]*Session, 0, len(s.sessions))
		for sess := range s.sessions {
			sessions = append(sessions, sess)
		}
		s.mu.Unlock()
		for _, sess := range sessions {
			sess.ch.Close()
		}
		if uerr := s.reg.Unlink(); err == nil {
			err = uerr
		}
	})
	return err
}

// Wait blocks until the service stops. It returns nil after Close, and the
// error that stopped the service otherwise.
func (s *Service) Wait() error {
	<-s.ctx.Done()
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()
	return err
}

// scanClients lists the ids of the client channels that exist under the
// service name.
func scanClients(name string) []string {
	return scanChannels(name, serviceClientPrefix)
}

// scanChannels lists the ids of the channels that exist under name and a client prefix.
func scanChannels(name, clientPrefix string) []string {
	entries, err := os.ReadDir(runtimeDir())
	if err != nil {
		return nil
	}
	prefix := "go-ipc-" + name + clientPrefix
	suffix := chanOpenerToCreator + ".name"
	var ids []string
	for _, entry := range entries {
		rest, ok := strings.CutPrefix(entry.Name(), prefix)
		if !ok {
			continue
		}
		id, ok := strings.CutSuffix(rest, suffix)
		if !ok {
			continue
		}
		if _, ok := parseIncarnation([]byte(id)); ok {
			ids = append(ids, id)
		}
	}
	return ids
}

// A Client is a connection to a Service. One call is in flight at a time;
// concurrent calls wait for each other.
type Client struct {
	name    string
	ordinal int
	ch      *Channel
	mu      sync.Mutex
	seq     uint64
}

// Connect connects to the named service. A service that does not exist yet is
// waited for, parked in the kernel, until ctx ends.
func Connect(ctx context.Context, name string, opts ...Option) (*Client, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	return connect(ctx, name, opts, nil)
}

// connect is Connect after the name check. reg is the service's queue when
// the caller holds it open already; the knock then goes out on it, and
// connect closes it.
func connect(ctx context.Context, name string, opts []Option, reg *Queue) (*Client, error) {
	ch, ordinal, err := handshake(ctx, name, serviceRegistrySuffix, serviceClientPrefix, serviceOptions(opts), reg)
	if err != nil {
		return nil, err
	}
	return &Client{name: name, ordinal: ordinal, ch: ch, seq: serviceFirstSeq - 1}, nil
}

// handshake is the client half of a connection. It creates a channel of its
// own under name and clientPrefix, knocks on the queue name and
// registrySuffix give, and waits for the hello. It returns the channel and
// the ordinal the server gave it. reg is that queue when the caller holds it
// open; handshake closes it.
func handshake(ctx context.Context, name, registrySuffix, clientPrefix string, opts []Option, reg *Queue) (*Channel, int, error) {
	id, err := newIncarnation()
	if err != nil {
		closeQueue(reg)
		return nil, 0, err
	}
	ch, err := CreateChannel(name+clientPrefix+id, opts...)
	if err != nil {
		closeQueue(reg)
		return nil, 0, fmt.Errorf("ipc: connect %q: %w", name, err)
	}
	knock(ctx, name+registrySuffix, id, reg)
	typ, msg, err := ch.Recv(ctx)
	if err == nil && (typ != typeServiceHello || len(msg) != serviceSeqSize) {
		err = fmt.Errorf("%w: the server sent type %d before hello", ErrCorrupt, typ)
	}
	if err != nil {
		ch.Close()
		ch.Unlink()
		return nil, 0, fmt.Errorf("ipc: connect %q: %w", name, err)
	}
	return ch, int(binary.LittleEndian.Uint64(msg)), nil
}

// sendHello gives a client its ordinal, which is the first record on its channel.
func sendHello(ctx context.Context, ch *Channel, ordinal int) error {
	var hello [serviceSeqSize]byte
	binary.LittleEndian.PutUint64(hello[:], uint64(ordinal))
	return ch.SendTyped(ctx, typeServiceHello, hello[:])
}

// TryConnect connects to a service that runs now. Where Connect waits for a
// service to start, TryConnect returns ErrNoService at once when no live
// process serves the name. A service that still starts is waited for.
func TryConnect(ctx context.Context, name string, opts ...Option) (*Client, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	q, err := OpenQueue(name + serviceRegistrySuffix)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, ErrPeerGone) {
		return nil, fmt.Errorf("ipc: connect %q: %w", name, ErrNoService)
	}
	if err != nil {
		// Any other failure leaves the answer to the connect, within ctx.
		q = nil
	}
	// The knock goes out on the queue this check opened.
	return connect(ctx, name, opts, q)
}

// serviceOptions puts the service capacity first, so a caller that names its own capacity still wins.
func serviceOptions(opts []Option) []Option {
	return append([]Option{WithCapacity(serviceCapacity)}, opts...)
}

// knock tells a running service about a new client. A service that is not up yet finds the client's channel when it starts, so a failure here costs nothing.
func knock(ctx context.Context, registry, id string, q *Queue) {
	if q == nil {
		var err error
		if q, err = OpenQueue(registry); err != nil {
			return
		}
	}
	defer q.Close()
	q.SendTyped(ctx, typeServiceKnock, []byte(id))
}

// closeQueue closes q when there is one.
func closeQueue(q *Queue) {
	if q != nil {
		q.Close()
	}
}

// Name returns the service name.
func (c *Client) Name() string { return c.name }

// Ordinal is the number the service gave this connection.
func (c *Client) Ordinal() int { return c.ordinal }

// MaxPayloadSize returns the largest request or reply payload a call carries.
func (c *Client) MaxPayloadSize() int { return c.ch.MaxMessageSize() - serviceSeqSize }

// Call sends one request and returns the reply. A handler error comes back
// as a *CallError. A service that exited comes back as ErrPeerGone.
func (c *Client) Call(ctx context.Context, typ uint32, payload []byte) (uint32, []byte, error) {
	if typ >= serviceReservedMin {
		return 0, nil, ErrReservedType
	}
	if len(payload) > c.MaxPayloadSize() {
		return 0, nil, ErrMessageTooLarge
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	seq := c.seq
	if err := c.ch.SendTyped(ctx, typ, frame(seq, payload)); err != nil {
		return 0, nil, err
	}
	for {
		rtyp, msg, err := c.ch.Recv(ctx)
		if err != nil {
			return 0, nil, err
		}
		if len(msg) < serviceSeqSize {
			return 0, nil, fmt.Errorf("%w: a reply is shorter than its sequence number", ErrCorrupt)
		}
		rseq := binary.LittleEndian.Uint64(msg)
		// A reply below the sequence answers a call this client gave up on.
		if rseq < seq {
			continue
		}
		if rseq > seq {
			return 0, nil, fmt.Errorf("%w: reply sequence %d is ahead of call %d", ErrCorrupt, rseq, seq)
		}
		switch {
		case rtyp == typeServiceError:
			return 0, nil, &CallError{Message: string(msg[serviceSeqSize:])}
		case rtyp >= serviceReservedMin:
			return 0, nil, fmt.Errorf("%w: reply type %d is reserved", ErrCorrupt, rtyp)
		}
		return rtyp, msg[serviceSeqSize:], nil
	}
}

// CallTyped sends req and decodes the reply into reply. A reply of another
// type than reply's is an error that names both.
func (c *Client) CallTyped(ctx context.Context, req, reply Message) error {
	payload, err := req.MarshalBinary()
	if err != nil {
		return err
	}
	typ, msg, err := c.Call(ctx, req.TypeID(), payload)
	if err != nil {
		return err
	}
	if typ != reply.TypeID() {
		return fmt.Errorf("ipc: call of type %d got a reply of type %d, want %d", req.TypeID(), typ, reply.TypeID())
	}
	return reply.UnmarshalBinary(msg)
}

// Close ends the connection and removes its name. The service sees the client go.
func (c *Client) Close() error {
	err := c.ch.Close()
	if uerr := c.ch.Unlink(); err == nil {
		err = uerr
	}
	return err
}
