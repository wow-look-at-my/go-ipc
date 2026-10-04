package ipc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Request and reply types the test service speaks.
const (
	testEcho    = uint32(1)
	testEchoed  = uint32(2)
	testFail    = uint32(3)
	testWho     = uint32(4)
	testWhoAmI  = uint32(5)
	testPark    = uint32(6)
	testParked  = uint32(7)
	testCounter = uint32(8)
)

// testService answers echo, fail and who, and parks a park call until
// release closes. It counts the clients that go.
type testService struct {
	release chan struct{}
	mu      sync.Mutex
	gone    []int
	goneCh  chan int
}

func newTestService() *testService {
	return &testService{release: make(chan struct{}), goneCh: make(chan int, 64)}
}

func (h *testService) Call(s *Session, typ uint32, payload []byte) (uint32, []byte, error) {
	switch typ {
	case testEcho:
		return testEchoed, payload, nil
	case testFail:
		return 0, nil, errors.New(string(payload))
	case testWho:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(s.Ordinal()))
		return testWhoAmI, b[:], nil
	case testPark:
		<-h.release
		return testParked, nil, nil
	}
	return 0, nil, fmt.Errorf("type %d is not a request", typ)
}

func (h *testService) Gone(s *Session) {
	h.mu.Lock()
	h.gone = append(h.gone, s.Ordinal())
	h.mu.Unlock()
	h.goneCh <- s.Ordinal()
}

func serveTest(t *testing.T, name string) (*Service, *testService) {
	t.Helper()
	h := newTestService()
	svc, err := Serve(name, h, WithCapacity(MinCapacity))
	require.NoError(t, err)
	t.Cleanup(func() { svc.Close() })
	return svc, h
}

func connectTest(t *testing.T, name string) *Client {
	t.Helper()
	c, err := Connect(contextWithTimeout(t), name, WithCapacity(MinCapacity))
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

func TestServiceAnswersCalls(t *testing.T) {
	name := uniqueName(t)
	serveTest(t, name)
	c := connectTest(t, name)
	ctx := contextWithTimeout(t)

	for i := range 100 {
		typ, reply, err := c.Call(ctx, testEcho, []byte(strconv.Itoa(i)))
		require.NoError(t, err)
		assert.Equal(t, testEchoed, typ)
		assert.Equal(t, strconv.Itoa(i), string(reply))
	}

	_, _, err := c.Call(ctx, testFail, []byte("boom"))
	var ce *CallError
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, "boom", ce.Message)

	// The connection is still good after an error reply.
	typ, reply, err := c.Call(ctx, testWho, nil)
	require.NoError(t, err)
	assert.Equal(t, testWhoAmI, typ)
	assert.Equal(t, uint64(c.Ordinal()), binary.LittleEndian.Uint64(reply))
}

func TestServiceGivesEachClientAnOrdinal(t *testing.T) {
	name := uniqueName(t)
	serveTest(t, name)
	ctx := contextWithTimeout(t)

	seen := map[int]bool{}
	for range 4 {
		c := connectTest(t, name)
		typ, reply, err := c.Call(ctx, testWho, nil)
		require.NoError(t, err)
		assert.Equal(t, testWhoAmI, typ)
		assert.Equal(t, uint64(c.Ordinal()), binary.LittleEndian.Uint64(reply))
		assert.False(t, seen[c.Ordinal()], "ordinal %d given twice", c.Ordinal())
		seen[c.Ordinal()] = true
	}
	assert.Len(t, seen, 4)
}

func TestServiceClientsRunIndependently(t *testing.T) {
	name := uniqueName(t)
	_, h := serveTest(t, name)
	ctx := contextWithTimeout(t)

	parked := connectTest(t, name)
	done := make(chan error, 1)
	go func() {
		typ, _, err := parked.Call(ctx, testPark, nil)
		if err == nil && typ != testParked {
			err = fmt.Errorf("type %d", typ)
		}
		done <- err
	}()

	// A client parked in its handler holds up nobody else.
	other := connectTest(t, name)
	typ, reply, err := other.Call(ctx, testEcho, []byte("free"))
	require.NoError(t, err)
	assert.Equal(t, testEchoed, typ)
	assert.Equal(t, "free", string(reply))

	close(h.release)
	require.NoError(t, <-done)
}

// TestServiceClientWaitsForTheService connects before any service exists. The
// client parks on its own channel and the service adopts it at start.
func TestServiceClientWaitsForTheService(t *testing.T) {
	name := uniqueName(t)
	ctx := contextWithTimeout(t)

	connected := make(chan *Client, 1)
	failed := make(chan error, 1)
	go func() {
		c, err := Connect(ctx, name, WithCapacity(MinCapacity))
		if err != nil {
			failed <- err
			return
		}
		connected <- c
	}()
	// The client must have its channel in place before the service scans.
	require.Eventually(t, func() bool { return len(scanClients(name)) == 1 }, 10*time.Second, time.Millisecond)

	serveTest(t, name)
	select {
	case c := <-connected:
		defer c.Close()
		typ, reply, err := c.Call(ctx, testEcho, []byte("early"))
		require.NoError(t, err)
		assert.Equal(t, testEchoed, typ)
		assert.Equal(t, "early", string(reply))
	case err := <-failed:
		t.Fatal(err)
	}
}

func TestServiceReportsAClientThatCloses(t *testing.T) {
	name := uniqueName(t)
	_, h := serveTest(t, name)
	c := connectTest(t, name)
	ordinal := c.Ordinal()
	require.NoError(t, c.Close())
	select {
	case gone := <-h.goneCh:
		assert.Equal(t, ordinal, gone)
	case <-time.After(10 * time.Second):
		t.Fatal("the handler was not told that the client went")
	}
}

func TestServiceReportsAClientThatExits(t *testing.T) {
	name := uniqueName(t)
	_, h := serveTest(t, name)
	cmd, stdin := startPeer(t, "service-client", name)
	stdin.Close()
	require.NoError(t, cmd.Wait())
	select {
	case gone := <-h.goneCh:
		assert.Equal(t, 0, gone)
	case <-time.After(10 * time.Second):
		t.Fatal("the handler was not told that the client exited")
	}
	assert.Empty(t, scanClients(name), "the service must unlink a dead client's channel")
}

func TestServiceCloseFailsEveryCall(t *testing.T) {
	name := uniqueName(t)
	svc, _ := serveTest(t, name)
	ctx := contextWithTimeout(t)
	c := connectTest(t, name)

	done := make(chan error, 1)
	go func() {
		_, _, err := c.Call(ctx, testPark, nil)
		done <- err
	}()
	waitForWaiters(t, c.ch.rx, 1, 0)

	require.NoError(t, svc.Close())
	assert.ErrorIs(t, <-done, ErrPeerGone)
	_, _, err := c.Call(ctx, testEcho, nil)
	assert.ErrorIs(t, err, ErrPeerGone)
	assert.NoError(t, svc.Wait())

	// The name is free again.
	svc2, err := Serve(name, HandlerFunc(func(*Session, uint32, []byte) (uint32, []byte, error) {
		return testEchoed, nil, nil
	}))
	require.NoError(t, err)
	svc2.Close()
}

func TestServiceNameIsHeldWhileItRuns(t *testing.T) {
	name := uniqueName(t)
	serveTest(t, name)
	_, err := Serve(name, HandlerFunc(func(*Session, uint32, []byte) (uint32, []byte, error) {
		return 0, nil, nil
	}))
	assert.ErrorIs(t, err, ErrInUse)
}

func TestServiceClientDiscardsAStaleReply(t *testing.T) {
	name := uniqueName(t)
	_, h := serveTest(t, name)
	c := connectTest(t, name)

	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, err := c.Call(short, testPark, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// The parked reply arrives after the next call has been sent, and the next call's own reply follows it.
	close(h.release)
	ctx := contextWithTimeout(t)
	typ, reply, err := c.Call(ctx, testEcho, []byte("after"))
	require.NoError(t, err)
	assert.Equal(t, testEchoed, typ)
	assert.Equal(t, "after", string(reply))
}

func TestServiceRejectsReservedTypes(t *testing.T) {
	name := uniqueName(t)
	serveTest(t, name)
	c := connectTest(t, name)
	_, _, err := c.Call(contextWithTimeout(t), typeServiceKnock, nil)
	assert.ErrorIs(t, err, ErrReservedType)
	_, _, err = c.Call(contextWithTimeout(t), TypePadding, nil)
	assert.ErrorIs(t, err, ErrReservedType)
}

func TestServiceRejectsAnOversizedCall(t *testing.T) {
	name := uniqueName(t)
	serveTest(t, name)
	c := connectTest(t, name)
	_, _, err := c.Call(contextWithTimeout(t), testEcho, make([]byte, c.MaxPayloadSize()+1))
	assert.ErrorIs(t, err, ErrMessageTooLarge)
	typ, _, err := c.Call(contextWithTimeout(t), testEcho, make([]byte, c.MaxPayloadSize()))
	require.NoError(t, err)
	assert.Equal(t, testEchoed, typ)
}

// A prompt's token array travels in one call, so a default service channel has to hold a whole context.
func TestServiceCarriesAFullContextPrompt(t *testing.T) {
	name := uniqueName(t)
	h := newTestService()
	svc, err := Serve(name, h)
	require.NoError(t, err)
	t.Cleanup(func() { svc.Close() })
	c, err := Connect(contextWithTimeout(t), name)
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })

	prompt := make([]byte, 1<<19)
	for i := range prompt {
		prompt[i] = byte(i)
	}
	typ, reply, err := c.Call(contextWithTimeout(t), testEcho, prompt)
	require.NoError(t, err)
	assert.Equal(t, testEchoed, typ)
	assert.Equal(t, prompt, reply)
}

func TestServiceRejectsABadName(t *testing.T) {
	_, err := Serve("no/slashes", HandlerFunc(nil))
	assert.ErrorIs(t, err, ErrInvalidName)
	_, err = Connect(contextWithTimeout(t), "no/slashes")
	assert.ErrorIs(t, err, ErrInvalidName)
	_, err = Serve(uniqueName(t), nil)
	assert.Error(t, err)
}

func TestServiceConnectGivesUpWithItsContext(t *testing.T) {
	name := uniqueName(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := Connect(ctx, name, WithCapacity(MinCapacity))
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Empty(t, scanClients(name), "a client that gives up must unlink its channel")
}

// counterMessage is a hand-written Message, as ipcgen would generate one.
type counterMessage struct {
	N uint64
}

func (m *counterMessage) TypeID() uint32 { return testCounter }

func (m *counterMessage) MarshalBinary() ([]byte, error) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], m.N)
	return b[:], nil
}

func (m *counterMessage) UnmarshalBinary(b []byte) error {
	if len(b) != 8 {
		return errors.New("counter: want 8 bytes")
	}
	m.N = binary.LittleEndian.Uint64(b)
	return nil
}

func newCounterMessage(typeID uint32) Message {
	if typeID == testCounter {
		return &counterMessage{}
	}
	return nil
}

func TestServiceTypedCalls(t *testing.T) {
	name := uniqueName(t)
	h := TypedHandler{
		New: newCounterMessage,
		Call: func(s *Session, req Message) (Message, error) {
			m := req.(*counterMessage)
			if m.N == 0 {
				return nil, errors.New("zero")
			}
			return &counterMessage{N: m.N + 1}, nil
		},
	}
	svc, err := Serve(name, h.Handler(), WithCapacity(MinCapacity))
	require.NoError(t, err)
	defer svc.Close()
	c := connectTest(t, name)
	ctx := contextWithTimeout(t)

	var reply counterMessage
	require.NoError(t, c.CallTyped(ctx, &counterMessage{N: 41}, &reply))
	assert.Equal(t, uint64(42), reply.N)

	err = c.CallTyped(ctx, &counterMessage{N: 0}, &reply)
	var ce *CallError
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, "zero", ce.Message)

	// A type the schema does not know is an error reply, not a dead server.
	_, _, err = c.Call(ctx, 99, nil)
	require.ErrorAs(t, err, &ce)
	assert.Contains(t, ce.Message, "99")
}
