//go:build unix

package ipc

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sockEventsEnv switches the socket backend on in a child process too.
const sockEventsEnv = "GO_IPC_TEST_SOCK_EVENTS"

func init() {
	if os.Getenv(sockEventsEnv) == "1" {
		sockHost = func() bool { return true }
	}
}

// The socket backend runs only on a Windows host in production. These tests
// switch it on here, so every host exercises it.
func useSockEvents(t *testing.T) {
	t.Setenv(sockEventsEnv, "1")
	old := sockHost
	sockHost = func() bool { return true }
	t.Cleanup(func() { sockHost = old })
}

func TestSockEventSendReportsDeadReceiver(t *testing.T) {
	useSockEvents(t)
	sendReportsDeadReceiver(t)
}

func TestSockEventSignalBeforeWait(t *testing.T) {
	useSockEvents(t)
	name := uniqueName(t)
	creator, err := CreateEvent(name)
	require.NoError(t, err)
	defer creator.Unlink()
	defer creator.Close()
	opener, err := OpenEvent(name)
	require.NoError(t, err)
	defer opener.Close()

	require.NoError(t, opener.Signal())
	require.NoError(t, creator.Wait(testContext(t)))
}

// An open handle keeps working after the creator unlinks the name. A new
// waiter there must not need the path.
func TestEventWaitsAfterUnlink(t *testing.T) {
	waitsAfterUnlink(t)
}

func TestSockEventWaitsAfterUnlink(t *testing.T) {
	useSockEvents(t)
	waitsAfterUnlink(t)
}

func waitsAfterUnlink(t *testing.T) {
	name := uniqueName(t)
	creator, err := CreateEvent(name)
	require.NoError(t, err)
	defer creator.Close()
	opener, err := OpenEvent(name)
	require.NoError(t, err)
	defer opener.Close()
	require.NoError(t, creator.Unlink())

	result := make(chan error, 1)
	go func() { result <- opener.Wait(testContext(t)) }()
	require.NoError(t, creator.Signal())
	require.NoError(t, <-result)
}

// Waiters of one handle share its connection. Each signal releases exactly one
// of them, and a token that no waiter of the handle takes goes back.
func TestSockEventWaitersShareAConnection(t *testing.T) {
	useSockEvents(t)
	name := uniqueName(t)
	creator, err := CreateEvent(name)
	require.NoError(t, err)
	defer creator.Unlink()
	defer creator.Close()
	opener, err := OpenEvent(name)
	require.NoError(t, err)
	defer opener.Close()

	const n = 4
	done := make(chan error, n)
	for range n {
		go func() { done <- opener.Wait(testContext(t)) }()
	}
	require.Eventually(t, func() bool {
		creator.impl.sock.srv.mu.Lock()
		defer creator.impl.sock.srv.mu.Unlock()
		return len(creator.impl.sock.srv.waiting) == n
	}, 10*time.Second, time.Millisecond)
	for range n {
		require.NoError(t, creator.Signal())
		require.NoError(t, <-done)
	}

	// The token of a waiter that gave up goes to the next waiter anywhere.
	ctx, cancel := context.WithCancel(context.Background())
	gaveUp := make(chan error, 1)
	go func() { gaveUp <- opener.Wait(ctx) }()
	require.Eventually(t, func() bool {
		creator.impl.sock.srv.mu.Lock()
		defer creator.impl.sock.srv.mu.Unlock()
		return len(creator.impl.sock.srv.waiting) == 1
	}, 10*time.Second, time.Millisecond)
	require.NoError(t, creator.Signal())
	cancel()
	if err := <-gaveUp; err != nil {
		assert.ErrorIs(t, err, context.Canceled)
		require.NoError(t, creator.Wait(testContext(t)), "the token of the cancelled waiter was lost")
	}
}

func TestSockEventOpenRejectsMissingName(t *testing.T) {
	useSockEvents(t)
	_, err := OpenEvent(uniqueName(t))
	assert.Error(t, err)
}

func TestSockEventLongName(t *testing.T) {
	useSockEvents(t)
	name := uniqueName(t) + strings.Repeat("x", 150)
	creator, err := CreateEvent(name)
	require.NoError(t, err)
	defer creator.Unlink()
	defer creator.Close()
	require.NoError(t, creator.Signal())
	require.NoError(t, creator.Wait(testContext(t)))
}

func TestSockEventReleasesParkedWaiter(t *testing.T) {
	useSockEvents(t)
	name := uniqueName(t)
	creator, err := CreateEvent(name)
	require.NoError(t, err)
	defer creator.Unlink()
	defer creator.Close()

	done := make(chan error, 1)
	go func() { done <- creator.Wait(testContext(t)) }()
	select {
	case err := <-done:
		t.Fatalf("wait returned %v with no signal", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, creator.SignalN(1))
	require.NoError(t, <-done)
}

func TestSockEventCancelKeepsToken(t *testing.T) {
	useSockEvents(t)
	name := uniqueName(t)
	creator, err := CreateEvent(name)
	require.NoError(t, err)
	defer creator.Unlink()
	defer creator.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, creator.Wait(ctx), context.Canceled)

	require.NoError(t, creator.Signal())
	require.NoError(t, creator.Wait(testContext(t)))
}

func TestSockEventQueueRoundTrip(t *testing.T) {
	useSockEvents(t)
	q := newReceiver(t, uniqueName(t), WithCapacity(MinCapacity))
	got := make(chan string, 1)
	go func() {
		_, msg, err := q.Recv(testContext(t))
		if err != nil {
			got <- err.Error()
			return
		}
		got <- string(msg)
	}()
	waitForWaiters(t, q, 1, 0)
	require.NoError(t, q.TrySend([]byte("over a socket")))
	assert.Equal(t, "over a socket", <-got)
}
