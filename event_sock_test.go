//go:build unix

package ipc

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The socket backend runs only on a Windows host in production. These tests
// switch it on here, so every host exercises it.
func useSockEvents(t *testing.T) {
	old := sockHost
	sockHost = func() bool { return true }
	t.Cleanup(func() { sockHost = old })
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
