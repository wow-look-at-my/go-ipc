package ipc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestEventDoesNotStealForeignWakeups covers a liveness hazard specific to a
// wake handle that processes share.
//
// A process that consumes a token with nobody waiting behind it strands the
// waiter in the other process, which then sleeps with no further signal due.
// Only a waiter reads here, so an idle process takes nothing.
func TestEventDoesNotStealForeignWakeups(t *testing.T) {
	name := uniqueName(t)

	thief, err := CreateEvent(name)
	require.NoError(t, err)
	defer func() {
		thief.Close()
		thief.Unlink()
	}()

	victim, err := OpenEvent(name)
	require.NoError(t, err)
	defer victim.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Give the thief a live reader handle the only way a caller can: wait
	// once and be released. The handle stays open in its pool afterwards.
	require.NoError(t, thief.Signal())
	require.NoError(t, thief.Wait(ctx))

	// The victim parks, and exactly enough wakeups for it are signalled.
	parked := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(parked)
		result <- victim.Wait(ctx)
	}()
	<-parked

	require.NoError(t, thief.Signal())

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("the waiter never woke: an idle process took its wakeup")
	}
}

// TestQueueWakesSenderInAnotherProcess is the same hazard at the queue layer,
// across a real process boundary.
//
// The child fills the queue and parks in Send. This side drains the queue and
// must release it. A wakeup lost to a local reader would hang the child.
func TestQueueWakesSenderInAnotherProcess(t *testing.T) {
	const count = 2000

	name := uniqueName(t)
	q, err := CreateQueue(name, WithCapacity(MinCapacity))
	require.NoError(t, err)
	defer func() {
		q.Close()
		q.Unlink()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Park a sender in this process then release it. That leaves a reader
	// running here with no waiter behind it, which is the state that lets a
	// wakeup go astray.
	payload := make([]byte, 504)
	for q.TrySend(payload) == nil {
	}
	local := make(chan error, 1)
	go func() { local <- q.Send(ctx, payload) }()

	buf := make([]byte, len(payload))
	_, _, err = q.RecvInto(ctx, buf)
	require.NoError(t, err)
	require.NoError(t, <-local)

	// Drain what is left so the child starts against an empty queue.
	for {
		if _, _, rerr := q.TryRecv(buf); rerr != nil {
			require.ErrorIs(t, rerr, ErrEmpty)
			break
		}
	}

	child := startChild(t, "sender", name, count)
	for i := 0; i < count; i++ {
		_, _, rerr := q.RecvInto(ctx, buf)
		require.NoError(t, rerr, "message %d", i)
	}
	require.NoError(t, child.Wait())
}
