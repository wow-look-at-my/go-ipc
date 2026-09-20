package ipc

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestQueuePair creates a queue and opens another handle on it, the way
// processes would, and removes the name afterwards.
func newTestQueuePair(t *testing.T, opts ...Option) (*Queue, *Queue) {
	t.Helper()
	name := uniqueName(t)

	creator, err := CreateQueue(name, opts...)
	require.NoError(t, err)
	t.Cleanup(func() {
		creator.Close()
		creator.Unlink()
	})

	opener, err := OpenQueue(name)
	require.NoError(t, err)
	t.Cleanup(func() { opener.Close() })

	return creator, opener
}

func TestQueueSendRecvAcrossHandles(t *testing.T) {
	recv, send := newTestQueuePair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, send.Send(ctx, []byte("over here")))

	typ, msg, err := recv.Recv(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), typ)
	assert.Equal(t, "over here", string(msg))
}

func TestQueueOpenReportsCreatorCapacity(t *testing.T) {
	recv, send := newTestQueuePair(t, WithCapacity(1<<16))
	assert.Equal(t, 1<<16, recv.Capacity())
	assert.Equal(t, 1<<16, send.Capacity())
	assert.Equal(t, 1<<15-RecordHeaderSize, send.MaxMessageSize())
}

func TestQueueRejectsBadCapacity(t *testing.T) {
	_, err := CreateQueue(uniqueName(t), WithCapacity(1000))
	assert.ErrorIs(t, err, ErrInvalidCapacity)

	_, err = CreateQueue(uniqueName(t), WithCapacity(5000))
	assert.ErrorIs(t, err, ErrInvalidCapacity)
}

func TestQueueRejectsInvalidName(t *testing.T) {
	_, err := CreateQueue("bad/name")
	assert.ErrorIs(t, err, ErrInvalidName)
}

func TestOpenQueueRejectsMissingName(t *testing.T) {
	_, err := OpenQueue(uniqueName(t))
	assert.Error(t, err)
}

func TestQueueTrySendReportsFull(t *testing.T) {
	recv, send := newTestQueuePair(t, WithCapacity(MinCapacity))

	payload := make([]byte, 504)
	var err error
	for i := 0; i < 64 && err == nil; i++ {
		err = send.TrySend(payload)
	}
	require.ErrorIs(t, err, ErrFull)

	_, _, rerr := recv.TryRecv(nil)
	require.NoError(t, rerr)
	assert.NoError(t, send.TrySend(payload), "space freed by the receiver should be usable")
}

func TestQueueTryRecvReportsEmpty(t *testing.T) {
	recv, _ := newTestQueuePair(t)
	_, _, err := recv.TryRecv(nil)
	assert.ErrorIs(t, err, ErrEmpty)
}

// TestQueueSendBlocksUntilReceiverDrains exercises the not-full wakeup: the
// sender fills the queue, parks, and is released by the receiver.
func TestQueueSendBlocksUntilReceiverDrains(t *testing.T) {
	recv, send := newTestQueuePair(t, WithCapacity(MinCapacity))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const messages = 200
	payload := make([]byte, 504)

	sent := make(chan error, 1)
	go func() {
		for i := 0; i < messages; i++ {
			if err := send.Send(ctx, payload); err != nil {
				sent <- err
				return
			}
		}
		sent <- nil
	}()

	buf := make([]byte, len(payload))
	for i := 0; i < messages; i++ {
		_, msg, err := recv.RecvInto(ctx, buf)
		require.NoError(t, err, "message %d", i)
		require.Len(t, msg, len(payload))
	}
	require.NoError(t, <-sent)
}

func TestQueueRecvHonorsContext(t *testing.T) {
	recv, _ := newTestQueuePair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, _, err := recv.Recv(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestQueueSendHonorsContext(t *testing.T) {
	_, send := newTestQueuePair(t, WithCapacity(MinCapacity))

	payload := make([]byte, 504)
	for send.TrySend(payload) == nil {
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, send.Send(ctx, payload), context.DeadlineExceeded)
}

func TestQueueClaimCommit(t *testing.T) {
	recv, send := newTestQueuePair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := send.Claim(ctx, 5, 4)
	require.NoError(t, err)
	copy(c.Bytes, "zero")
	send.Commit(c)

	typ, msg, err := recv.Recv(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(5), typ)
	assert.Equal(t, "zero", string(msg))
}

// TestQueueAbortReclaimsSpace covers the claim a sender gives up on. The
// receiver must skip it rather than deliver it, and the space must come back.
func TestQueueAbortReclaimsSpace(t *testing.T) {
	recv, send := newTestQueuePair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := send.Claim(ctx, 7, 64)
	require.NoError(t, err)
	send.Abort(c)

	_, _, err = recv.TryRecv(nil)
	require.ErrorIs(t, err, ErrEmpty, "an aborted claim must not reach the receiver")
	assert.True(t, recv.Ring().Empty())

	require.NoError(t, send.Send(ctx, []byte("next")))
	typ, msg, err := recv.Recv(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), typ)
	assert.Equal(t, "next", string(msg))
}

// TestQueueAbortReleasesParkedSender covers a deadlock an abort can leave.
//
// An abort commits a padding record. That frees no space by itself: only the
// receiver can step over padding and move the cursor. So the abort has to
// reach a parked receiver, and the receiver has to announce the space it frees
// even though padding delivers no message. Miss either half and the sender
// below waits for a wakeup nobody sends.
func TestQueueAbortReleasesParkedSender(t *testing.T) {
	recv, send := newTestQueuePair(t, WithCapacity(MinCapacity))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Claim most of the ring, then fill what is left.
	held, err := send.Claim(ctx, 1, 1024)
	require.NoError(t, err)

	payload := make([]byte, 504)
	for send.TrySend(payload) == nil {
	}

	// This receiver parks: every record ahead of it sits behind the claim.
	received := make(chan error, 1)
	go func() {
		_, _, rerr := recv.Recv(ctx)
		received <- rerr
	}()

	// This sender parks: the ring has no room until the claim resolves.
	parked := make(chan error, 1)
	go func() { parked <- send.Send(ctx, payload) }()

	// Both peers must be genuinely asleep before the abort, or the test
	// exercises the non-blocking path and proves nothing.
	waitForWaiters(t, send, 1, 1)

	send.Abort(held)

	require.NoError(t, <-received)
	require.NoError(t, <-parked, "the sender never woke after the abort")
}

func TestQueueReadBatchDrainsBurst(t *testing.T) {
	recv, send := newTestQueuePair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		require.NoError(t, send.SendTyped(ctx, uint32(i), []byte{byte(i)}))
	}

	var seen []uint32
	n, err := recv.ReadBatch(ctx, 16, func(typ uint32, _ []byte) { seen = append(seen, typ) })
	require.NoError(t, err)
	assert.Equal(t, 10, n)
	assert.Equal(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, seen)
}

// TestQueueManySendersOneReceiver is the shape the queue is named for.
func TestQueueManySendersOneReceiver(t *testing.T) {
	recv, send := newTestQueuePair(t, WithCapacity(1<<16))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		senders   = 6
		perSender = 500
	)

	var wg sync.WaitGroup
	errs := make(chan error, senders)
	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perSender; i++ {
				if err := send.SendTyped(ctx, uint32(id), []byte(fmt.Sprintf("%d:%d", id, i))); err != nil {
					errs <- err
					return
				}
			}
		}(s)
	}

	counts := make([]int, senders)
	for i := 0; i < senders*perSender; i++ {
		typ, msg, err := recv.Recv(ctx)
		require.NoError(t, err)
		var id, seq int
		_, serr := fmt.Sscanf(string(msg), "%d:%d", &id, &seq)
		require.NoError(t, serr)
		require.Equal(t, uint32(id), typ)
		require.Equal(t, counts[id], seq, "sender %d out of order", id)
		counts[id]++
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	for id, got := range counts {
		assert.Equal(t, perSender, got, "sender %d", id)
	}
}

func TestQueueClosedRejectsUse(t *testing.T) {
	q, err := CreateQueue(uniqueName(t))
	require.NoError(t, err)
	defer q.Unlink()

	require.NoError(t, q.Close())
	assert.ErrorIs(t, q.Close(), ErrClosed)
	assert.ErrorIs(t, q.TrySend(nil), ErrClosed)

	_, _, rerr := q.TryRecv(nil)
	assert.ErrorIs(t, rerr, ErrClosed)
}
