package ipc

import (
	"context"
	"io"
	"io/fs"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// newReceiver creates a queue this test owns and removes it afterwards.
func newReceiver(t *testing.T, name string, opts ...Option) *Queue {
	t.Helper()
	q, err := CreateQueue(name, opts...)
	require.NoError(t, err)
	t.Cleanup(func() {
		q.Close()
		q.Unlink()
	})
	return q
}

// strandClaim leaves a claim of size bytes at the tail, owned by a dead
// process. With header set, the claim has its header written, as a producer
// that dies before its commit leaves it. Without it, the claim is bare, as a
// producer that dies right after its cursor update leaves it.
func strandClaim(t *testing.T, q *Queue, owner procID, slot int, size uint64, header bool) {
	t.Helper()
	hdr := q.ring.hdr
	tail := hdr.tail.Load()
	hdr.slots[slot].owner.Store(uint64(owner))
	hdr.slots[slot].size.Store(size)
	hdr.slots[slot].at.Store(tail)
	hdr.tail.Store(tail + size)
	if header {
		q.ring.storeType(tail&q.ring.mask, 5)
		q.ring.storeLength(tail&q.ring.mask, -int32(size))
	}
}

func TestReceiverSkipsClaimOfDeadProducer(t *testing.T) {
	name := uniqueName(t)
	q := newReceiver(t, name, WithCapacity(MinCapacity))

	child := startChild(t, "abandon", name, 0)
	require.NoError(t, child.Wait())
	require.NoError(t, q.TrySend([]byte("after")))

	typ, msg, err := q.Recv(testContext(t))
	require.NoError(t, err)
	assert.Equal(t, uint32(0), typ)
	assert.Equal(t, "after", string(msg))
	assert.True(t, q.ring.Empty())
}

func TestReceiverWakesWhenProducerDiesHoldingClaim(t *testing.T) {
	name := uniqueName(t)
	q := newReceiver(t, name, WithCapacity(MinCapacity))

	holder, _ := startPeer(t, "hold", name)
	require.NoError(t, q.TrySend([]byte("after")))

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
	select {
	case msg := <-got:
		t.Fatalf("receive returned %q past a live producer's claim", msg)
	default:
	}

	kill(t, holder)
	assert.Equal(t, "after", <-got)
}

func TestReceiverReclaimsBareClaimOfDeadProducer(t *testing.T) {
	q := newReceiver(t, uniqueName(t), WithCapacity(MinCapacity))
	strandClaim(t, q, deadProcID(t), 7, 16, false)
	require.NoError(t, q.TrySend([]byte("after")))

	_, msg, err := q.Recv(testContext(t))
	require.NoError(t, err)
	assert.Equal(t, "after", string(msg))
}

func TestReceiverReclaimsDeadClaimAcrossWrap(t *testing.T) {
	q := newReceiver(t, uniqueName(t), WithCapacity(MinCapacity))
	ctx := testContext(t)

	for _, size := range []int{2040, 2024} {
		require.NoError(t, q.TrySend(make([]byte, size)))
		_, _, err := q.Recv(ctx)
		require.NoError(t, err)
	}
	require.Equal(t, uint64(MinCapacity-16), q.ring.hdr.tail.Load())

	strandClaim(t, q, deadProcID(t), 7, 48, false)
	require.NoError(t, q.TrySend([]byte("after")))

	_, msg, err := q.Recv(ctx)
	require.NoError(t, err)
	assert.Equal(t, "after", string(msg))
	assert.Equal(t, uint64(noIntent), q.ring.hdr.slots[7].at.Load())
}

func TestReceiverReclaimsWrittenClaimOfDeadProducer(t *testing.T) {
	q := newReceiver(t, uniqueName(t), WithCapacity(MinCapacity))
	strandClaim(t, q, deadProcID(t), 3, 24, true)
	require.NoError(t, q.TrySend([]byte("after")))

	typ, msg, err := q.TryRecv(nil)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), typ)
	assert.Equal(t, "after", string(msg))
}

func TestDeadProducersThatDisagreeAreCorrupt(t *testing.T) {
	q := newReceiver(t, uniqueName(t), WithCapacity(MinCapacity))
	dead := deadProcID(t)
	strandClaim(t, q, dead, 1, 16, false)
	// A second dead producer claims to own a longer range at the same spot.
	q.ring.hdr.slots[2].owner.Store(uint64(dead))
	q.ring.hdr.slots[2].size.Store(32)
	q.ring.hdr.slots[2].at.Store(q.ring.hdr.slots[1].at.Load())

	_, _, err := q.TryRecv(nil)
	assert.ErrorIs(t, err, ErrCorrupt)
}

func TestSlotsOfDeadProducersAreReused(t *testing.T) {
	q := newReceiver(t, uniqueName(t), WithCapacity(MinCapacity))
	dead := deadProcID(t)
	for idx := range q.ring.hdr.slots {
		q.ring.hdr.slots[idx].owner.Store(uint64(dead))
	}
	require.NoError(t, q.TrySend([]byte("reused")))
	_, msg, err := q.TryRecv(nil)
	require.NoError(t, err)
	assert.Equal(t, "reused", string(msg))
}

// A process with no life socket cannot be checked, so its claims and its
// slots are never judged.
func TestUnwatchableIsNeverJudged(t *testing.T) {
	q := newReceiver(t, uniqueName(t), WithCapacity(MinCapacity))
	dead := deadProcID(t) &^ watchable
	strandClaim(t, q, dead, 1, 16, false)
	for idx := 2; idx < ClaimSlots; idx++ {
		q.ring.hdr.slots[idx].owner.Store(uint64(dead))
	}

	_, _, err := q.TryRecv(nil)
	assert.ErrorIs(t, err, ErrEmpty)
	assert.Equal(t, uint64(dead), q.ring.hdr.slots[1].owner.Load())

	ctx := testContext(t)
	held, err := q.Claim(ctx, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(q.self), q.ring.hdr.slots[0].owner.Load())
	_, err = q.Claim(ctx, 0, 0)
	assert.ErrorIs(t, err, ErrTooManyClaims)
	q.Abort(held)
}

func TestUnwatchableReceiverIsNotWatched(t *testing.T) {
	name := uniqueName(t)
	q := newReceiver(t, name)
	sender, err := OpenQueue(name)
	require.NoError(t, err)
	defer sender.Close()

	q.ring.hdr.consumer.Store(uint64(deadProcID(t) &^ watchable))
	assert.NoError(t, sender.TrySend([]byte("x")))
	assert.False(t, isDead(deadProcID(t)&^watchable))
	_, err = openExit(q.self &^ watchable)
	assert.ErrorIs(t, err, errNotWatchable)
}

func TestClaimSlotsRunOut(t *testing.T) {
	q := newReceiver(t, uniqueName(t), WithCapacity(1<<16))
	ctx := testContext(t)

	claims := make([]Claim, 0, ClaimSlots)
	for range ClaimSlots {
		c, err := q.Claim(ctx, 0, 0)
		require.NoError(t, err)
		claims = append(claims, c)
	}
	_, err := q.Claim(ctx, 0, 0)
	require.ErrorIs(t, err, ErrTooManyClaims)

	for _, c := range claims {
		q.Abort(c)
	}
	require.NoError(t, q.TrySend([]byte("after")))
	_, msg, err := q.TryRecv(nil)
	require.NoError(t, err)
	assert.Equal(t, "after", string(msg))
}

func TestSendReportsDeadReceiver(t *testing.T) { sendReportsDeadReceiver(t) }

func sendReportsDeadReceiver(t *testing.T) {
	name := uniqueName(t)
	consumer, _ := startPeer(t, "consumer", name)

	q, err := OpenQueue(name)
	require.NoError(t, err)
	defer q.Close()
	payload := make([]byte, 512)
	for q.TrySend(payload) == nil {
	}

	sent := make(chan error, 1)
	go func() { sent <- q.Send(testContext(t), payload) }()
	waitForWaiters(t, q, 0, 1)

	kill(t, consumer)
	assert.ErrorIs(t, <-sent, ErrPeerGone)
	assert.ErrorIs(t, q.TrySend(payload), ErrPeerGone)

	_, err = OpenQueue(name)
	if nameOutlivesHolder {
		assert.ErrorIs(t, err, ErrPeerGone)
	} else {
		assert.ErrorIs(t, err, fs.ErrNotExist)
	}
}

func TestSendReportsClosedReceiver(t *testing.T) {
	name := uniqueName(t)
	q, err := CreateQueue(name)
	require.NoError(t, err)
	defer q.Unlink()
	sender, err := OpenQueue(name)
	require.NoError(t, err)
	defer sender.Close()

	require.NoError(t, q.Close())
	assert.ErrorIs(t, sender.TrySend([]byte("x")), ErrPeerGone)
}

func TestCreateQueueRejectsLiveHolder(t *testing.T) {
	name := uniqueName(t)
	q, err := CreateQueue(name)
	require.NoError(t, err)

	_, err = CreateQueue(name)
	require.ErrorIs(t, err, ErrInUse)

	require.NoError(t, q.Close())
	again, err := CreateQueue(name)
	require.NoError(t, err)
	require.NoError(t, again.Close())
	require.NoError(t, again.Unlink())
}

func TestCreateQueueReplacesInstanceOfDeadCreator(t *testing.T) {
	name := uniqueName(t)
	consumer, _ := startPeer(t, "consumer", name)
	old, err := readName(name)
	require.NoError(t, err)

	_, err = CreateQueue(name)
	require.ErrorIs(t, err, ErrInUse)

	kill(t, consumer)
	q := newReceiver(t, name)
	assert.NotEqual(t, old, q.inc)

	require.NoError(t, q.TrySend([]byte("fresh")))
	_, msg, err := q.TryRecv(nil)
	require.NoError(t, err)
	assert.Equal(t, "fresh", string(msg))
}

func TestOpenQueueHandleCannotReceive(t *testing.T) {
	name := uniqueName(t)
	newReceiver(t, name)
	sender, err := OpenQueue(name)
	require.NoError(t, err)
	defer sender.Close()

	_, _, err = sender.TryRecv(nil)
	assert.ErrorIs(t, err, ErrNotConsumer)
	_, _, err = sender.Recv(testContext(t))
	assert.ErrorIs(t, err, ErrNotConsumer)
	_, err = sender.ReadBatch(testContext(t), 1, func(uint32, []byte) {})
	assert.ErrorIs(t, err, ErrNotConsumer)
}

func TestConcurrentReceiversShareTheQueue(t *testing.T) {
	const count = 2000
	q := newReceiver(t, uniqueName(t), WithCapacity(1<<16))
	ctx := testContext(t)

	var (
		mu   sync.Mutex
		seen = make(map[uint32]int)
		wg   sync.WaitGroup
	)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				typ, _, err := q.Recv(ctx)
				if err != nil {
					return
				}
				mu.Lock()
				seen[typ]++
				done := len(seen) == count
				mu.Unlock()
				if done {
					q.Close()
					return
				}
			}
		}()
	}
	for idx := range count {
		require.NoError(t, q.SendTyped(ctx, uint32(idx), []byte("x")))
	}
	wg.Wait()

	require.Len(t, seen, count)
	for typ, times := range seen {
		require.Equal(t, 1, times, "message %d", typ)
	}
}

func TestChannelRecvReportsPeerClose(t *testing.T) {
	name := uniqueName(t)
	server, err := CreateChannel(name)
	require.NoError(t, err)
	defer func() {
		server.Close()
		server.Unlink()
	}()
	client, err := OpenChannel(name)
	require.NoError(t, err)

	ctx := testContext(t)
	require.NoError(t, client.Send(ctx, []byte("last")))
	require.NoError(t, client.Close())

	_, msg, err := server.Recv(ctx)
	require.NoError(t, err)
	assert.Equal(t, "last", string(msg))
	_, _, err = server.Recv(ctx)
	assert.ErrorIs(t, err, ErrPeerGone)
	assert.ErrorIs(t, server.Send(ctx, []byte("x")), ErrPeerGone)
}

func TestChannelRecvWakesOnPeerClose(t *testing.T) {
	name := uniqueName(t)
	server, err := CreateChannel(name)
	require.NoError(t, err)
	defer func() {
		server.Close()
		server.Unlink()
	}()
	client, err := OpenChannel(name)
	require.NoError(t, err)

	got := make(chan error, 1)
	go func() {
		_, _, err := client.Recv(testContext(t))
		got <- err
	}()
	waitForWaiters(t, client.rx, 1, 0)
	require.NoError(t, server.Close())
	assert.ErrorIs(t, <-got, ErrPeerGone)
	client.Close()
}

func TestChannelRecvReportsPeerExit(t *testing.T) {
	name := uniqueName(t)
	server, err := CreateChannel(name, WithCapacity(MinCapacity))
	require.NoError(t, err)
	defer func() {
		server.Close()
		server.Unlink()
	}()

	peer, _ := startPeer(t, "peer", name)
	ctx := testContext(t)
	_, msg, err := server.Recv(ctx)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(msg))

	got := make(chan error, 1)
	go func() {
		_, _, err := server.Recv(ctx)
		got <- err
	}()
	waitForWaiters(t, server.rx, 1, 0)
	kill(t, peer)
	assert.ErrorIs(t, <-got, ErrPeerGone)
	assert.ErrorIs(t, server.Send(ctx, []byte("x")), ErrPeerGone)
}

func TestChannelPeerExitWithoutClose(t *testing.T) {
	name := uniqueName(t)
	server, err := CreateChannel(name, WithCapacity(MinCapacity))
	require.NoError(t, err)
	defer func() {
		server.Close()
		server.Unlink()
	}()

	peer, stdin := startPeer(t, "peer", name)
	require.NoError(t, stdin.Close())
	require.NoError(t, peer.Wait())

	ctx := testContext(t)
	_, msg, err := server.Recv(ctx)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(msg))
	_, _, err = server.Recv(ctx)
	assert.ErrorIs(t, err, ErrPeerGone)
}

func TestOpenChannelRejectsSecondPeer(t *testing.T) {
	name := uniqueName(t)
	server, err := CreateChannel(name)
	require.NoError(t, err)
	defer func() {
		server.Close()
		server.Unlink()
	}()
	first, err := OpenChannel(name)
	require.NoError(t, err)
	defer first.Close()

	_, err = OpenChannel(name)
	assert.ErrorIs(t, err, ErrInUse)
}

func TestConnDeadlineReachesBlockedRead(t *testing.T) {
	name := uniqueName(t)
	server, err := Listen(name)
	require.NoError(t, err)
	defer func() {
		server.Close()
		server.Unlink()
	}()
	client, err := Dial(name)
	require.NoError(t, err)
	defer client.Close()

	read := make(chan error, 1)
	go func() {
		_, err := server.Read(make([]byte, 8))
		read <- err
	}()
	waitForWaiters(t, server.ch.rx, 1, 0)
	require.NoError(t, server.SetReadDeadline(time.Now().Add(10*time.Millisecond)))
	assert.ErrorIs(t, <-read, os.ErrDeadlineExceeded)

	_, err = server.Read(make([]byte, 8))
	assert.ErrorIs(t, err, os.ErrDeadlineExceeded)

	require.NoError(t, server.SetReadDeadline(time.Time{}))
	_, err = client.Write([]byte("ping"))
	require.NoError(t, err)
	buf := make([]byte, 4)
	_, err = io.ReadFull(server, buf)
	require.NoError(t, err)
	assert.Equal(t, "ping", string(buf))
}

func TestConnDeadlineExtendedWhileBlocked(t *testing.T) {
	name := uniqueName(t)
	server, err := Listen(name)
	require.NoError(t, err)
	defer func() {
		server.Close()
		server.Unlink()
	}()
	client, err := Dial(name)
	require.NoError(t, err)
	defer client.Close()

	require.NoError(t, server.SetReadDeadline(time.Now().Add(time.Hour)))
	read := make(chan error, 1)
	go func() {
		_, err := server.Read(make([]byte, 8))
		read <- err
	}()
	waitForWaiters(t, server.ch.rx, 1, 0)
	require.NoError(t, server.SetReadDeadline(time.Now().Add(-time.Second)))
	assert.ErrorIs(t, <-read, os.ErrDeadlineExceeded)
}

func TestOnExitReportsChildExit(t *testing.T) {
	_, id, stdin := startIdent(t)

	exited := make(chan error, 1)
	cancel, err := onExit(id, func(err error) { exited <- err })
	require.NoError(t, err)
	defer cancel()

	require.NoError(t, stdin.Close())
	require.NoError(t, <-exited)
	assert.True(t, isDead(id))
}

func TestOnExitCancelStopsTheCall(t *testing.T) {
	cmd, id, _ := startIdent(t)

	cancel, err := onExit(id, func(error) { t.Error("cancelled watch fired") })
	require.NoError(t, err)
	cancel()
	kill(t, cmd)

	// A second watch of the same process proves that the first one let go.
	exited := make(chan error, 1)
	_, err = onExit(id, func(err error) { exited <- err })
	require.NoError(t, err)
	require.NoError(t, <-exited)
}

func TestSelfIsAlive(t *testing.T) {
	require.NoError(t, selfErr())
	id := selfID()
	assert.True(t, id.watchable())
	assert.NotEqual(t, pendingProc, id&^watchable)
	assert.False(t, isDead(id))
}
