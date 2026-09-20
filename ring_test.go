package ipc

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestRing returns a ring over aligned heap memory of the given capacity.
func newTestRing(t *testing.T, capacity int) *Ring {
	t.Helper()
	r, err := InitRing(make([]byte, RingSize(capacity)))
	require.NoError(t, err)
	require.Equal(t, capacity, r.Capacity())
	return r
}

func TestRingHeaderLayout(t *testing.T) {
	buf := make([]byte, RingSize(MinCapacity))
	r, err := InitRing(buf)
	require.NoError(t, err)

	assert.Equal(t, HeaderSize, 4*cacheLine)
	assert.Equal(t, MinCapacity, r.Capacity())
	assert.True(t, r.Empty())
	assert.Equal(t, 0, r.Buffered())
}

func TestInitRingRejectsBadBuffers(t *testing.T) {
	_, err := InitRing(make([]byte, 16))
	assert.ErrorIs(t, err, ErrTooSmall)

	// One byte into an aligned allocation is guaranteed to be misaligned.
	buf := make([]byte, RingSize(MinCapacity)+8)
	_, err = InitRing(buf[1:])
	assert.ErrorIs(t, err, ErrUnaligned)
}

func TestAttachRingRejectsUnformattedBuffer(t *testing.T) {
	_, err := AttachRing(make([]byte, RingSize(MinCapacity)))
	assert.ErrorIs(t, err, ErrBadLayout)
}

func TestAttachRingSeesInitializedRing(t *testing.T) {
	buf := make([]byte, RingSize(MinCapacity))
	writer, err := InitRing(buf)
	require.NoError(t, err)
	reader, err := AttachRing(buf)
	require.NoError(t, err)

	require.NoError(t, writer.TryWrite(7, []byte("payload")))

	typ, msg, err := reader.TryRecv(nil)
	require.NoError(t, err)
	assert.Equal(t, uint32(7), typ)
	assert.Equal(t, "payload", string(msg))
}

func TestRingRoundTrip(t *testing.T) {
	r := newTestRing(t, MinCapacity)

	_, _, err := r.TryRecv(nil)
	assert.ErrorIs(t, err, ErrEmpty)

	require.NoError(t, r.TryWrite(1, []byte("alpha")))
	require.NoError(t, r.TryWrite(2, []byte("beta")))
	assert.False(t, r.Empty())

	typ, msg, err := r.TryRecv(nil)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), typ)
	assert.Equal(t, "alpha", string(msg))

	typ, msg, err = r.TryRecv(nil)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), typ)
	assert.Equal(t, "beta", string(msg))

	assert.True(t, r.Empty())
}

func TestRingEmptyPayload(t *testing.T) {
	r := newTestRing(t, MinCapacity)
	require.NoError(t, r.TryWrite(3, nil))

	typ, msg, err := r.TryRecv(nil)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), typ)
	assert.Empty(t, msg)
}

func TestRingRejectsReservedType(t *testing.T) {
	r := newTestRing(t, MinCapacity)
	assert.ErrorIs(t, r.TryWrite(TypePadding, []byte("x")), ErrReservedType)
}

func TestRingRejectsOversizedMessage(t *testing.T) {
	r := newTestRing(t, MinCapacity)
	assert.ErrorIs(t, r.TryWrite(0, make([]byte, r.MaxMessageSize()+1)), ErrMessageTooLarge)
	assert.NoError(t, r.TryWrite(0, make([]byte, r.MaxMessageSize())))
}

func TestRingReportsFull(t *testing.T) {
	r := newTestRing(t, MinCapacity)
	payload := make([]byte, 504)

	written := 0
	for {
		if err := r.TryWrite(0, payload); err != nil {
			assert.ErrorIs(t, err, ErrFull)
			break
		}
		written++
		require.Less(t, written, 100, "ring never reported full")
	}
	assert.Equal(t, MinCapacity/512, written)
}

// TestRingWrapsWithPadding drives the cursor past the end of the data region
// many times, which is the path that inserts padding records.
func TestRingWrapsWithPadding(t *testing.T) {
	r := newTestRing(t, MinCapacity)

	// 300 bytes rounds to a 312-byte record, which divides 4096 unevenly and
	// therefore straddles the wrap point on most laps.
	payload := make([]byte, 300)
	for i := range payload {
		payload[i] = byte(i)
	}

	for i := 0; i < 1000; i++ {
		require.NoError(t, r.TryWrite(uint32(i), payload), "write %d", i)
		typ, msg, err := r.TryRecv(nil)
		require.NoError(t, err, "read %d", i)
		require.Equal(t, uint32(i), typ)
		require.Equal(t, payload, msg)
	}
	assert.True(t, r.Empty())
}

func TestRingClaimCommit(t *testing.T) {
	r := newTestRing(t, MinCapacity)

	c, err := r.TryClaim(9, 5)
	require.NoError(t, err)
	require.Len(t, c.Bytes, 5)

	// The reader must not see the record before it is committed.
	_, _, err = r.TryRecv(nil)
	assert.ErrorIs(t, err, ErrEmpty)

	copy(c.Bytes, "hello")
	c.Commit()

	typ, msg, err := r.TryRecv(nil)
	require.NoError(t, err)
	assert.Equal(t, uint32(9), typ)
	assert.Equal(t, "hello", string(msg))
}

func TestRingClaimAbortReclaimsSpace(t *testing.T) {
	r := newTestRing(t, MinCapacity)

	c, err := r.TryClaim(1, 100)
	require.NoError(t, err)
	c.Abort()

	// The aborted record becomes padding: the reader skips it and reports no
	// message, but the cursor still advances past it.
	n, err := r.Read(10, func(uint32, []byte) { t.Fatal("aborted claim was delivered") })
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.True(t, r.Empty())

	require.NoError(t, r.TryWrite(2, []byte("after")))
	typ, msg, err := r.TryRecv(nil)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), typ)
	assert.Equal(t, "after", string(msg))
}

func TestRingReadBatchRespectsLimit(t *testing.T) {
	r := newTestRing(t, MinCapacity)
	for i := 0; i < 5; i++ {
		require.NoError(t, r.TryWrite(uint32(i), []byte{byte(i)}))
	}

	var seen []uint32
	n, err := r.Read(3, func(typ uint32, _ []byte) { seen = append(seen, typ) })
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, []uint32{0, 1, 2}, seen)

	seen = nil
	n, err = r.Read(10, func(typ uint32, _ []byte) { seen = append(seen, typ) })
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, []uint32{3, 4}, seen)
}

func TestRingRecvIntoReusesBuffer(t *testing.T) {
	r := newTestRing(t, MinCapacity)
	require.NoError(t, r.TryWrite(0, []byte("reuse")))

	dst := make([]byte, 64)
	_, msg, err := r.TryRecv(dst)
	require.NoError(t, err)
	assert.Equal(t, "reuse", string(msg))
	assert.Same(t, &dst[0], &msg[0], "payload should land in the supplied buffer")
}

func TestRingDetectsCorruptLength(t *testing.T) {
	r := newTestRing(t, MinCapacity)
	require.NoError(t, r.TryWrite(0, []byte("victim")))

	// Overstate the record length so it runs past the committed cursor.
	r.storeLength(0, 1<<20)
	_, err := r.Read(1, func(uint32, []byte) {})
	assert.ErrorIs(t, err, ErrCorrupt)
}

// TestRingManyProducers is the contended path: several goroutines claim
// concurrently while a single reader drains, which is the arrangement the ring
// is designed for.
func TestRingManyProducers(t *testing.T) {
	r := newTestRing(t, 1<<16)

	const (
		producers = 8
		perWriter = 2000
	)

	var (
		wg      sync.WaitGroup
		failMu  sync.Mutex
		failure string
	)
	fail := func(format string, args ...any) {
		failMu.Lock()
		defer failMu.Unlock()
		if failure == "" {
			failure = fmt.Sprintf(format, args...)
		}
	}

	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				msg := []byte(fmt.Sprintf("%d:%d", id, i))
				for {
					err := r.TryWrite(uint32(id), msg)
					if err == nil {
						break
					}
					if err != ErrFull {
						fail("producer %d: %v", id, err)
						return
					}
				}
			}
		}(p)
	}

	producersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(producersDone)
	}()

	counts := make([]int, producers)
	seen := 0
	total := producers * perWriter
	done := make(chan struct{})
	go func() {
		defer close(done)
		for seen < total {
			n, err := r.Read(64, func(typ uint32, payload []byte) {
				var id, i int
				if _, serr := fmt.Sscanf(string(payload), "%d:%d", &id, &i); serr != nil {
					fail("undecodable payload %q: %v", payload, serr)
					return
				}
				if uint32(id) != typ {
					fail("payload %q carried type %d", payload, typ)
					return
				}
				if counts[id] != i {
					fail("producer %d delivered %d after %d", id, i, counts[id])
					return
				}
				counts[id]++
			})
			if err != nil {
				fail("read: %v", err)
				return
			}
			seen += n
			// Every producer finished and the ring drained, so no further
			// message can arrive. Report the shortfall instead of hanging.
			if n == 0 && r.Empty() {
				select {
				case <-producersDone:
					fail("stalled after %d of %d messages", seen, total)
					return
				default:
				}
			}
		}
	}()

	wg.Wait()
	<-done

	require.Empty(t, failure)
	for id, got := range counts {
		assert.Equal(t, perWriter, got, "producer %d", id)
	}
}
