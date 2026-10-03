package ipc

import (
	"encoding/binary"
	"fmt"
	"strings"
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

	assert.Equal(t, HeaderSize, 4*cacheLine+ClaimSlots*slotSize)
	assert.Equal(t, MinCapacity, r.Capacity())
	assert.True(t, r.Empty())
	assert.Equal(t, 0, r.Buffered())
}

func TestInitRingRejectsBadBuffers(t *testing.T) {
	_, err := InitRing(make([]byte, 16))
	assert.ErrorIs(t, err, ErrTooSmall)

	// A single byte into an aligned allocation is guaranteed to be misaligned.
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

// A claimed record whose header is not written yet must stop the reader, even
// when the earlier lap left payload bytes there that look like a record.
func TestReadIgnoresBytesLeftByAnEarlierLap(t *testing.T) {
	r := newTestRing(t, MinCapacity)

	fake := make([]byte, 24)
	fake[8], fake[12] = 16, 7
	require.NoError(t, r.TryWrite(1, fake))
	require.NoError(t, r.TryWrite(1, make([]byte, 2024)))
	require.NoError(t, r.TryWrite(1, make([]byte, 2024)))
	n, err := r.Read(10, func(uint32, []byte) {})
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, uint64(MinCapacity), r.hdr.tail.Load())

	require.NoError(t, r.TryWrite(2, make([]byte, 8)))
	r.hdr.tail.Add(16)

	var types []uint32
	n, err = r.Read(10, func(typ uint32, _ []byte) { types = append(types, typ) })
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, []uint32{2}, types)
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

	// A 300-byte payload rounds to a 312-byte record.
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

	// The aborted record becomes padding: the reader skips it and reports no message.
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

// TestRingStaleHeadCacheDoesNotOverrun sets the state that a producer leaves
// when a scheduler stops it between its load of head and its store into
// headCache: a cached head more than a lap old, on a full ring.
func TestRingStaleHeadCacheDoesNotOverrun(t *testing.T) {
	r := newTestRing(t, MinCapacity)
	payload := make([]byte, r.MaxMessageSize())
	for lap := 0; lap < 3; lap++ {
		require.NoError(t, r.TryWrite(1, payload))
		require.NoError(t, r.TryWrite(1, payload))
		_, err := r.Read(2, func(uint32, []byte) {})
		require.NoError(t, err)
	}
	require.NoError(t, r.TryWrite(2, payload))
	require.NoError(t, r.TryWrite(2, payload))

	r.hdr.headCache.Store(0)
	_, err := r.TryClaim(3, 0)
	require.ErrorIs(t, err, ErrFull, "a stale cached head must not open room in a full ring")

	n, err := r.Read(10, func(typ uint32, _ []byte) { assert.Equal(t, uint32(2), typ) })
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

// TestRingReadStopsAtUnpublishedClaim sets the state between a producer's
// advance of tail and its store of the header.
func TestRingReadStopsAtUnpublishedClaim(t *testing.T) {
	r := newTestRing(t, MinCapacity)
	first := make([]byte, r.MaxMessageSize())
	binary.LittleEndian.PutUint32(first[8:], RecordHeaderSize)
	binary.LittleEndian.PutUint32(first[12:], 7)
	require.NoError(t, r.TryWrite(1, first))
	require.NoError(t, r.TryWrite(1, make([]byte, r.MaxMessageSize())))
	_, err := r.Read(2, func(uint32, []byte) {})
	require.NoError(t, err)

	require.NoError(t, r.TryWrite(2, []byte("12345678")))
	r.hdr.tail.Add(16)

	var types []uint32
	n, err := r.Read(10, func(typ uint32, _ []byte) { types = append(types, typ) })
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, []uint32{2}, types, "the reader must stop at the unpublished claim")
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
				// Varied sizes put later headers on earlier payload bytes.
				msg := []byte(fmt.Sprintf("%d:%d:%s", id, i, strings.Repeat("#", (i*7+id)%61)))
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
			// The producers must be seen done BEFORE the read. Then every record is committed and visible to it.
			finished := false
			select {
			case <-producersDone:
				finished = true
			default:
			}
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
			// After the producers finish, a read that delivers nothing means a record is lost or stuck uncommitted.
			if n == 0 && finished {
				fail("stalled after %d of %d messages, empty=%v", seen, total, r.Empty())
				return
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
