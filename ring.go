package ipc

import (
	"math/bits"
	"sync/atomic"
	"unsafe"
)

const (
	// cacheLine separates independently written cursors. The value is x86
	// lines, which also defeats the adjacent-line prefetcher.
	cacheLine = 128

	ringMagic   = uint64(0x676F2D6970632D31) // "go-ipc-1"
	ringVersion = uint32(1)

	// RecordHeaderSize is the per-message overhead a ring adds, in bytes.
	RecordHeaderSize = 8

	// HeaderSize is the size of the ring control block that precedes the
	// data region.
	HeaderSize = 4 * cacheLine

	// MinCapacity is the smallest data region a ring accepts.
	MinCapacity = 4096

	recordAlignment = 8

	// TypePadding fills the tail of the data region when a record would
	// otherwise straddle the wrap point. A reader skips it.
	TypePadding = uint32(0xFFFFFFFF)
)

// ringHeader is the control block. It is mapped directly onto the
// earliest HeaderSize bytes of the buffer, so field order and padding are
// the wire format and must not change without a version bump.
type ringHeader struct {
	magic    uint64
	version  uint32
	flags    uint32
	capacity uint64
	_        [cacheLine - 24]byte

	// tail is the producer cursor. Producers claim by advancing it.
	tail atomic.Uint64
	_    [cacheLine - 8]byte

	// head is the consumer cursor. Only the consumer advances it.
	head atomic.Uint64
	_    [cacheLine - 8]byte

	// headCache lets a producer skip reading head while the ring has room.
	headCache atomic.Uint64
	// recvWaiters counts consumers parked on the not-empty event.
	recvWaiters atomic.Int32
	// sendWaiters counts producers parked on the not-full event.
	sendWaiters atomic.Int32
	_           [cacheLine - 16]byte
}

// A wrong size here would put a cursor on the wrong cache line and silently
// lose the false-sharing guarantee, so the build fails instead.
var _ [0]struct{} = [unsafe.Sizeof(ringHeader{}) - HeaderSize]struct{}{}

// Ring is a lock-free multi-producer single-consumer buffer of
// variable-length records held inside a caller-supplied byte slice.
//
// Any number of goroutines in any number of processes may write. Exactly a
// single goroutine may read. A Ring holds no pointer into the Go heap, so
// the same bytes work through a shared memory mapping.
type Ring struct {
	hdr  *ringHeader
	data []byte
	mask uint64
}

// RingSize returns the buffer size needed for a ring with the given data
// capacity, which must be a power of of at least MinCapacity bytes.
func RingSize(capacity int) int {
	return HeaderSize + capacity
}

// InitRing formats buf as an empty ring and returns a handle to it.
//
// The data capacity is the largest power of that fits after the header.
// Every byte of buf is overwritten. Exactly a single participant calls
// InitRing; the others call AttachRing.
func InitRing(buf []byte) (*Ring, error) {
	if len(buf) < HeaderSize+MinCapacity {
		return nil, ErrTooSmall
	}
	if uintptr(unsafe.Pointer(&buf[0]))%8 != 0 {
		return nil, ErrUnaligned
	}
	capacity := uint64(1) << (bits.Len64(uint64(len(buf)-HeaderSize)) - 1)

	clear(buf)
	hdr := (*ringHeader)(unsafe.Pointer(&buf[0]))
	hdr.capacity = capacity
	hdr.version = ringVersion
	// The magic is written last and read so a peer that attaches while
	// this runs sees either nothing or a complete header.
	atomic.StoreUint64(&hdr.magic, ringMagic)

	return newRing(hdr, buf, capacity), nil
}

// AttachRing returns a handle to a ring another participant already wrote
// into buf. It does not modify the buffer.
func AttachRing(buf []byte) (*Ring, error) {
	if len(buf) < HeaderSize+MinCapacity {
		return nil, ErrTooSmall
	}
	if uintptr(unsafe.Pointer(&buf[0]))%8 != 0 {
		return nil, ErrUnaligned
	}
	hdr := (*ringHeader)(unsafe.Pointer(&buf[0]))
	if atomic.LoadUint64(&hdr.magic) != ringMagic || hdr.version != ringVersion {
		return nil, ErrBadLayout
	}
	capacity := hdr.capacity
	if capacity < MinCapacity || capacity&(capacity-1) != 0 || uint64(len(buf)) < HeaderSize+capacity {
		return nil, ErrBadLayout
	}
	return newRing(hdr, buf, capacity), nil
}

func newRing(hdr *ringHeader, buf []byte, capacity uint64) *Ring {
	return &Ring{
		hdr:  hdr,
		data: buf[HeaderSize : HeaderSize+capacity],
		mask: capacity - 1,
	}
}

// Capacity returns the size of the data region in bytes.
func (r *Ring) Capacity() int { return int(r.mask) + 1 }

// MaxMessageSize returns the largest payload a single record may carry.
//
// The limit is half the capacity so that a claim always fits even when the
// wrap point forces a padding record ahead of it.
func (r *Ring) MaxMessageSize() int { return r.Capacity()/2 - RecordHeaderSize }

// Buffered returns the number of bytes claimed but not yet consumed. It is a
// point-in-time sample of a value other participants are changing.
func (r *Ring) Buffered() int {
	return int(r.hdr.tail.Load() - r.hdr.head.Load())
}

// Empty reports whether the consumer has drained every committed record.
func (r *Ring) Empty() bool {
	return r.hdr.head.Load() == r.hdr.tail.Load()
}

func align(n int32) uint64 {
	return uint64((n + recordAlignment - 1) &^ (recordAlignment - 1))
}

func (r *Ring) loadLength(idx uint64) int32 {
	return atomic.LoadInt32((*int32)(unsafe.Pointer(&r.data[idx])))
}

func (r *Ring) storeLength(idx uint64, v int32) {
	atomic.StoreInt32((*int32)(unsafe.Pointer(&r.data[idx])), v)
}

// The type field is ordered by the release store and the acquire load of the
// length beside it, so it needs no atomic of its own.
func (r *Ring) loadType(idx uint64) uint32 {
	return *(*uint32)(unsafe.Pointer(&r.data[idx+4]))
}

func (r *Ring) storeType(idx uint64, v uint32) {
	*(*uint32)(unsafe.Pointer(&r.data[idx+4])) = v
}

// A Claim is a reserved region of a ring that a producer may fill in place.
//
// A single side of Commit or Abort must follow. Until then the reader stops
// at the claim, so a claim left open stalls the ring.
type Claim struct {
	r     *Ring
	index uint64
	total int32

	// Bytes is the payload region. Writes to it become visible to the
	// reader on Commit.
	Bytes []byte
}

// Commit publishes the claim to the reader.
func (c Claim) Commit() {
	c.r.storeLength(c.index, c.total)
}

// Abort discards the claim. The region becomes a padding record, which the
// reader skips and reclaims.
func (c Claim) Abort() {
	c.r.storeType(c.index, TypePadding)
	c.r.storeLength(c.index, c.total)
}

// TryClaim reserves room for a payload of length bytes and returns it for the
// caller to fill. It returns ErrFull when the ring has no room.
//
// This is the allocation-free write path: build the message straight into
// Claim.Bytes rather than into a buffer that Write then copies.
func (r *Ring) TryClaim(typ uint32, length int) (Claim, error) {
	if typ == TypePadding {
		return Claim{}, ErrReservedType
	}
	if length < 0 || length > r.MaxMessageSize() {
		return Claim{}, ErrMessageTooLarge
	}

	recordLen := int32(RecordHeaderSize + length)
	aligned := align(recordLen)
	capacity := uint64(r.Capacity())

	var index uint64
	for {
		tail := r.hdr.tail.Load()
		head := r.hdr.headCache.Load()

		index = tail & r.mask
		toEnd := capacity - index
		need := aligned
		if toEnd < aligned {
			// The record cannot straddle the wrap point, so the claim
			// also covers a padding record over the remaining bytes.
			need = aligned + toEnd
		}

		if capacity-(tail-head) < need {
			head = r.hdr.head.Load()
			if capacity-(tail-head) < need {
				return Claim{}, ErrFull
			}
			r.hdr.headCache.Store(head)
		}

		if !r.hdr.tail.CompareAndSwap(tail, tail+need) {
			continue
		}
		if need != aligned {
			r.storeType(index, TypePadding)
			r.storeLength(index, int32(toEnd))
			index = 0
		}
		break
	}

	// A negative length marks the record claimed but not yet readable. The
	// reader stops on it, and it tells a diagnostic tool the difference
	// between a slot in flight and a slot never used.
	r.storeLength(index, -recordLen)
	r.storeType(index, typ)

	return Claim{
		r:     r,
		index: index,
		total: recordLen,
		Bytes: r.data[index+RecordHeaderSize : index+uint64(recordLen) : index+uint64(recordLen)],
	}, nil
}

// TryWrite copies payload into the ring as a single record of the given
// type. It returns ErrFull when the ring has no room.
func (r *Ring) TryWrite(typ uint32, payload []byte) error {
	c, err := r.TryClaim(typ, len(payload))
	if err != nil {
		return err
	}
	copy(c.Bytes, payload)
	c.Commit()
	return nil
}

// ReadFunc receives a single record. The payload aliases the ring and stays
// valid only until the function returns, so a caller that keeps it must copy it.
type ReadFunc func(typ uint32, payload []byte)

// Read passes up to limit committed records to fn and returns how many it
// passed. It never blocks: a return of empty means the ring is empty or
// the next record is still being written.
//
// Only a single goroutine across all processes may call Read on a ring.
func (r *Ring) Read(limit int, fn ReadFunc) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	head := r.hdr.head.Load()
	available := r.hdr.tail.Load() - head

	var consumed uint64
	count := 0
	for count < limit && consumed < available {
		index := (head + consumed) & r.mask
		length := r.loadLength(index)
		if length <= 0 {
			break
		}
		step := align(length)
		if length < RecordHeaderSize || step > available-consumed {
			return count, ErrCorrupt
		}
		typ := r.loadType(index)
		consumed += step

		if typ != TypePadding {
			fn(typ, r.data[index+RecordHeaderSize:index+uint64(length):index+uint64(length)])
			count++
		}
		// Zeroing before head moves keeps the slot unreadable on the next
		// lap until its new producer commits.
		r.storeLength(index, 0)
	}

	if consumed > 0 {
		r.hdr.head.Store(head + consumed)
	}
	return count, nil
}

// TryRecv copies the next record into dst and returns its type and the
// payload. It returns ErrEmpty when no record is ready.
//
// When dst is too small, or nil, TryRecv allocates a new slice.
func (r *Ring) TryRecv(dst []byte) (uint32, []byte, error) {
	var (
		typ uint32
		out []byte
	)
	n, err := r.Read(1, func(t uint32, payload []byte) {
		typ = t
		if cap(dst) >= len(payload) {
			out = dst[:len(payload)]
		} else {
			out = make([]byte, len(payload))
		}
		copy(out, payload)
	})
	if err != nil {
		return 0, nil, err
	}
	if n == 0 {
		return 0, nil, ErrEmpty
	}
	return typ, out, nil
}
