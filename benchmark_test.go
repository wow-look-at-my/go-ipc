package ipc

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

var payloadSizes = []int{16, 256, 4096}

// A benchmark counts its failures rather than asserting inside the timed loop,
// because an assertion helper measured per iteration would be reported as the
// cost of the operation under test.

func BenchmarkRingWriteRead(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			r, err := InitRing(make([]byte, RingSize(1<<20)))
			require.NoError(b, err)

			payload := make([]byte, size)
			dst := make([]byte, size)
			fails := 0

			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if r.TryWrite(0, payload) != nil {
					fails++
				}
				if _, _, rerr := r.TryRecv(dst); rerr != nil {
					fails++
				}
			}
			b.StopTimer()
			require.Zero(b, fails)
		})
	}
}

// BenchmarkRingClaimCommit measures the path that builds a message in place.
// It skips the copy that TryWrite makes.
func BenchmarkRingClaimCommit(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			r, err := InitRing(make([]byte, RingSize(1<<20)))
			require.NoError(b, err)

			fails := 0

			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c, cerr := r.TryClaim(0, size)
				if cerr != nil {
					fails++
					continue
				}
				c.Bytes[0] = byte(i)
				c.Commit()
				if _, rerr := r.Read(1, discard); rerr != nil {
					fails++
				}
			}
			b.StopTimer()
			require.Zero(b, fails)
		})
	}
}

func discard(uint32, []byte) {}

// BenchmarkQueuePingPong reports the round-trip latency between goroutines
// through shared memory queues. It is the number that matters for a
// request-response workload.
func BenchmarkQueuePingPong(b *testing.B) {
	name := "bench-pingpong-" + strconv.Itoa(int(nameCounter.Add(1)))
	server, err := CreateChannel(name, WithCapacity(1<<16))
	require.NoError(b, err)
	defer func() {
		server.Close()
		server.Unlink()
	}()

	client, err := OpenChannel(name)
	require.NoError(b, err)
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	payload := make([]byte, 64)
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 64)
		for i := 0; i < b.N; i++ {
			_, msg, rerr := server.RecvInto(ctx, buf)
			if rerr != nil {
				return
			}
			if server.Send(ctx, msg) != nil {
				return
			}
		}
	}()

	buf := make([]byte, 64)
	fails := 0

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if client.Send(ctx, payload) != nil {
			fails++
			break
		}
		if _, _, rerr := client.RecvInto(ctx, buf); rerr != nil {
			fails++
			break
		}
	}
	b.StopTimer()

	require.Zero(b, fails)
	<-done
}

// BenchmarkQueueThroughput streams messages between goroutines without
// waiting for a reply, which is where the ring's batching shows up.
func BenchmarkQueueThroughput(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			name := "bench-throughput-" + strconv.Itoa(int(nameCounter.Add(1)))
			recv, err := CreateQueue(name, WithCapacity(1<<20))
			require.NoError(b, err)
			defer func() {
				recv.Close()
				recv.Unlink()
			}()

			send, err := OpenQueue(name)
			require.NoError(b, err)
			defer send.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			payload := make([]byte, size)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for i := 0; i < b.N; i++ {
					if send.Send(ctx, payload) != nil {
						return
					}
				}
			}()

			read, fails := 0, 0

			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for read < b.N {
				n, rerr := recv.ReadBatch(ctx, 256, discard)
				if rerr != nil {
					fails++
					break
				}
				read += n
			}
			b.StopTimer()

			require.Zero(b, fails)
			<-done
		})
	}
}
