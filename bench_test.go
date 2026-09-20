package ipc

import (
	"context"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

var payloadSizes = []int{16, 256, 4096}

func BenchmarkRingWriteRead(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			r, err := InitRing(make([]byte, RingSize(1<<20)))
			require.Nil(b, err)

			payload := make([]byte, size)
			dst := make([]byte, size)

			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				require.NoError(b, r.TryWrite(0, payload))

				_, _, err = r.TryRecv(dst)
				require.Nil(b, err)

			}
		})
	}
}

// BenchmarkRingClaimCommit measures the path that builds a message in place.
// It skips the copy that TryWrite makes.
func BenchmarkRingClaimCommit(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			r, err := InitRing(make([]byte, RingSize(1<<20)))
			require.Nil(b, err)

			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c, err := r.TryClaim(0, size)
				require.Nil(b, err)

				c.Bytes[0] = byte(i)
				c.Commit()
				_, err = r.Read(1, func(uint32, []byte) {})
				require.Nil(b, err)

			}
		})
	}
}

// BenchmarkQueuePingPong reports the round-trip latency between goroutines
// through shared memory queues. It is the number that matters for a
// request-response workload.
func BenchmarkQueuePingPong(b *testing.B) {
	name := "bench-pingpong-" + strconv.Itoa(int(nameCounter.Add(1)))
	server, err := CreateChannel(name, WithCapacity(1<<16))
	require.Nil(b, err)

	defer func() {
		server.Close()
		server.Unlink()
	}()

	client, err := OpenChannel(name)
	require.Nil(b, err)

	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	payload := make([]byte, 64)
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 64)
		for i := 0; i < b.N; i++ {
			_, msg, err := server.RecvInto(ctx, buf)
			if err != nil {
				return
			}
			if err := server.Send(ctx, msg); err != nil {
				return
			}
		}
	}()

	buf := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, client.Send(ctx, payload))

		_, _, err = client.RecvInto(ctx, buf)
		require.Nil(b, err)

	}
	b.StopTimer()
	<-done
}

// BenchmarkQueueThroughput streams messages between goroutines without
// waiting for a reply, which is where the ring's batching shows up.
func BenchmarkQueueThroughput(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			name := "bench-throughput-" + strconv.Itoa(int(nameCounter.Add(1)))
			recv, err := CreateQueue(name, WithCapacity(1<<20))
			require.Nil(b, err)

			defer func() {
				recv.Close()
				recv.Unlink()
			}()

			send, err := OpenQueue(name)
			require.Nil(b, err)

			defer send.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			payload := make([]byte, size)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for i := 0; i < b.N; i++ {
					if err := send.Send(ctx, payload); err != nil {
						return
					}
				}
			}()

			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			read := 0
			for read < b.N {
				n, err := recv.ReadBatch(ctx, 256, func(uint32, []byte) {})
				require.Nil(b, err)

				read += n
			}
			b.StopTimer()
			<-done
		})
	}
}
