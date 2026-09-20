package ipc

import (
	"context"
	"strconv"
	"testing"
)

var payloadSizes = []int{16, 256, 4096}

func BenchmarkRingWriteRead(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			r, err := InitRing(make([]byte, RingSize(1<<20)))
			if err != nil {
				b.Fatal(err)
			}
			payload := make([]byte, size)
			dst := make([]byte, size)

			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := r.TryWrite(0, payload); err != nil {
					b.Fatal(err)
				}
				if _, _, err := r.TryRecv(dst); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRingClaimCommit measures the zero-copy path, which skips the copy
// that TryWrite makes.
func BenchmarkRingClaimCommit(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			r, err := InitRing(make([]byte, RingSize(1<<20)))
			if err != nil {
				b.Fatal(err)
			}

			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c, err := r.TryClaim(0, size)
				if err != nil {
					b.Fatal(err)
				}
				c.Bytes[0] = byte(i)
				c.Commit()
				if _, err := r.Read(1, func(uint32, []byte) {}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkQueuePingPong reports the round-trip latency between two
// goroutines through two shared memory queues. It is the number that matters
// for a request-response workload.
func BenchmarkQueuePingPong(b *testing.B) {
	name := "bench-pingpong-" + strconv.Itoa(int(nameCounter.Add(1)))
	server, err := CreateChannel(name, WithCapacity(1<<16))
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		server.Close()
		server.Unlink()
	}()

	client, err := OpenChannel(name)
	if err != nil {
		b.Fatal(err)
	}
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
		if err := client.Send(ctx, payload); err != nil {
			b.Fatal(err)
		}
		if _, _, err := client.RecvInto(ctx, buf); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	<-done
}

// BenchmarkQueueThroughput streams messages between two goroutines without
// waiting for a reply, which is where the ring's batching shows up.
func BenchmarkQueueThroughput(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			name := "bench-throughput-" + strconv.Itoa(int(nameCounter.Add(1)))
			recv, err := CreateQueue(name, WithCapacity(1<<20))
			if err != nil {
				b.Fatal(err)
			}
			defer func() {
				recv.Close()
				recv.Unlink()
			}()

			send, err := OpenQueue(name)
			if err != nil {
				b.Fatal(err)
			}
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
				if err != nil {
					b.Fatal(err)
				}
				read += n
			}
			b.StopTimer()
			<-done
		})
	}
}
