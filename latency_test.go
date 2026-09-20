//go:build unix

package ipc

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// The benchmarks here bound what a parked round trip can cost on the machine
// running them. A queue round trip cannot beat the goroutine handoff, and it
// cannot beat the kernel round trip that carries the wakeup between processes.
// Read BenchmarkQueuePingPong against these, not against empty.

// BenchmarkGoChannelPingPong is the scheduler floor: a round trip between
// goroutines with no kernel involved at all.
func BenchmarkGoChannelPingPong(b *testing.B) {
	ping := make(chan struct{})
	pong := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)
		for i := 0; i < b.N; i++ {
			<-ping
			pong <- struct{}{}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ping <- struct{}{}
		<-pong
	}
	b.StopTimer()
	<-done
}

// BenchmarkPipePingPong is the kernel floor: a byte each way over a pair of
// pipes, through the same poller a parked queue wait uses.
func BenchmarkPipePingPong(b *testing.B) {
	aR, aW, err := os.Pipe()
	require.NoError(b, err)
	defer aR.Close()
	defer aW.Close()

	bR, bW, err := os.Pipe()
	require.NoError(b, err)
	defer bR.Close()
	defer bW.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		var buf [1]byte
		for i := 0; i < b.N; i++ {
			if _, rerr := aR.Read(buf[:]); rerr != nil {
				return
			}
			if _, werr := bW.Write(buf[:]); werr != nil {
				return
			}
		}
	}()

	var buf [1]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, werr := aW.Write(buf[:]); werr != nil {
			break
		}
		if _, rerr := bR.Read(buf[:]); rerr != nil {
			break
		}
	}
	b.StopTimer()
	<-done
}

// BenchmarkEventPingPong isolates the wake primitive from the ring. It is the
// cost a queue pays whenever both sides have to sleep.
func BenchmarkEventPingPong(b *testing.B) {
	toServer := newBenchEvent(b)
	toClient := newBenchEvent(b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < b.N; i++ {
			if toServer.Wait(ctx) != nil {
				return
			}
			if toClient.Signal() != nil {
				return
			}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if toServer.Signal() != nil {
			break
		}
		if toClient.Wait(ctx) != nil {
			break
		}
	}
	b.StopTimer()
	<-done
}

func newBenchEvent(b *testing.B) *Event {
	b.Helper()
	e, err := CreateEvent("bench-event-" + strconv.Itoa(int(nameCounter.Add(1))))
	require.NoError(b, err)
	b.Cleanup(func() {
		e.Close()
		e.Unlink()
	})
	return e
}
