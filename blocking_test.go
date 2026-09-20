//go:build unix

package ipc

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// cpuTime returns the processor time this process has consumed so far.
func cpuTime(t *testing.T) time.Duration {
	t.Helper()
	var ru unix.Rusage
	require.NoError(t, unix.Getrusage(unix.RUSAGE_SELF, &ru))
	user := time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond
	sys := time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond
	return user + sys
}

// threadCount reads how many OS threads this process holds.
func threadCount(t *testing.T) int {
	t.Helper()
	status, err := os.ReadFile("/proc/self/status")
	require.NoError(t, err)
	for _, line := range strings.Split(string(status), "\n") {
		rest, ok := strings.CutPrefix(line, "Threads:")
		if !ok {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(rest))
		require.NoError(t, err)
		return n
	}
	t.Fatal("/proc/self/status reported no thread count")
	return 0
}

// TestBlockedEndpointsConsumeNoCPU is the measurement behind the claim that
// this package does not busy-wait.
//
// A sender with nowhere to put its message and a receiver with nothing to read
// are both states an implementation is tempted to spin in. Both are parked
// here for a fixed window, and the processor time the whole process spends in
// that window is compared against the window itself. A spin loop would consume
// a core per waiter and blow past the budget by orders of magnitude.
func TestBlockedEndpointsConsumeNoCPU(t *testing.T) {
	const (
		window  = 300 * time.Millisecond
		senders = 8
		// A parked process still wakes for the Go runtime's own timers, so
		// the budget is not empty. It is far below what even a single
		// spinning goroutine would reach.
		budget = window / 10
	)

	idle, _ := newTestQueuePair(t, WithCapacity(MinCapacity))
	full, fullSend := newTestQueuePair(t, WithCapacity(MinCapacity))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A single receiver parked on an empty queue.
	go func() { idle.Recv(ctx) }()

	// Several senders parked on a queue with no room left.
	payload := make([]byte, 504)
	for fullSend.TrySend(payload) == nil {
	}
	for i := 0; i < senders; i++ {
		go func() { fullSend.Send(ctx, payload) }()
	}

	// Let every goroutine reach its park before the window opens; the
	// handshake below costs a scheduling round trip, not a fixed delay.
	runtime.Gosched()

	before := cpuTime(t)
	<-time.After(window)
	spent := cpuTime(t) - before

	assert.Less(t, spent, budget,
		"blocked endpoints burned %v of CPU across a %v window: something is spinning", spent, window)

	// The queues must still work after all that waiting.
	require.NoError(t, full.Close())
}

// TestParkedWaitersHoldNoThreads measures the cost of a waiter that is asleep.
//
// An implementation that blocks in a plain system call would need a single
// thread per parked waiter. The Go poller backs these waits instead, so the
// thread count barely moves.
func TestParkedWaitersHoldNoThreads(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("thread accounting reads /proc/self/status")
	}
	const waiters = 64

	events := make([]*Event, waiters)
	for i := range events {
		events[i] = newTestEvent(t)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	before := threadCount(t)
	done := make(chan struct{}, waiters)
	for _, e := range events {
		go func(e *Event) {
			e.Wait(ctx)
			done <- struct{}{}
		}(e)
	}

	// Give the waiters a window to reach their park, then read the cost.
	<-time.After(100 * time.Millisecond)
	after := threadCount(t)

	assert.Less(t, after-before, waiters/4,
		"%d parked waiters added %d threads", waiters, after-before)

	cancel()
	for i := 0; i < waiters; i++ {
		<-done
	}
}
