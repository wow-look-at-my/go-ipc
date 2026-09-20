package ipc

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var nameCounter atomic.Uint64

// waitForWaiters blocks until the ring reports at least the given number of
// parked receivers and senders.
//
// A test that wants a peer to be genuinely asleep has to wait for it. Launching
// the goroutine is not enough: it may not have run yet, and a test that races
// ahead of the park exercises the non-blocking path instead of the thing it
// means to. These are the same counters the wakeup protocol itself reads.
func waitForWaiters(t *testing.T, q *Queue, recv, send int32) {
	t.Helper()
	require.Eventually(t, func() bool {
		return q.ring.hdr.recvWaiters.Load() >= recv &&
			q.ring.hdr.sendWaiters.Load() >= send
	}, 10*time.Second, time.Millisecond, "waiters never parked")
}

// uniqueName builds a shared-memory name that no other test or concurrent run
// can collide with. The names live in a system-wide namespace, so the process
// id has to be part of them.
func uniqueName(t *testing.T) string {
	t.Helper()
	clean := strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' {
			return '-'
		}
		return r
	}, t.Name())
	return fmt.Sprintf("test-%s-%d-%d", clean, os.Getpid(), nameCounter.Add(1))
}
