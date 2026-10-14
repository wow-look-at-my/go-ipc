package ipc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var nameCounter atomic.Uint64

// available reports whether a call succeeded. Where the platform lacks the primitives of this package, the call must fail with errors.ErrUnsupported,
// and the test stops there.
func available(t *testing.T, err error) bool {
	t.Helper()
	if !supported {
		require.ErrorIs(t, err, errors.ErrUnsupported)
		return false
	}
	require.NoError(t, err)
	return true
}

// contextWithTimeout bounds a blocking call in a test, so a defect fails the
// test rather than hangs it.
func contextWithTimeout(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// waitForWaiters blocks until the ring reports at least the given number of
// parked receivers and senders.
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
