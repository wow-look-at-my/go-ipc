package ipc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestEvent creates an event whose name is unique to the running test and
// removes it afterwards.
func newTestEvent(t *testing.T) *Event {
	t.Helper()
	e, err := CreateEvent(uniqueName(t))
	require.NoError(t, err)
	t.Cleanup(func() {
		e.Close()
		e.Unlink()
	})
	return e
}

func TestEventSignalReleasesWaiter(t *testing.T) {
	e := newTestEvent(t)

	waiting := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(waiting)
		result <- e.Wait(context.Background())
	}()

	<-waiting
	require.NoError(t, e.Signal())
	require.NoError(t, <-result)
}

// TestEventSignalBeforeWaitIsKept is the property that makes the check-then-
// wait protocol safe: a wakeup that arrives early is not discarded.
func TestEventSignalBeforeWaitIsKept(t *testing.T) {
	e := newTestEvent(t)

	require.NoError(t, e.Signal())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, e.Wait(ctx))
}

func TestEventSignalNReleasesEachWaiter(t *testing.T) {
	e := newTestEvent(t)

	const waiters = 4
	result := make(chan error, waiters)
	ready := make(chan struct{}, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			ready <- struct{}{}
			result <- e.Wait(context.Background())
		}()
	}
	for i := 0; i < waiters; i++ {
		<-ready
	}

	require.NoError(t, e.SignalN(waiters))
	for i := 0; i < waiters; i++ {
		require.NoError(t, <-result)
	}
}

func TestEventWaitHonorsContext(t *testing.T) {
	e := newTestEvent(t)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- e.Wait(ctx) }()

	cancel()
	assert.ErrorIs(t, <-result, context.Canceled)
}

func TestEventWaitHonorsDeadline(t *testing.T) {
	e := newTestEvent(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, e.Wait(ctx), context.DeadlineExceeded)
}

func TestEventCloseReleasesWaiter(t *testing.T) {
	e, err := CreateEvent(uniqueName(t))
	require.NoError(t, err)
	defer e.Unlink()

	ready := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(ready)
		result <- e.Wait(context.Background())
	}()
	<-ready

	require.NoError(t, e.Close())
	assert.ErrorIs(t, <-result, ErrClosed)
}

func TestEventCloseWithoutWaiterIsClean(t *testing.T) {
	e, err := CreateEvent(uniqueName(t))
	require.NoError(t, err)
	defer e.Unlink()

	require.NoError(t, e.Close())
	assert.ErrorIs(t, e.Signal(), ErrClosed)
}

func TestOpenEventJoinsExistingName(t *testing.T) {
	name := uniqueName(t)
	creator, err := CreateEvent(name)
	require.NoError(t, err)
	defer func() {
		creator.Close()
		creator.Unlink()
	}()

	opener, err := OpenEvent(name)
	require.NoError(t, err)
	defer opener.Close()

	ready := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(ready)
		result <- opener.Wait(context.Background())
	}()
	<-ready

	require.NoError(t, creator.Signal())
	require.NoError(t, <-result)
}

func TestOpenEventRejectsMissingName(t *testing.T) {
	_, err := OpenEvent(uniqueName(t))
	assert.Error(t, err)
}

func TestEventRejectsInvalidNames(t *testing.T) {
	for _, name := range []string{"", "a/b", `a\b`, ".", ".."} {
		_, err := CreateEvent(name)
		assert.ErrorIs(t, err, ErrInvalidName, "name %q", name)
	}
}
