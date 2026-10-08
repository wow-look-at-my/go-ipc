package ipc

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lockWithin is LockFile bounded by wait.
func lockWithin(path string, wait time.Duration) (func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	return LockFile(ctx, path)
}

// A second holder blocks until the first lets go, then gets the lock.
func TestLockFileExcludesASecondHolder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.lock")
	first, err := lockWithin(path, 5*time.Second)
	require.NoError(t, err)

	got := make(chan func())
	go func() {
		unlock, err := lockWithin(path, 10*time.Second)
		assert.NoError(t, err)
		got <- unlock
	}()
	select {
	case <-got:
		t.Fatal("a second holder got the lock while the first held it")
	case <-time.After(200 * time.Millisecond):
	}

	first()
	unlock := <-got
	require.NotNil(t, unlock, "the second holder gets the lock once the first lets go")
	unlock()
}

// A ctx that ends returns the waiter at once, and the abandoned wait does not
// keep the lock once it is granted.
func TestLockFileCancelReturnsAtOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.lock")
	holder, err := lockWithin(path, 5*time.Second)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() {
		_, err := LockFile(ctx, path)
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled wait did not return")
	}

	holder()
	unlock, err := lockWithin(path, 5*time.Second)
	require.NoError(t, err, "the abandoned waiter let go of the lock it was granted")
	unlock()
}

// A holder that dies without unlocking releases the lock: the kernel drops it
// with the process.
func TestLockFileHolderDeathReleases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.lock")
	child, _ := startPeer(t, "lock-file", path)

	_, err := lockWithin(path, 200*time.Millisecond)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the child holds the lock")

	kill(t, child)
	unlock, err := lockWithin(path, 10*time.Second)
	require.NoError(t, err, "the dead child's lock is released")
	unlock()
}

// The non-waiting take fails at once on a held lock and succeeds on a free one.
func TestLockFileWithoutWaitFailsWhileHeld(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.lock")
	holder, err := lockWithin(path, 5*time.Second)
	require.NoError(t, err)

	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	defer f.Close()
	require.Error(t, lockFile(f, false), "a held lock refuses a take that does not wait")

	holder()
	require.NoError(t, lockFile(f, false))
	unlockFile(f)
}
