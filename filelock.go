package ipc

import (
	"context"

	"github.com/wow-look-at-my/go-ipc/filelock"
)

// LockFile is filelock.Lock. A program that must not link net imports
// filelock directly.
func LockFile(ctx context.Context, path string) (unlock func(), err error) {
	return filelock.Lock(ctx, path)
}

// TryLockFile is filelock.TryLock.
func TryLockFile(path string) (unlock func(), err error) {
	return filelock.TryLock(path)
}
