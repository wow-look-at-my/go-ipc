// Package filelock is the OS file lock: flock on unix, LockFileEx on Windows.
// It imports no net, so a program that must not link net can take the lock.
package filelock

import (
	"context"
	"os"
)

// Lock takes an exclusive OS lock on the file at path, creating it if it does
// not exist, and returns the function that releases it. The wait blocks in
// the kernel. The kernel drops the lock when its holder exits, however it
// exits, so a lock never outlives the process that took it. The file itself
// stays in place; removing it would let a later opener lock a new inode while
// a waiter still blocks on the one.
//
// A ctx that ends returns the caller at once. The kernel wait it leaves
// behind drops the lock as soon as it is granted.
func Lock(ctx context.Context, path string) (unlock func(), err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}
	granted := make(chan error)
	abandoned := make(chan struct{})
	go func() {
		err := Take(f, true)
		select {
		case granted <- err:
		case <-abandoned:
			if err == nil {
				Release(f)
			}
			f.Close()
		}
	}()
	select {
	case err := <-granted:
		if err != nil {
			f.Close()
			return nil, &os.PathError{Op: "lock", Path: path, Err: err}
		}
		return releaser(f), nil
	case <-ctx.Done():
		close(abandoned)
		return nil, ctx.Err()
	}
}

// TryLock is Lock without the wait. A lock another holder has fails at once.
func TryLock(path string) (unlock func(), err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}
	if err := Take(f, false); err != nil {
		f.Close()
		return nil, &os.PathError{Op: "lock", Path: path, Err: err}
	}
	return releaser(f), nil
}

// releaser answers the function that lets go of f's lock and closes f.
func releaser(f *os.File) func() {
	return func() {
		Release(f)
		f.Close()
	}
}
