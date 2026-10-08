package ipc

import (
	"context"
	"os"
)

// LockFile takes an exclusive OS lock on the file at path, creating it if it
// does not exist, and returns the function that releases it. The wait blocks
// lock when its holder exits, however it exits, so a lock never outlives the
// process that took it. The file itself stays in place; removing it would let
// a later opener lock a new inode while a waiter still blocks on the one.
//
// A ctx that ends returns the caller at once. The kernel wait it leaves
// behind drops the lock as soon as it is granted.
func LockFile(ctx context.Context, path string) (unlock func(), err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}
	granted := make(chan error)
	abandoned := make(chan struct{})
	go func() {
		err := lockFile(f, true)
		select {
		case granted <- err:
		case <-abandoned:
			if err == nil {
				unlockFile(f)
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
		return func() {
			unlockFile(f)
			f.Close()
		}, nil
	case <-ctx.Done():
		close(abandoned)
		return nil, ctx.Err()
	}
}
