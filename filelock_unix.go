//go:build unix

package ipc

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// lockFile takes f's exclusive flock. With wait it blocks in the kernel until
// the lock is granted. Without it a held lock fails at once with EWOULDBLOCK.
func lockFile(f *os.File, wait bool) error {
	how := unix.LOCK_EX
	if !wait {
		how |= unix.LOCK_NB
	}
	for {
		err := unix.Flock(int(f.Fd()), how)
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

func unlockFile(f *os.File) {
	unix.Flock(int(f.Fd()), unix.LOCK_UN)
}
