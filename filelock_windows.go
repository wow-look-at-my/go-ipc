package ipc

import (
	"os"

	"golang.org/x/sys/windows"
)

// allBytes is the length of the range locked: every byte the file can have.
const allBytes = ^uint32(0)

// lockFile takes f's exclusive LockFileEx lock. With wait it blocks until the
// lock is granted. Without it a held lock fails at once.
func lockFile(f *os.File, wait bool) error {
	flags := uint32(windows.LOCKFILE_EXCLUSIVE_LOCK)
	if !wait {
		flags |= windows.LOCKFILE_FAIL_IMMEDIATELY
	}
	var overlapped windows.Overlapped
	return windows.LockFileEx(windows.Handle(f.Fd()), flags, 0, allBytes, allBytes, &overlapped)
}

func unlockFile(f *os.File) {
	var overlapped windows.Overlapped
	windows.UnlockFileEx(windows.Handle(f.Fd()), 0, allBytes, allBytes, &overlapped)
}
