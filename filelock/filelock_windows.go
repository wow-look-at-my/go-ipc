package filelock

import (
	"os"
	"syscall"
	"unsafe"
)

// The calls go through syscall, because golang.org/x/sys/windows imports net.
var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = kernel32.NewProc("LockFileEx")
	procUnlockFileEx = kernel32.NewProc("UnlockFileEx")
)

const (
	lockfileFailImmediately = 0x1
	lockfileExclusiveLock   = 0x2
)

// allBytes is the length of the range locked: every byte the file can have.
const allBytes = ^uint32(0)

// Take takes f's exclusive LockFileEx lock. With wait it blocks until the
// lock is granted. Without it a held lock fails at once.
func Take(f *os.File, wait bool) error {
	flags := uint32(lockfileExclusiveLock)
	if !wait {
		flags |= lockfileFailImmediately
	}
	var overlapped syscall.Overlapped
	ok, _, err := procLockFileEx.Call(f.Fd(), uintptr(flags), 0, uintptr(allBytes), uintptr(allBytes), uintptr(unsafe.Pointer(&overlapped)))
	if ok == 0 {
		return err
	}
	return nil
}

// Release lets go of the lock Take took on f.
func Release(f *os.File) {
	var overlapped syscall.Overlapped
	procUnlockFileEx.Call(f.Fd(), 0, uintptr(allBytes), uintptr(allBytes), uintptr(unsafe.Pointer(&overlapped)))
}
