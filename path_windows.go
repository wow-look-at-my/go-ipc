package ipc

import (
	"os"
	"path/filepath"
	"sync"
)

// runtimeDir holds every file of this package: events, names and life
// sockets. It is not TMPDIR, because a process that exits without Release
// leaves its life socket behind, and a test harness that wants an empty
// TMPDIR at exit then fails on it.
func runtimeDir() string {
	dirOnce.Do(func() {
		base, err := os.UserCacheDir()
		if err != nil {
			dir = os.TempDir()
			return
		}
		dir = filepath.Join(base, "gosmopolitan", "go-ipc")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			dir = os.TempDir()
		}
	})
	return dir
}

var (
	dirOnce sync.Once
	dir     string
)

func eventPath(name string) string {
	return filepath.Join(runtimeDir(), "go-ipc-"+name+".event")
}
