package ipc

import (
	"os"
	"path/filepath"
	"sync"
)

// runtimeDir holds the name files and the sockets that track live processes.
// A life socket outlives its process on Windows, and a sweep removes it. It
// is a per-user directory, so it stays out of the TEMP of the program.
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
