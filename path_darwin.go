package ipc

import (
	"os"
	"path/filepath"
	"sync"
)

// runtimeDir holds the FIFOs that back named events, and the sockets that
// track live processes. macOS has no /dev/shm, and the go command's tests
// fail on any file left in TMPDIR, so the tree gets a directory of its own
// under the user cache instead of the shared temporary directory.
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
