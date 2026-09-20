package ipc

import (
	"os"
	"path/filepath"
)

// runtimeDir holds the files that back named events. Windows has no /dev/shm,
// and its per-user temporary directory is reachable by every process of that
// user, which is the scope an event needs.
func runtimeDir() string { return os.TempDir() }

func eventPath(name string) string {
	return filepath.Join(runtimeDir(), "go-ipc-"+name+".event")
}
