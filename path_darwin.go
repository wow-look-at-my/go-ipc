package ipc

import (
	"os"
	"path/filepath"
)

// runtimeDir holds the FIFOs that back named events. macOS has no /dev/shm,
// and its per-user temporary directory is shared by every process of that
// user, which is the scope an event needs.
func runtimeDir() string { return os.TempDir() }

func eventPath(name string) string {
	return filepath.Join(runtimeDir(), "go-ipc-"+name+".event")
}
