package ipc

import (
	"os"
	"path/filepath"
)

// runtimeDir holds the FIFOs that back named events. macOS has no /dev/shm.
func runtimeDir() string { return os.TempDir() }

func eventPath(name string) string {
	return filepath.Join(runtimeDir(), "go-ipc-"+name+".event")
}
