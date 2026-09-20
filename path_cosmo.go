package ipc

import (
	"os"
	"path/filepath"
)

// runtimeDir holds the FIFOs that back named events. One binary picks its host
// at run time, so this asks the file system rather than the build: /dev/shm is
// the tmpfs every process on Linux reaches, and a host without one has a
// per-user temporary directory of the same scope.
func runtimeDir() string {
	if info, err := os.Stat("/dev/shm"); err == nil && info.IsDir() {
		return "/dev/shm"
	}
	return os.TempDir()
}

func eventPath(name string) string {
	return filepath.Join(runtimeDir(), "go-ipc-"+name+".event")
}
