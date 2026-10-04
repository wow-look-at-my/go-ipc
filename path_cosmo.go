//go:build cosmo

package ipc

import (
	"os"
	"path/filepath"
)

// runtimeDir holds the FIFOs that back named events.
func runtimeDir() string {
	if info, err := os.Stat("/dev/shm"); err == nil && info.IsDir() {
		return "/dev/shm"
	}
	return os.TempDir()
}

func eventPath(name string) string {
	return filepath.Join(runtimeDir(), "go-ipc-"+name+".event")
}
