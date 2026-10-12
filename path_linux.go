//go:build !cosmo

package ipc

import "path/filepath"

// runtimeDir holds the FIFOs that back named events.
func runtimeDir() string { return "/dev/shm" }

func eventPath(name string) string {
	return filepath.Join(runtimeDir(), "go-ipc-"+name+".event")
}
