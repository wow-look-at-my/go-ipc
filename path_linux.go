//go:build !cosmo

package ipc

import "path/filepath"

// runtimeDir holds the FIFOs that back named events. On Linux /dev/shm is a
// tmpfs that every process can reach, which is where go-shm puts its segments
// too, so an endpoint's files stay together.
func runtimeDir() string { return "/dev/shm" }

func eventPath(name string) string {
	return filepath.Join(runtimeDir(), "go-ipc-"+name+".event")
}
