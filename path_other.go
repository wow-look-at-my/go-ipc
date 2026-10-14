//go:build !linux && !darwin && !windows && !cosmo

package ipc

import (
	"os"
	"path/filepath"
)

// runtimeDir holds the files of named objects.
func runtimeDir() string { return os.TempDir() }

func eventPath(name string) string {
	return filepath.Join(runtimeDir(), "go-ipc-"+name+".event")
}
