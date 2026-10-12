package ipc

import (
	"os"
	"path/filepath"
)

// runtimeDir holds the files that back named events.
func runtimeDir() string { return os.TempDir() }

func eventPath(name string) string {
	return filepath.Join(runtimeDir(), "go-ipc-"+name+".event")
}
