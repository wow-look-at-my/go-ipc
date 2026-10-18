//go:build js || wasip1

package ipc

import (
	"os"
	"path/filepath"
)

// No file backs a name or an event on WebAssembly. These paths name the files
// that wire.json gives them, and lifePath names a socket of the network
// inside this process.

func runtimeDir() string { return os.TempDir() }

func namePath(name string) string {
	return filepath.Join(runtimeDir(), "go-ipc-"+name+".name")
}

func eventPath(name string) string {
	return filepath.Join(runtimeDir(), "go-ipc-"+name+".event")
}
