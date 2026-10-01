//go:build unix

package ipc

import (
	"errors"
	"net"
	"os"
	"syscall"
)

func lifeDir() string { return runtimeDir() }

// listenLife binds the socket under a temporary name, then renames it into
// place. A bound socket refuses a dial until it listens, and the sweep
// removes a socket that refuses. The sweep never touches a temporary name.
func listenLife(id procID) (net.Listener, error) {
	path := lifePath(id)
	tmp := path + ".tmp"
	ln, err := net.Listen("unix", tmp)
	if err != nil {
		return nil, err
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	// Only the owner may connect, as with every other file of this package.
	if err := os.Chmod(tmp, 0o600); err != nil {
		ln.Close()
		os.Remove(tmp)
		return nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		ln.Close()
		os.Remove(tmp)
		return nil, err
	}
	return ln, nil
}

func isRefused(err error) bool { return errors.Is(err, syscall.ECONNREFUSED) }
