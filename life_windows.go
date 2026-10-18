package ipc

import (
	"errors"
	"net"

	"golang.org/x/sys/windows"
)

func lifeDir() string { return runtimeDir() }

// listenLife binds the socket at its final name. Windows has no sweep, so
// nothing can remove the socket before it listens.
func listenLife(id procID) (net.Listener, error) {
	return net.Listen("unix", lifePath(id))
}

func isRefused(err error) bool { return errors.Is(err, windows.WSAECONNREFUSED) }
