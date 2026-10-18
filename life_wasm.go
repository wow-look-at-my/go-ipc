//go:build js || wasip1

package ipc

import (
	"errors"
	"net"
	"syscall"
)

// The net package of both ports is a network inside this process. A life
// socket there makes no file, and a dial to a name nobody listens on is refused.

func lifeDir() string { return runtimeDir() }

func listenLife(id procID) (net.Listener, error) {
	return net.Listen("unix", lifePath(id))
}

func isRefused(err error) bool { return errors.Is(err, syscall.ECONNREFUSED) }
