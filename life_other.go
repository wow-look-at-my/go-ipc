//go:build !unix && !windows

package ipc

import (
	"errors"
	"net"
	"os"
)

// This platform has no unix socket, so no process here can be watched.

func lifeDir() string { return os.TempDir() }

func listenLife(procID) (net.Listener, error) { return nil, errors.ErrUnsupported }

func isRefused(error) bool { return false }
