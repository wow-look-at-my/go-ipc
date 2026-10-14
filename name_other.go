//go:build !unix && !windows

package ipc

import (
	"errors"
	"path/filepath"
)

// This platform has no file lock, so no process here can hold a name.

// nameOutlivesHolder is true, as on unix: nothing removes the name with its holder.
const nameOutlivesHolder = true

// supported is false: this platform has no shared memory, no file lock, no FIFO and no unix socket.
const supported = false

func namePath(name string) string {
	return filepath.Join(runtimeDir(), "go-ipc-"+name+".name")
}

type nameLock struct{}

func lockName(string) (*nameLock, error) { return nil, errors.ErrUnsupported }

func (*nameLock) previous() string { return "" }

func (*nameLock) publish(string) error { return errors.ErrUnsupported }

func (*nameLock) release() error { return nil }

func (*nameLock) unlink() error { return nil }

func readName(string) (string, error) { return "", errors.ErrUnsupported }

func unlinkName(string, string) error { return errors.ErrUnsupported }

// sweepStale has nothing to do: no instance can exist here.
func sweepStale() {}
