//go:build cosmo

package ipc

import (
	"errors"
	"runtime"
)

// One cosmo binary runs on Linux, macOS and Windows, and runtime.GOOS names the host it runs on.

var errNoExitWatch = errors.New("ipc: this host has no exit watch for a cosmo binary")

func startTime(pid int) (uint64, error) {
	if runtime.GOOS == "linux" {
		return procfsStartTime(pid)
	}
	return 0, nil
}

func procNS() (uint64, error) {
	if runtime.GOOS == "linux" {
		return procfsNS()
	}
	return nsUnknown, nil
}

func openExit(id procID) (exitWaiter, error) {
	if runtime.GOOS == "linux" {
		return procfsOpenExit(id)
	}
	return nil, errNoExitWatch
}
