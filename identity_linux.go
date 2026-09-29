//go:build !cosmo

package ipc

func startTime(pid int) (uint64, error) { return procfsStartTime(pid) }

func procNS() (uint64, error) { return procfsNS() }

func openExit(id procID) (exitWaiter, error) { return procfsOpenExit(id) }
