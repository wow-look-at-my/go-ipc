//go:build cosmo || darwin || linux || windows

package ipc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-shm"
)

// TestUnlinkOpenedToleratesTheOtherEnd unlinks a segment through a second
// handle after the first handle already removed it. That is the order a
// client's Close and the service's cleanup race into.
func TestUnlinkOpenedToleratesTheOtherEnd(t *testing.T) {
	name := uniqueName(t)
	first, err := shm.Create(name, 4096)
	require.NoError(t, err)
	defer first.Close()
	second, err := shm.Open(name)
	require.NoError(t, err)
	defer second.Close()

	require.NoError(t, first.Unlink())
	require.NoError(t, unlinkOpened(second), "the segment the other end removed is gone, which is not an error")
}
