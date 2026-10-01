//go:build unix

package ipc

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-shm"
)

func TestSweepRemovesNameOfDeadCreator(t *testing.T) {
	name := uniqueName(t)
	consumer, _ := startPeer(t, "consumer", name)
	inc, err := readName(name)
	require.NoError(t, err)

	sweepName(name)
	_, err = readName(name)
	require.NoError(t, err, "the sweep removed a name its live holder has")

	kill(t, consumer)
	sweepDir(runtimeDir())

	_, err = readName(name)
	assert.ErrorIs(t, err, fs.ErrNotExist)
	_, err = shm.Open(instanceName(name, inc))
	assert.Error(t, err, "the segment outlived the sweep")
	_, err = os.Stat(eventPath(instanceName(name, inc) + ".ne"))
	assert.ErrorIs(t, err, fs.ErrNotExist)
}

func TestSweepRemovesLifeSocketOfDeadProcess(t *testing.T) {
	live, liveID, _ := startIdent(t)
	dead := deadProcID(t)
	require.True(t, listed(t, lifePath(dead)), "a process leaves its life socket behind")

	sweepDir(lifeDir())
	assert.False(t, listed(t, lifePath(dead)))
	assert.True(t, listed(t, lifePath(liveID)), "the sweep removed the life socket of a live process")
	kill(t, live)
}

// listed reads the directory, because a stat of a socket fails with EIO on a Windows host.
func listed(t *testing.T, path string) bool {
	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.Name() == filepath.Base(path) {
			return true
		}
	}
	return false
}
