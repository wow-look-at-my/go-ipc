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

// TestUnlinkLeavesTheNameASuccessorHolds closes a queue, lets a successor
// take the name, and unlinks the closed queue before the successor has
// published its instance. The name file must stay the one the successor
// locked.
func TestUnlinkLeavesTheNameASuccessorHolds(t *testing.T) {
	name := uniqueName(t)
	q, err := CreateQueue(name, WithCapacity(MinCapacity))
	require.NoError(t, err)
	require.NoError(t, q.Close())

	successor, err := lockName(name)
	require.NoError(t, err)
	t.Cleanup(func() {
		successor.release()
		removeName(name)
	})
	locked, err := successor.f.Stat()
	require.NoError(t, err)

	require.NoError(t, q.Unlink())
	now, err := os.Stat(namePath(name))
	require.NoError(t, err, "the unlink removed the name file its successor holds")
	assert.True(t, os.SameFile(locked, now))
}

// TestUnlinkRemovesTheNameNobodyHolds is the same sequence with no successor.
func TestUnlinkRemovesTheNameNobodyHolds(t *testing.T) {
	name := uniqueName(t)
	q, err := CreateQueue(name, WithCapacity(MinCapacity))
	require.NoError(t, err)
	require.NoError(t, q.Close())
	require.NoError(t, q.Unlink())
	_, err = os.Stat(namePath(name))
	assert.ErrorIs(t, err, fs.ErrNotExist)
	require.NoError(t, q.Unlink(), "a second unlink finds nothing to do")

	held, err := CreateQueue(name, WithCapacity(MinCapacity))
	require.NoError(t, err)
	require.NoError(t, held.Unlink(), "a holder unlinks under its own lock")
	_, err = os.Stat(namePath(name))
	assert.ErrorIs(t, err, fs.ErrNotExist)
	require.NoError(t, held.Close())
}

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

func TestUnlinkSegmentAcceptsAnInstanceAnotherProcessRemoved(t *testing.T) {
	inst := instanceName(uniqueName(t), "0123456789abcdef")
	created, err := shm.Create(inst, 4096)
	require.NoError(t, err)
	defer created.Close()
	opened, err := shm.Open(inst)
	require.NoError(t, err)
	defer opened.Close()

	require.NoError(t, unlinkSegment(created))
	require.NoError(t, unlinkSegment(opened), "the segment was already gone")
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
