//go:build unix

package ipc

import (
	"io/fs"
	"os"
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
