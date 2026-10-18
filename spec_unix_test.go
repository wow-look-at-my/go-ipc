//go:build unix

package ipc

import (
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInstanceFileMatchesSpec(t *testing.T) {
	s := loadWire(t)
	assert.Equal(t, specPath(t, s, s.Paths.InstanceFile, map[string]string{"name": "q"}), incPath("q"))
}

// The spec's absolute paths hold where the runtime directory is the spec's.
// Elsewhere TestPathsMatchSpec covers the names.
func TestQueueFilesMatchSpec(t *testing.T) {
	s := loadWire(t)
	if runtimeDir() != s.Paths.RuntimeDir {
		t.Skipf("the runtime directory here is %s, not %s", runtimeDir(), s.Paths.RuntimeDir)
	}
	mode, err := strconv.ParseUint(s.Paths.FileMode, 8, 32)
	require.NoError(t, err)

	name := uniqueName(t)
	q := newReceiver(t, name, WithCapacity(MinCapacity))
	vars := map[string]string{"name": name, "id": q.inc}

	content, err := os.ReadFile(specPath(t, s, s.Paths.InstanceFile, vars))
	require.NoError(t, err)
	assert.Equal(t, q.inc, string(content))
	assert.Len(t, content, s.Paths.InstanceIDDigits)
	_, err = hex.DecodeString(string(content))
	assert.NoError(t, err)

	check := func(pattern string, kind fs.FileMode, size int64) {
		t.Helper()
		info, err := os.Lstat(specPath(t, s, pattern, vars))
		require.NoError(t, err, pattern)
		assert.Equal(t, kind, info.Mode().Type(), pattern)
		assert.Equal(t, fs.FileMode(mode), info.Mode().Perm(), pattern)
		if size >= 0 {
			assert.Equal(t, size, info.Size(), pattern)
		}
	}
	check(s.Paths.NameFile, 0, -1)
	check(s.Paths.InstanceFile, 0, int64(s.Paths.InstanceIDDigits))
	check(s.Paths.Segment, 0, int64(s.Ring.HeaderSize+MinCapacity))
	if !sockHost() {
		check(s.Paths.NotEmptyEvent, fs.ModeNamedPipe, -1)
		check(s.Paths.NotFullEvent, fs.ModeNamedPipe, -1)
	}
	require.NoError(t, selfErr())
	assert.Equal(t, uint64(q.self), q.ring.hdr.consumer.Load())
	vars["procid"] = fmt.Sprintf("%016x", uint64(q.self))
	check(s.Paths.LifeSocket, fs.ModeSocket, -1)
}
