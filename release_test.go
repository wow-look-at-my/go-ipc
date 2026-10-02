package ipc

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// childRelease prints its procID, releases its life socket twice on the first
// line of stdin, prints "released", and exits when stdin closes. The test
// process itself must never release, because its other tests need its socket.
func childRelease() error {
	if err := Release(); err != nil {
		return fmt.Errorf("release before any procID: %w", err)
	}
	if self.live.Load() {
		return errors.New("release made a procID")
	}
	if _, err := fmt.Println(uint64(selfID())); err != nil {
		return err
	}
	in := bufio.NewReader(os.Stdin)
	if _, err := in.ReadString('\n'); err != nil {
		return err
	}
	if err := Release(); err != nil {
		return err
	}
	if err := Release(); err != nil {
		return fmt.Errorf("second release: %w", err)
	}
	if _, err := fmt.Println("released"); err != nil {
		return err
	}
	_, err := io.Copy(io.Discard, in)
	return err
}

func TestReleaseRemovesLifeSocket(t *testing.T) {
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), childRoleEnv+"=release")
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	out := bufio.NewReader(stdout)
	line, err := out.ReadString('\n')
	require.NoError(t, err)
	raw, err := strconv.ParseUint(strings.TrimSpace(line), 10, 64)
	require.NoError(t, err)
	id := procID(raw)
	require.True(t, id.watchable(), "the child has no life socket")

	// Both watches start before the release.
	conn, err := net.Dial("unix", lifePath(id))
	require.NoError(t, err)
	defer conn.Close()
	fired := make(chan error, 1)
	cancel, err := onExit(id, func(err error) { fired <- err })
	require.NoError(t, err)
	defer cancel()

	_, err = io.WriteString(stdin, "release\n")
	require.NoError(t, err)
	line, err = out.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "released\n", line)

	_, err = os.Stat(lifePath(id))
	assert.ErrorIs(t, err, fs.ErrNotExist, "the life socket file is still there after release")
	assert.True(t, isDead(id), "a check after release must judge the process gone")

	// The child still lives, so the earlier connection stays open.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	_, err = conn.Read(make([]byte, 1))
	assert.ErrorIs(t, err, os.ErrDeadlineExceeded, "release ended a connection that a peer held")
	select {
	case err := <-fired:
		t.Fatalf("the watch fired at release, not at exit: %v", err)
	default:
	}

	// The exit ends the connection and fires the watch.
	require.NoError(t, stdin.Close())
	require.NoError(t, cmd.Wait())
	require.NoError(t, conn.SetReadDeadline(time.Time{}))
	_, err = conn.Read(make([]byte, 1))
	assert.True(t, errors.Is(err, io.EOF), "read after the exit: %v", err)
	assert.NoError(t, <-fired)
}
