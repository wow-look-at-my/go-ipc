//go:build cosmo || darwin || linux || windows

package ipc

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// filesOf lists every file of the runtime directory that belongs to name. Its
// name files, its segments and its events, with those of the endpoints under
// it.
func filesOf(t testing.TB, name string) []string {
	t.Helper()
	entries, err := os.ReadDir(runtimeDir())
	require.NoError(t, err)
	var found []string
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "-"+name+".") {
			found = append(found, entry.Name())
		}
	}
	return found
}

// TestTeardownLeavesNoFile ends a connection in each way it can end. A name
// file is not all an endpoint makes. A segment and events stand beside it,
// and a test that reads only the names does not see them.
func TestTeardownLeavesNoFile(t *testing.T) {
	t.Run("a queue", func(t *testing.T) {
		name := uniqueName(t)
		q, err := CreateQueue(name, WithCapacity(MinCapacity))
		require.NoError(t, err)
		sender, err := OpenQueue(name)
		require.NoError(t, err)
		require.NotEmpty(t, filesOf(t, name))
		require.NoError(t, sender.Close())
		require.NoError(t, q.Close())
		require.NoError(t, q.Unlink())
		assert.Empty(t, filesOf(t, name))
	})

	t.Run("a service client that closes", func(t *testing.T) {
		name := uniqueName(t)
		svc, _ := serveTest(t, name)
		c := connectTest(t, name)
		_, _, err := c.Call(contextWithTimeout(t), testEcho, []byte("x"))
		require.NoError(t, err)
		require.NoError(t, c.Close())
		require.NoError(t, svc.Close())
		assert.Empty(t, filesOf(t, name))
	})

	t.Run("a service that closes first", func(t *testing.T) {
		name := uniqueName(t)
		svc, _ := serveTest(t, name)
		c := connectTest(t, name)
		require.NoError(t, svc.Close())
		require.NoError(t, c.Close())
		assert.Empty(t, filesOf(t, name))
	})

	t.Run("a service client that dies", func(t *testing.T) {
		name := uniqueName(t)
		svc, h := serveTest(t, name)
		child, _ := startPeer(t, "service-client", name)
		kill(t, child)
		select {
		case <-h.goneCh:
		case <-time.After(10 * time.Second):
			require.FailNow(t, "the handler was not told that the client died")
		}
		require.NoError(t, svc.Close())
		assert.Empty(t, filesOf(t, name))
	})

	t.Run("a connect that gives up", func(t *testing.T) {
		name := uniqueName(t)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := Connect(ctx, name, WithCapacity(MinCapacity))
		require.Error(t, err)
		_, err = DialStream(ctx, name, WithCapacity(MinCapacity))
		require.Error(t, err)
		assert.Empty(t, filesOf(t, name))
	})

	t.Run("a stream that both sides close", func(t *testing.T) {
		name := uniqueName(t)
		l := listenTest(t, name)
		client := dialTest(t, name)
		server := acceptTest(t, l)
		say(t, client, server, "bye")
		require.NoError(t, client.Close())
		_, err := server.Read(make([]byte, 1))
		require.ErrorIs(t, err, io.EOF)
		require.NoError(t, server.Close())
		require.NoError(t, l.Close())
		assert.Empty(t, filesOf(t, name))
	})

	t.Run("a stream nobody accepted", func(t *testing.T) {
		name := uniqueName(t)
		l := listenTest(t, name)
		client := dialTest(t, name)
		require.NoError(t, l.Close())
		require.NoError(t, client.Close())
		assert.Empty(t, filesOf(t, name))
	})

	t.Run("a stream dialer that dies", func(t *testing.T) {
		name := uniqueName(t)
		l := listenTest(t, name)
		child, _ := startPeer(t, "stream-dialer", name)
		server := acceptTest(t, l)
		kill(t, child)
		require.NoError(t, server.SetReadDeadline(time.Now().Add(10*time.Second)))
		_, err := io.ReadAll(server)
		require.ErrorIs(t, err, ErrPeerGone)
		require.NoError(t, server.Close())
		require.NoError(t, l.Close())
		assert.Empty(t, filesOf(t, name))
	})
}
