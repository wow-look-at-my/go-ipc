package ipc

import (
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestConnPair returns the two ends of a connection and removes the name
// afterwards. Both live in this process, which the stream layer allows: the
// queues underneath carry the direction.
func newTestConnPair(t *testing.T, opts ...Option) (*Conn, *Conn) {
	t.Helper()
	name := uniqueName(t)

	server, err := Listen(name, opts...)
	require.NoError(t, err)
	t.Cleanup(func() {
		server.Close()
		server.Unlink()
	})

	client, err := Dial(name)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	return server, client
}

func TestConnRoundTrip(t *testing.T) {
	server, client := newTestConnPair(t)

	require.NoError(t, client.SetDeadline(time.Now().Add(10*time.Second)))
	require.NoError(t, server.SetDeadline(time.Now().Add(10*time.Second)))

	_, err := client.Write([]byte("hello stream"))
	require.NoError(t, err)

	got := make([]byte, len("hello stream"))
	_, err = io.ReadFull(server, got)
	require.NoError(t, err)
	assert.Equal(t, "hello stream", string(got))
}

// TestConnSplitsAcrossMessages pushes more than one message worth of bytes so
// the write splits and the read reassembles.
func TestConnSplitsAcrossMessages(t *testing.T) {
	server, client := newTestConnPair(t, WithCapacity(MinCapacity))
	require.NoError(t, client.SetDeadline(time.Now().Add(30*time.Second)))
	require.NoError(t, server.SetDeadline(time.Now().Add(30*time.Second)))

	payload := make([]byte, 8*client.Channel().MaxMessageSize())
	for i := range payload {
		payload[i] = byte(i)
	}

	written := make(chan error, 1)
	go func() {
		_, werr := client.Write(payload)
		written <- werr
	}()

	got := make([]byte, len(payload))
	_, err := io.ReadFull(server, got)
	require.NoError(t, err)
	require.NoError(t, <-written)
	assert.Equal(t, payload, got)
}

// TestConnReadDeadline covers the deadline path: a read with nothing to
// deliver reports the error net.Conn callers check for.
func TestConnReadDeadline(t *testing.T) {
	server, _ := newTestConnPair(t)

	require.NoError(t, server.SetReadDeadline(time.Now().Add(20*time.Millisecond)))

	_, err := server.Read(make([]byte, 8))
	assert.ErrorIs(t, err, os.ErrDeadlineExceeded)
}

// TestConnWriteDeadline fills the channel so the next write must park, then
// lets the deadline end it.
func TestConnWriteDeadline(t *testing.T) {
	_, client := newTestConnPair(t, WithCapacity(MinCapacity))

	// Nobody reads the other end, so the channel stays full once filled.
	require.NoError(t, client.SetWriteDeadline(time.Now().Add(20*time.Millisecond)))

	payload := make([]byte, client.Channel().MaxMessageSize())
	_, err := client.Write(payload)
	for i := 0; err == nil && i < 64; i++ {
		_, err = client.Write(payload)
	}
	assert.ErrorIs(t, err, os.ErrDeadlineExceeded)
}

// TestConnCloseReportsNetErrClosed checks the mapping onto the error a
// net.Conn caller expects, rather than the package's own ErrClosed.
func TestConnCloseReportsNetErrClosed(t *testing.T) {
	server, _ := newTestConnPair(t)
	require.NoError(t, server.Close())

	_, err := server.Read(make([]byte, 8))
	assert.ErrorIs(t, err, net.ErrClosed)

	_, err = server.Write([]byte("gone"))
	assert.ErrorIs(t, err, net.ErrClosed)
}

// TestConnCloseReleasesBlockedRead proves Close ends a read that is already
// parked, rather than leaving the goroutine there.
func TestConnCloseReleasesBlockedRead(t *testing.T) {
	server, _ := newTestConnPair(t)

	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		_, err := server.Read(make([]byte, 8))
		result <- err
	}()
	<-started

	require.NoError(t, server.Close())

	select {
	case err := <-result:
		assert.ErrorIs(t, err, net.ErrClosed)
	case <-time.After(10 * time.Second):
		t.Fatal("Close left a reader parked")
	}
}

func TestConnPeerCloseIsEOF(t *testing.T) {
	server, client := newTestConnPair(t)

	require.NoError(t, client.Close())
	require.NoError(t, server.SetReadDeadline(time.Now().Add(10*time.Second)))

	_, err := server.Read(make([]byte, 8))
	assert.ErrorIs(t, err, io.EOF)
}

func TestConnAddr(t *testing.T) {
	server, _ := newTestConnPair(t)
	assert.Equal(t, "ipc", server.LocalAddr().Network())
	assert.Equal(t, server.LocalAddr().String(), server.RemoteAddr().String())
	assert.NotEmpty(t, server.LocalAddr().String())
}

func TestDialRejectsMissingName(t *testing.T) {
	_, err := Dial(uniqueName(t))
	assert.Error(t, err)
}

// TestNewConnAdoptsChannel covers the constructor that wraps a channel a
// caller already holds.
func TestNewConnAdoptsChannel(t *testing.T) {
	name := uniqueName(t)

	serverCh, err := CreateChannel(name)
	require.NoError(t, err)
	defer func() {
		serverCh.Close()
		serverCh.Unlink()
	}()

	clientCh, err := OpenChannel(name)
	require.NoError(t, err)

	server := NewConn(serverCh)
	client := NewConn(clientCh)
	defer client.Close()

	require.NoError(t, server.SetDeadline(time.Now().Add(10*time.Second)))
	_, err = client.Write([]byte("adopted"))
	require.NoError(t, err)

	got := make([]byte, len("adopted"))
	_, err = io.ReadFull(server, got)
	require.NoError(t, err)
	assert.Equal(t, "adopted", string(got))
}

// TestConnDeadlineIsNotFatal checks that a read which timed out leaves the
// connection usable, the way a net.Conn does.
func TestConnDeadlineIsNotFatal(t *testing.T) {
	server, client := newTestConnPair(t)

	require.NoError(t, server.SetReadDeadline(time.Now().Add(20*time.Millisecond)))
	_, err := server.Read(make([]byte, 8))
	require.True(t, errors.Is(err, os.ErrDeadlineExceeded))

	require.NoError(t, client.SetWriteDeadline(time.Now().Add(10*time.Second)))
	_, err = client.Write([]byte("after"))
	require.NoError(t, err)

	require.NoError(t, server.SetReadDeadline(time.Now().Add(10*time.Second)))
	got := make([]byte, len("after"))
	_, err = io.ReadFull(server, got)
	require.NoError(t, err)
	assert.Equal(t, "after", string(got))
}
