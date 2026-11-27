//go:build cosmo || darwin || linux || windows

package ipc

import (
	"context"
	"io"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func listenTest(t *testing.T, name string) *Listener {
	t.Helper()
	l, err := ListenStream(name, WithCapacity(MinCapacity))
	require.NoError(t, err)
	t.Cleanup(func() { l.Close() })
	return l
}

func dialTest(t *testing.T, name string) *Conn {
	t.Helper()
	c, err := DialStream(contextWithTimeout(t), name, WithCapacity(MinCapacity))
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

type accepted struct {
	conn net.Conn
	err  error
}

// acceptAsync runs one Accept and delivers its result.
func acceptAsync(l *Listener) <-chan accepted {
	out := make(chan accepted, 1)
	go func() {
		conn, err := l.Accept()
		out <- accepted{conn, err}
	}()
	return out
}

func acceptTest(t *testing.T, l *Listener) net.Conn {
	t.Helper()
	select {
	case a := <-acceptAsync(l):
		require.NoError(t, a.err)
		t.Cleanup(func() { a.conn.Close() })
		return a.conn
	case <-time.After(10 * time.Second):
		require.FailNow(t, "no dialer was accepted")
		return nil
	}
}

// say writes text on one end and reads it on the other.
func say(t *testing.T, from, to net.Conn, text string) {
	t.Helper()
	_, err := from.Write([]byte(text))
	require.NoError(t, err)
	got := make([]byte, len(text))
	_, err = io.ReadFull(to, got)
	require.NoError(t, err)
	assert.Equal(t, text, string(got))
}

func TestListenerGivesEachDialerAStream(t *testing.T) {
	name := uniqueName(t)
	l := listenTest(t, name)
	assert.Equal(t, name, l.Addr().String())

	// Every dial returns before any Accept runs.
	dialers := make([]*Conn, 3)
	for i := range dialers {
		dialers[i] = dialTest(t, name)
		_, err := dialers[i].Write([]byte(strconv.Itoa(i)))
		require.NoError(t, err)
	}
	for range dialers {
		server := acceptTest(t, l)
		first := make([]byte, 1)
		_, err := io.ReadFull(server, first)
		require.NoError(t, err)
		i, err := strconv.Atoi(string(first))
		require.NoError(t, err)
		say(t, server, dialers[i], "reply to "+string(first))
		say(t, dialers[i], server, "more from "+string(first))
	}
}

func TestListenerAcceptsADialerThatCameFirst(t *testing.T) {
	name := uniqueName(t)
	dialed := make(chan *Conn, 1)
	ctx := contextWithTimeout(t)
	go func() {
		c, err := DialStream(ctx, name, WithCapacity(MinCapacity))
		if err != nil {
			c = nil
		}
		dialed <- c
	}()
	// The dialer must have its channel in place before the listener scans.
	require.Eventually(t, func() bool { return len(scanChannels(name, listenerClientPrefix)) == 1 }, 10*time.Second, time.Millisecond)

	l := listenTest(t, name)
	server := acceptTest(t, l)
	client := <-dialed
	require.NotNil(t, client)
	t.Cleanup(func() { client.Close() })
	say(t, client, server, "early")
}

func TestListenerRefusesASecondListener(t *testing.T) {
	name := uniqueName(t)
	listenTest(t, name)
	_, err := ListenStream(name, WithCapacity(MinCapacity))
	require.ErrorIs(t, err, ErrInUse)
}

func TestListenerRejectsAnEmptyName(t *testing.T) {
	_, err := ListenStream("")
	require.ErrorIs(t, err, ErrInvalidName)
	_, err = DialStream(context.Background(), "")
	require.ErrorIs(t, err, ErrInvalidName)
}

func TestListenerCloseEndsAccept(t *testing.T) {
	name := uniqueName(t)
	l := listenTest(t, name)
	client := dialTest(t, name)
	server := acceptTest(t, l)

	waiting := acceptAsync(l)
	require.NoError(t, l.Close())
	select {
	case a := <-waiting:
		require.ErrorIs(t, a.err, net.ErrClosed)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "Close left an Accept parked")
	}
	_, err := l.Accept()
	require.ErrorIs(t, err, net.ErrClosed)
	require.NoError(t, l.Close(), "a second Close finds nothing to do")

	say(t, client, server, "an accepted stream outlives its listener")
}

func TestListenerCloseEndsAStreamNobodyAccepted(t *testing.T) {
	name := uniqueName(t)
	l := listenTest(t, name)
	client := dialTest(t, name)
	require.NoError(t, l.Close())
	_, err := client.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
}

func TestDialStreamEndsWithItsContext(t *testing.T) {
	name := uniqueName(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := DialStream(ctx, name, WithCapacity(MinCapacity))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Empty(t, scanChannels(name, listenerClientPrefix), "the dial left its channel behind")
}

func TestStreamCloseRemovesItsNames(t *testing.T) {
	name := uniqueName(t)
	l := listenTest(t, name)
	client := dialTest(t, name)
	server := acceptTest(t, l)
	say(t, client, server, "bye")
	require.Len(t, scanChannels(name, listenerClientPrefix), 1)

	require.NoError(t, client.Close())
	_, err := server.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, server.Close())
	assert.Empty(t, scanChannels(name, listenerClientPrefix))
}

// TestAcceptedStreamRemovesTheNamesOfADeadDialer kills a dialer that never
// closed. The accepting side finds it gone and removes what it left.
func TestAcceptedStreamRemovesTheNamesOfADeadDialer(t *testing.T) {
	name := uniqueName(t)
	l := listenTest(t, name)
	child, _ := startPeer(t, "stream-dialer", name)
	server := acceptTest(t, l)
	greeting := make([]byte, len("hello"))
	_, err := io.ReadFull(server, greeting)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(greeting))

	kill(t, child)
	require.NoError(t, server.SetReadDeadline(time.Now().Add(10*time.Second)))
	_, err = server.Read(make([]byte, 1))
	require.ErrorIs(t, err, ErrPeerGone)
	require.Len(t, scanChannels(name, listenerClientPrefix), 1)
	require.NoError(t, server.Close())
	assert.Empty(t, scanChannels(name, listenerClientPrefix))
}

// childStreamDialer dials a listener, writes a greeting, and waits to be killed.
func childStreamDialer(ctx context.Context, name string) error {
	c, err := DialStream(ctx, name, WithCapacity(MinCapacity))
	if err != nil {
		return err
	}
	if _, err := c.Write([]byte("hello")); err != nil {
		return err
	}
	if err := readyThenWait(); err != nil {
		return err
	}
	os.Exit(0)
	return nil
}
