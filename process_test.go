package ipc

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Environment variables that turn this test binary into a peer process.
const (
	childRoleEnv  = "GO_IPC_TEST_ROLE"
	childNameEnv  = "GO_IPC_TEST_NAME"
	childCountEnv = "GO_IPC_TEST_COUNT"
)

// TestMain turns the test binary into the second process when the role
// variable is set. Re-executing the binary is how these tests reach a real
// separate address space, which is the only place the shared memory claims
// mean anything.
func TestMain(m *testing.M) {
	if role := os.Getenv(childRoleEnv); role != "" {
		if err := runChild(role, os.Getenv(childNameEnv), os.Getenv(childCountEnv)); err != nil {
			fmt.Fprintf(os.Stderr, "child %s: %v\n", role, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runChild(role, name, count string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	switch role {
	case "sender":
		n, err := strconv.Atoi(count)
		if err != nil {
			return err
		}
		return childSender(ctx, name, n)
	case "echo":
		return childEcho(ctx, name)
	default:
		return fmt.Errorf("unknown role %q", role)
	}
}

// childSender writes a numbered sequence the parent checks for order and for
// loss.
func childSender(ctx context.Context, name string, count int) error {
	q, err := OpenQueue(name)
	if err != nil {
		return err
	}
	defer q.Close()

	for i := 0; i < count; i++ {
		if err := q.SendTyped(ctx, uint32(i), []byte(strconv.Itoa(i))); err != nil {
			return fmt.Errorf("send %d: %w", i, err)
		}
	}
	return nil
}

// childEcho copies the stream back until the parent closes it.
func childEcho(ctx context.Context, name string) error {
	conn, err := Dial(name)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := io.Copy(conn, conn); err != nil {
		return err
	}
	return nil
}

// startChild re-executes this binary in the given role.
func startChild(t *testing.T, role, name string, count int) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		childRoleEnv+"="+role,
		childNameEnv+"="+name,
		childCountEnv+"="+strconv.Itoa(count),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	})
	return cmd
}

// TestCrossProcessQueue proves the whole point of the package: a second
// process attaches by name, and its messages arrive in order.
//
// The parent blocks in Recv before the child has even started, so the first
// message also exercises a wakeup delivered from another process.
func TestCrossProcessQueue(t *testing.T) {
	const count = 5000

	name := uniqueName(t)
	q, err := CreateQueue(name, WithCapacity(1<<16))
	require.NoError(t, err)
	defer func() {
		q.Close()
		q.Unlink()
	}()

	child := startChild(t, "sender", name, count)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	buf := make([]byte, 64)
	for i := 0; i < count; i++ {
		typ, msg, err := q.RecvInto(ctx, buf)
		require.NoError(t, err, "message %d", i)
		require.Equal(t, uint32(i), typ, "message %d arrived out of order", i)
		require.Equal(t, strconv.Itoa(i), string(msg))
	}

	require.NoError(t, child.Wait())
}

// TestCrossProcessConnRoundTrip pushes a payload larger than one message
// through the stream layer and back, so both the split on write and the
// reassembly on read cross a process boundary.
func TestCrossProcessConnRoundTrip(t *testing.T) {
	name := uniqueName(t)
	conn, err := Listen(name, WithCapacity(1<<16))
	require.NoError(t, err)
	defer func() {
		conn.Close()
		conn.Unlink()
	}()

	startChild(t, "echo", name, 0)

	payload := make([]byte, 512*1024)
	_, err = rand.Read(payload)
	require.NoError(t, err)

	require.NoError(t, conn.SetDeadline(time.Now().Add(60*time.Second)))

	written := make(chan error, 1)
	go func() {
		_, werr := conn.Write(payload)
		written <- werr
	}()

	got := make([]byte, len(payload))
	_, err = io.ReadFull(conn, got)
	require.NoError(t, err)
	require.NoError(t, <-written)
	assert.Equal(t, payload, got)
}

// TestCrossProcessConnEOF checks that a peer's Close reaches the reader as a
// normal end of stream rather than as a hang.
func TestCrossProcessConnEOF(t *testing.T) {
	name := uniqueName(t)
	conn, err := Listen(name, WithCapacity(MinCapacity))
	require.NoError(t, err)
	defer func() {
		conn.Close()
		conn.Unlink()
	}()

	child := startChild(t, "echo", name, 0)

	require.NoError(t, conn.SetDeadline(time.Now().Add(60*time.Second)))
	_, err = conn.Write([]byte("ping"))
	require.NoError(t, err)

	got := make([]byte, 4)
	_, err = io.ReadFull(conn, got)
	require.NoError(t, err)
	assert.Equal(t, "ping", string(got))

	// Closing this side ends the child's io.Copy, and the child's own Close
	// then sends the end-of-stream marker back.
	require.NoError(t, conn.Close())
	require.NoError(t, child.Wait())
}
