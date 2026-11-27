//go:build cosmo || darwin || linux || windows

// connect_bench_test.go holds the benchmarks of what a connection costs to set up.

package ipc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The benchmarks here split the cost of a service connection into its
// parts. A call on an open connection is BenchmarkServiceCall. Everything
// else is paid once per connection.

// BenchmarkQueueCreate creates one queue and removes it.
func BenchmarkQueueCreate(b *testing.B) {
	name := uniqueName(b)
	b.ReportAllocs()
	for b.Loop() {
		q, err := CreateQueue(name, WithCapacity(MinCapacity))
		require.NoError(b, err)
		q.Close()
		q.Unlink()
	}
}

// BenchmarkQueueOpen attaches to a queue that exists, and lets go.
func BenchmarkQueueOpen(b *testing.B) {
	name := uniqueName(b)
	q, err := CreateQueue(name, WithCapacity(MinCapacity))
	require.NoError(b, err)
	defer q.Unlink()
	defer q.Close()
	b.ReportAllocs()
	for b.Loop() {
		s, err := OpenQueue(name)
		require.NoError(b, err)
		s.Close()
	}
}

// BenchmarkChannelCreate creates both directions of one channel and removes them.
func BenchmarkChannelCreate(b *testing.B) {
	name := uniqueName(b)
	b.ReportAllocs()
	for b.Loop() {
		ch, err := CreateChannel(name, WithCapacity(MinCapacity))
		require.NoError(b, err)
		ch.Close()
		ch.Unlink()
	}
}

func benchService(b *testing.B) string {
	b.Helper()
	name := uniqueName(b)
	svc, err := Serve(name, HandlerFunc(func(_ *Session, typ uint32, req []byte) (uint32, []byte, error) {
		return typ, req, nil
	}), WithCapacity(MinCapacity))
	require.NoError(b, err)
	b.Cleanup(func() { svc.Close() })
	return name
}

// BenchmarkServiceConnect connects to a running service and closes.
func BenchmarkServiceConnect(b *testing.B) {
	name := benchService(b)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		c, err := Connect(ctx, name, WithCapacity(MinCapacity))
		require.NoError(b, err)
		c.Close()
	}
}

// BenchmarkServiceConnectDefaultCapacity is BenchmarkServiceConnect with the capacity a client gets when it names none.
func BenchmarkServiceConnectDefaultCapacity(b *testing.B) {
	name := benchService(b)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		c, err := Connect(ctx, name)
		require.NoError(b, err)
		c.Close()
	}
}

// BenchmarkServiceTryConnect is BenchmarkServiceConnect through TryConnect.
func BenchmarkServiceTryConnect(b *testing.B) {
	name := benchService(b)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		c, err := TryConnect(ctx, name, WithCapacity(MinCapacity))
		require.NoError(b, err)
		c.Close()
	}
}

// BenchmarkServiceCall is one call on a connection that stays open.
func BenchmarkServiceCall(b *testing.B) {
	name := benchService(b)
	ctx := context.Background()
	c, err := Connect(ctx, name, WithCapacity(MinCapacity))
	require.NoError(b, err)
	defer c.Close()
	payload := []byte("ping")
	b.ReportAllocs()
	for b.Loop() {
		_, _, err := c.Call(ctx, 1, payload)
		require.NoError(b, err)
	}
}
