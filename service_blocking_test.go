//go:build unix

package ipc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParkedCallConsumesNoCPU holds the service layer to the no-spin rule. A
// client whose call the handler has not answered, and a client whose service
// does not exist yet, are both parked for a window. The processor time the
// process spends in that window is compared against the window itself.
func TestParkedCallConsumesNoCPU(t *testing.T) {
	const (
		window = 300 * time.Millisecond
		budget = window / 10
	)
	name := uniqueName(t)
	_, h := serveTest(t, name)
	ctx := contextWithTimeout(t)

	parked := connectTest(t, name)
	answered := make(chan error, 1)
	go func() {
		_, _, err := parked.Call(ctx, testPark, nil)
		answered <- err
	}()
	waitForWaiters(t, parked.ch.rx, 1, 0)

	// A client of a service that does not exist parks on its own channel.
	early := uniqueName(t)
	connected := make(chan error, 1)
	go func() {
		c, err := Connect(ctx, early, WithCapacity(MinCapacity))
		if err == nil {
			c.Close()
		}
		connected <- err
	}()
	require.Eventually(t, func() bool { return len(scanClients(early)) == 1 }, 10*time.Second, time.Millisecond)

	before := cpuTime(t)
	<-time.After(window)
	spent := cpuTime(t) - before
	assert.Less(t, spent, budget,
		"parked calls burned %v of CPU across a %v window: something is spinning", spent, window)

	close(h.release)
	require.NoError(t, <-answered)
	svc, err := Serve(early, h, WithCapacity(MinCapacity))
	require.NoError(t, err)
	defer svc.Close()
	require.NoError(t, <-connected)
}
