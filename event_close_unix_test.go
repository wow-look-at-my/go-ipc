//go:build unix

package ipc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpenReaderAfterCloseIsErrClosed opens a reader handle on an event that
// closed after acquire found it open. That is the order a first wait and a
// Close race into. The duplicate fails on the closed write handle, and the
// wait must report ErrClosed, not the raw closed-file error. A host on the
// socket backend has no reader handle, so its wait answers the same contract.
func TestOpenReaderAfterCloseIsErrClosed(t *testing.T) {
	ev, err := CreateEvent(uniqueName(t))
	require.NoError(t, err)
	t.Cleanup(func() { ev.Unlink() })

	require.NoError(t, ev.Close())
	if ev.impl.sock != nil {
		assert.ErrorIs(t, ev.impl.wait(context.Background()), ErrClosed)
		return
	}
	_, err = ev.impl.openReader()
	assert.ErrorIs(t, err, ErrClosed)
}
