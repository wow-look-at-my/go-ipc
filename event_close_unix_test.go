//go:build unix

package ipc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A wait that races Close reports ErrClosed on either backend.
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
