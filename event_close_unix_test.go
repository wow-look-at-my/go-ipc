//go:build unix

package ipc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpenReaderAfterCloseIsErrClosed opens a reader handle on an event that
// closed after acquire found it open. That is the order a first wait and a
// Close race into. The duplicate fails on the closed write handle, and the
// wait must report ErrClosed, not the raw closed-file error.
func TestOpenReaderAfterCloseIsErrClosed(t *testing.T) {
	ev, err := CreateEvent(uniqueName(t))
	require.NoError(t, err)
	t.Cleanup(func() { ev.Unlink() })
	if ev.impl.sock != nil {
		t.Fatal("this test drives the FIFO backend, and this host chose the socket backend")
	}

	require.NoError(t, ev.Close())
	_, err = ev.impl.openReader()
	assert.ErrorIs(t, err, ErrClosed)
}
