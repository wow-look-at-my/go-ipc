// Package ipc provides shared-memory interprocess communication without cgo.
//
// [Ring] is a lock-free multi-producer single-consumer buffer of
// variable-length records inside a byte slice. [Event] is a named cross-process
// wake primitive whose waiters park on the Go poller, so a blocked wait costs
// no thread and honors a [context.Context]. [Queue], [Channel] and [Conn]
// combine the two into named endpoints with blocking sends and receives.
//
// Send and Recv spin before they park, so an active endpoint moves messages
// with no system call. See README.md for usage and docs/design.md for layout.
package ipc

import (
	"errors"
	"strings"
)

// Errors reported by this package.
var (
	// ErrClosed reports use of an endpoint after Close.
	ErrClosed = errors.New("ipc: endpoint is closed")

	// ErrFull reports that a ring has no room right now.
	ErrFull = errors.New("ipc: ring is full")

	// ErrEmpty reports that a ring holds no message right now.
	ErrEmpty = errors.New("ipc: ring is empty")

	// ErrMessageTooLarge reports a message above the ring maximum size.
	ErrMessageTooLarge = errors.New("ipc: message exceeds the maximum size")

	// ErrReservedType reports use of the padding type as a message type.
	ErrReservedType = errors.New("ipc: message type is reserved")

	// ErrInvalidName reports a name that is empty or holds a path separator.
	ErrInvalidName = errors.New("ipc: name must not be empty or contain a separator")

	// ErrInvalidCapacity reports a capacity that is too small or not a power of two.
	ErrInvalidCapacity = errors.New("ipc: capacity must be a power of two of at least 4096 bytes")

	// ErrTooSmall reports a buffer that cannot hold the header and a data region.
	ErrTooSmall = errors.New("ipc: buffer is too small to hold a ring")

	// ErrBadLayout reports a buffer that does not hold a ring this build understands.
	ErrBadLayout = errors.New("ipc: buffer does not hold a compatible ring")

	// ErrCorrupt reports a record header that cannot be valid.
	ErrCorrupt = errors.New("ipc: ring contents are corrupt")

	// ErrUnaligned reports a buffer whose first byte is not 8-byte aligned.
	// The ring stores atomics inside the buffer and needs that alignment.
	ErrUnaligned = errors.New("ipc: buffer is not 8-byte aligned")

	// ErrNotPollable reports an event handle that cannot join the Go poller.
	// A wait on it would occupy a thread, so the constructor refuses it.
	ErrNotPollable = errors.New("ipc: event handle does not support polling")
)

// validateName rejects a name that cannot become a file name. A name reaches
// the file system on Unix and the kernel object namespace on Windows.
func validateName(name string) error {
	if name == "" {
		return ErrInvalidName
	}
	if strings.ContainsAny(name, `/\`) {
		return ErrInvalidName
	}
	if name == "." || name == ".." {
		return ErrInvalidName
	}
	return nil
}
