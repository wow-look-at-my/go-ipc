// Package ipc provides shared-memory interprocess communication without cgo.
//
// [Ring] is a lock-free multi-producer single-consumer buffer of
// variable-length records inside a byte slice. [Event] is a named cross-process
// wake primitive whose waiters park on the Go poller. A blocked wait costs no
// thread and honors a [context.Context]. [Queue], [Channel] and [Conn] combine
// both into named endpoints with blocking sends and receives.
//
// An endpoint with work to do makes no system call. An endpoint that cannot
// proceed parks on a kernel wait. Nothing spins. A process that dies holding
// a claim does not wedge the queue. spec/README.md is the wire contract that
// the C, C++ and Python implementations share.
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

	ErrInvalidCapacity = errors.New("ipc: capacity must be a power of two of at least 4096 bytes")

	// ErrTooSmall reports a buffer that cannot hold the header and a data region.
	ErrTooSmall = errors.New("ipc: buffer is too small to hold a ring")

	// ErrBadLayout reports a buffer that does not hold a ring this build understands.
	ErrBadLayout = errors.New("ipc: buffer does not hold a compatible ring")

	// ErrCorrupt reports a record header that cannot be valid.
	ErrCorrupt = errors.New("ipc: ring contents are corrupt")

	// ErrUnaligned reports a buffer whose earliest byte is not 8-byte aligned.
	ErrUnaligned = errors.New("ipc: buffer is not 8-byte aligned")

	// ErrNotPollable reports an event handle that cannot join the Go poller.
	ErrNotPollable = errors.New("ipc: event handle does not support polling")

	// ErrPeerGone reports that the process at the other end exited or closed its end.
	ErrPeerGone = errors.New("ipc: peer is gone")

	// ErrInUse reports a name that a live process holds.
	ErrInUse = errors.New("ipc: name is in use")

	// ErrNotConsumer reports a receive on a handle that does not own the receiving end.
	ErrNotConsumer = errors.New("ipc: handle is not the receiving end")

	// ErrTooManyClaims reports that every claim slot of a ring is in use.
	ErrTooManyClaims = errors.New("ipc: too many claims in progress")
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
