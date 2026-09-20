package ipc

import "runtime"

// Default spin and yield counts for the blocking endpoint operations.
//
// The spin phase keeps a busy pair of processes entirely in user space. The
// yield phase covers the case where the peer holds the only core on a small
// machine. Only after both does an endpoint pay for a wakeup.
const (
	DefaultSpins  = 64
	DefaultYields = 8
)

// backoff escalates from busy waiting to yielding to parking.
type backoff struct {
	spins  int
	yields int
	n      int
}

func newBackoff(spins, yields int) backoff {
	return backoff{spins: spins, yields: yields}
}

// once advances the policy one step. It reports whether the caller should try
// again without parking. A false return means the caller should park.
func (b *backoff) once() bool {
	switch {
	case b.n < b.spins:
		b.n++
		// The caller's own retry re-reads the shared cursor, so the spin
		// phase needs no delay of its own to be a spin.
		return true
	case b.n < b.spins+b.yields:
		b.n++
		runtime.Gosched()
		return true
	default:
		return false
	}
}

// reset returns the policy to its first step after a successful operation.
func (b *backoff) reset() { b.n = 0 }
