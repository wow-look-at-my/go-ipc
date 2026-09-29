# go-ipc

Shared-memory IPC for Go, without cgo. Layers: `Ring` (lock-free MPSC records in a byte slice), `Event` (named cross-process wake), `Queue`, `Channel`, `Conn`.

- Nothing spins and nothing sleeps for a guessed interval. A side that cannot proceed parks on a kernel wait. Never add a retry loop, a yield loop or a timed backoff to a blocking path.
- `TestBlockedEndpointsConsumeNoCPU` and `TestParkedWaitersHoldNoThreads` enforce the rule above. Both are in `blocking_test.go`.
- The ring header layout is a wire format shared between processes. A build-time assertion in `ring.go` fails if its size drifts. Bump `ringVersion` for any field change.
- `Queue.Close` waits for in-flight operations before it unmaps. Any new method that touches shared memory calls `enter` and `leave` first.
- An `Event` waiter reads the FIFO on a handle of its own, from a free list. Never share a reader handle. A read deadline cancels a wait, and it aborts every reader of the handle it is set on.
- The consumer clears every byte it consumes. A claimed header reads as zero until its producer writes it. Stale bytes there pass for a record.
- Every queue claim writes its range into a claim slot before the `tail` swap, and clears it after the commit. The reader finds a dead producer's claim that way.
- Peer death comes from the peer's life socket (`identity.go`): the kernel ends a connection to it when the process exits. Never detect death with a timeout or a pid.
- go-toolchain builds only `GOOS=cosmo`, so its tests run the cosmo binary on each host. Every `_cosmo.go` file carries `//go:build cosmo`, or stock Go builds it everywhere.
- A creator holds the name lock for the queue's life and builds a new instance each time. Never reuse an instance in place: its old senders may still write to it.
- Cross-process tests re-execute the test binary through `TestMain` in `process_test.go`. Add a role there rather than a new binary.
- docs/design.md -- layout, record format, wakeup protocol, close ordering, failure modes.
