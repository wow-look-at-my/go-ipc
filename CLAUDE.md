# go-ipc

Shared-memory IPC for Go, without cgo. Layers: `Ring` (lock-free MPSC records in a byte slice), `Event` (named cross-process wake), `Queue`, `Channel`, `Conn`.

- Nothing spins and nothing sleeps for a guessed interval. A side that cannot proceed parks on a kernel wait. Never add a retry loop, a yield loop or a timed backoff to a blocking path.
- `TestBlockedEndpointsConsumeNoCPU` and `TestParkedWaitersHoldNoThreads` enforce the rule above. Both are in `blocking_test.go`.
- The ring header layout is a wire format shared between processes. A build-time assertion in `ring.go` fails if its size drifts. Bump `ringVersion` for any field change.
- `Queue.Close` waits for in-flight operations before it unmaps. Any new method that touches shared memory calls `enter` and `leave` first.
- Cross-process tests re-execute the test binary through `TestMain` in `process_test.go`. Add a role there rather than a new binary.
- docs/design.md -- layout, record format, wakeup protocol, close ordering, failure modes.
