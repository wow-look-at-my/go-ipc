# go-ipc

Shared-memory IPC for Go, without cgo. Layers: `Ring` (lock-free MPSC records in a byte slice), `Event` (named cross-process wake), `Queue`, `Channel`, `Conn`.

- Nothing spins and nothing sleeps for a guessed interval. A side that cannot proceed parks on a kernel wait. Never add a retry loop, a yield loop or a timed backoff to a blocking path.
- `TestBlockedEndpointsConsumeNoCPU` and `TestParkedWaitersHoldNoThreads` enforce the rule above. Both are in `blocking_test.go`.
- The ring header layout is a wire format shared between processes. A build-time assertion in `ring.go` fails if its size drifts. Bump `ringVersion` for any field change.
- `Queue.Close` waits for in-flight operations before it unmaps. Any new method that touches shared memory calls `enter` and `leave` first.
- An `Event` waiter reads the FIFO on a handle of its own, from a free list. Never share a reader handle. A read deadline cancels a wait, and it aborts every reader of the handle it is set on.
- Cross-process tests re-execute the test binary through `TestMain` in `process_test.go`. Add a role there rather than a new binary.
- docs/design.md -- layout, record format, wakeup protocol, close ordering, failure modes.
- `make test` is the entry point for every language. The root `go-toolchain` run comes last, because it also runs the nested modules `interop/`, `codegen/go` and `ipcgen/`, which need the native peers and generated code.
- `spec/` is the wire contract, from wow-look-at-my/go-ipc-spec. A change to the ring, event or queue protocol changes spec/ first. Every implementation replays `spec/vectors/ring` and must match the images byte for byte.
- `c/` is the C library (`c/include/goipc.h`), `cpp/` the header-only C++20 port, `python/` a ctypes binding over the C library for CPython 3.8 and later. Each has its own Makefile and a peer for spec/peer.md.
- `interop/` runs every pair of languages against each other through the peers. A missing peer fails the run and never skips it.
- `ipcgen/` is a nested Go module: the schema code generator for Go, C, C++ and Python. spec/schema.md is its contract. `codegen/` tests the generated code against spec/vectors/schema.
