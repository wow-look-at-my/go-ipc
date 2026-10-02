# Design

This document covers the memory layout, the record format and the wakeup protocol. The API itself is in `go doc`.

## Layers

A `Queue` is a shared memory segment that holds a `Ring`, plus a pair of `Event` handles. The ring moves the bytes. The events move a goroutine between the run state and the parked state.

```
Queue "jobs", instance 1f0c...
  /dev/shm/go-ipc-jobs.name                 name file: the flock
  /dev/shm/go-ipc-jobs.inc                  the instance id
  /dev/shm/go-shm-jobs.1f0c...              segment: [ ring header 16.5K | data 2^n B ]
  /dev/shm/go-ipc-jobs.1f0c....ne.event     FIFO: the receiver parks here
  /dev/shm/go-ipc-jobs.1f0c....nf.event     FIFO: the senders park here
```

A `Channel` is a pair of queues with opposite directions. A `Conn` adds stream framing to a channel.

## Names and instances

A queue name points at an instance. The instance id is random hex digits. It is part of the name of the segment and of both events.

`CreateQueue` takes an exclusive lock on the name file, and holds it for the life of the queue. On Unix that is a `flock`. On Windows the creator opens the file with no write sharing and with delete-on-close. The kernel drops either lock when the creator exits. A second creator therefore gets `ErrInUse` while the first one lives, and gets the name when it does not.

The creator then removes the instance the name names, if any, and builds a new one. It writes the new id into the `.inc` file last. The id is not in the locked file. On a Windows host a cosmo `flock` is a mandatory lock. A reader of the locked file gets `EACCES`. An opener reads the id, then opens the instance. So an opener never sees a half-built instance. A stale instance is never reused, because its senders may still hold it.

A process killed before `Unlink` leaves its instance behind. On Linux that is memory in `/dev/shm`. The first `CreateQueue` or `CreateChannel` in each process sweeps the runtime directory. It removes every name file that no live process holds, with the instance it names. Windows needs no sweep: every object dies with its last handle.

The sweep takes each lock for a moment. A creator that races it on a stale name can get `ErrInUse`, and succeeds when it tries again.

## Ring layout

The control block is a run of cache lines, then the table of claim slots. `HeaderSize` is its size. Each cursor that a different party writes gets a line of its own.

Put a pair of such cursors on one line and every producer claim invalidates the consumer's line. The reverse also happens. That false sharing costs more than the work either side does. A build-time assertion fails if the struct size drifts.

| offset | field | written by |
| --- | --- | --- |
| 0 | magic, version, capacity | the creator, once |
| 24 | `consumer`, the procID of the reader | the creator, the channel peer, a close |
| 128 | `tail` | producers, by compare-and-swap |
| 256 | `head` | the consumer |
| 384 | `headCache`, `recvWaiters`, `sendWaiters` | both sides |
| 512 | claim slots: `owner`, `at`, `size` | the producer that owns the slot |

A cursor is many bits wide and never wraps in practice. So `tail - head` gives the byte count in flight with no empty-or-full ambiguity. An index into the data region is `cursor & (capacity - 1)`. That is why the capacity is a power of 2.

`headCache` lets a producer decide it has room without a read of the consumer's line. A producer refreshes it only when the cached value reports the ring full.

## Record format

A record is a header of several bytes and then its payload. The next record starts at the following 8-byte boundary.

```
 0      4        8
 +------+--------+------------------+
 | len  |  type  | payload          |
 +------+--------+------------------+
   int32  uint32
```

`len` counts the header and the payload. It is also the record's publication flag:

- `0` means that nobody claimed the slot, or that the consumer released it.
- A negative value means that a producer claimed the slot and has not finished the payload.
- A positive value means that the record is readable.

A producer writes `-len`, then the payload, then `+len`. The store of `+len` and the consumer's load of it are sequentially consistent. That is what makes the payload visible. The consumer stops at any record whose length is not positive. A record in flight therefore blocks the reader. The reader never sees a partial payload.

The consumer zeroes every byte it consumes before it advances `head`, not just the header. Records start at different offsets on each lap, so any 8-byte word of a payload can be a header on the next lap. A producer claims its range before it writes the header. If the old payload bytes stayed, a reader can take them for a committed record in that gap. No producer can claim the space until `head` moves, so the zeros always land first.

## Wrapping

A record never straddles the end of the data region. The reader must be able to take one contiguous slice.

A claim that crosses the end is handled in one step. The producer claims the remaining bytes as well. It fills them with a padding record, which carries the reserved type `0xFFFFFFFF`. The reader skips padding and does not report it.

`Abort` uses the same mechanism on an unwanted claim. The maximum message is half the capacity. A claim and its worst-case padding therefore always fit in an empty ring.

## Multiple producers

A producer claims by compare-and-swap on `tail`. A failed swap means that another producer claimed first. The retry is therefore bounded by real progress somewhere. This is a lock-free retry, not a wait. No producer blocks on another producer that the scheduler stopped in the middle of a claim.

There is exactly one consumer. It needs no atomic operation to find the next record beyond the acquire load of the length. It advances `head` with a single store.

Only the handle `CreateQueue` returns may receive. Any other handle gets `ErrNotConsumer`. The receiving handle runs its receives one at a time, so concurrent calls to `Recv` share the queue safely.

## Claim slots and dead producers

A producer that dies between its claim and its commit leaves a record the reader cannot pass. Each queue claim therefore names its producer.

A process takes a claim slot for each claim it has open, from a pool it keeps. Before the compare-and-swap on `tail`, the producer writes the range it is about to claim into its slot: `at` is the cursor, `size` is the claim. It clears `at` after the commit. The slot's `owner` is the procID of the process.

A reader that stops at a record that is not committed looks for the slots whose range covers its cursor. The true producer is always among them. A live producer's intent stays in place from before its compare-and-swap until after its commit. The reader saw `tail` move, so it also sees that intent.

- A live owner means a claim in progress. The reader watches that process for its exit and parks.
- A dead owner means a claim nobody will commit. The reader writes padding over the range, with a compare-and-swap on the length, and reads on. A claim that crossed the wrap point is reclaimed one lap segment at a time.
- Dead owners that claim different ranges at the cursor leave no way to tell which claim is real. The reader returns `ErrCorrupt`. For that, more than one producer must die at the same instant, on the same cursor.

## Process identity: the life socket

A `procID` is a random 64-bit value, so no later process reuses it. Its top bit says that a life socket stands behind it.

The first use of the package in a process listens on a Unix socket in the runtime directory, named after the procID. The process never writes to a connection there. The kernel closes every connection to it when the process exits, however it exits. So:

- A watch on a process is a connection to its life socket. A read on it ends when the process exits, and it parks in the Go poller until then.
- A check of a process is a dial. A refused dial, or a missing socket, means the process is gone.

Pids, start times and pid namespaces play no part. A container that shares the runtime directory reaches the socket, whatever pid namespace it runs in. The same code runs on Linux, macOS and Windows, and in a `GOOS=cosmo` binary on each of them.

On Unix the socket is bound under a temporary name and renamed into place. A bound socket refuses a dial until it listens, and the sweep removes a socket that refuses, but never one with a temporary name. The sweep removes the life sockets of processes that are gone, along with stale queue names. Windows has no sweep. As a result, a life socket stays in the temporary directory after its process exits.

`Release` removes the socket file of this process, just before it exits. The listener and every accepted connection stay open. A watch that started earlier still ends at the exit. A later check finds no file and judges the process gone, so nothing may send or receive after it. A process that releases leaves no file for the sweep, and on Windows none in the temporary directory.

A host that cannot listen on a Unix socket gives the process a procID without the top bit. Its queues work. No peer judges its liveness, and it judges no peer that lacks the bit either. Its death is not detected.

A process that runs out of free slots takes the slot of a dead owner, once that owner's claim no longer stops the reader. When live claims hold all `ClaimSlots` slots, a claim fails with `ErrTooManyClaims`. A slot is held only while a claim is open. So only a caller that keeps that many `Claim` values open, or that many senders inside the copy at once, reaches the limit.

`Ring.TryClaim` on a raw ring makes an unattributed claim. The reader cannot recover one, and waits for it forever.

## Peers

The `consumer` field holds the procID of the process that reads the queue. A sender checks it on every operation.

- `0` means the reader closed. The sender gets `ErrPeerGone`.
- A procID gets a watch on that process, the first time the sender sees it: a connection to its life socket. When the process exits, the watch marks the peer gone and wakes every sender this process has parked on the queue.
- The value `1` means a channel direction whose peer has not connected. Sends go through and wait in the ring.

A process that is already dead is reported at once.

A channel's peer is the reader of its outbound queue. `OpenChannel` connects by a compare-and-swap of that field from `1` to its own procID. A second peer gets `ErrInUse`. The receive side of a channel checks the peer before each read, and reports `ErrPeerGone` only after the read finds nothing. A peer that sent a message and then exited has its message delivered first.

## The wakeup protocol

An endpoint with work to do never calls into the kernel. It writes into the ring, or it reads out of the ring, and returns. A system call happens only when a side must sleep, or when a peer that sleeps must wake.

A side that cannot proceed follows this sequence:

1. Try the operation. Return if it succeeds.
2. Publish this side in `recvWaiters` or in `sendWaiters`.
3. Try the operation again. Withdraw and return if it succeeds now.
4. Park on the event.

A side that makes progress reads the opposite waiter count after it publishes its change. It signals only when that count is above zero.

The repeated attempt is what makes this safe. The waiter's increment and the peer's read of that counter are both sequentially consistent. So are the ring cursor updates. Each side therefore sees the other. Either the peer's change is visible to the waiter, or the waiter is visible to the peer. No wakeup falls between them.

A wakeup is also allowed to be wrong in the harmless direction. The event keeps a signal that finds no waiter. It hands that signal to the next waiter. A waiter that wakes with nothing to do loops back to the first step. Only a lost wakeup is a defect, and the sequence above rules that out.

## Why nothing spins

An earlier draft of this package spun for a while before it parked. That code is gone.

A spin in user space is a bet that the peer runs on another core right now. The bet loses when the peer does not. The spinner then holds a core for its whole timeslice. The peer it waits for cannot get scheduled onto that core. The load where IPC latency matters most is exactly the load where this bet loses. [This thread](https://www.realworldtech.com/forum/?threadid=189711&curpostid=189723) gives the argument in full.

The same reasoning rules out a sleep for a guessed interval. Such a sleep is wrong in both directions at once. Too long and the message waits. Too short and it is a spin with extra steps.

One honest option is left. Tell the kernel what you wait for. Let the kernel decide when to run you.

## Events

An event is a counting wake channel. `Signal` releases one waiter. `SignalN` releases n of them.

On Unix an event is a FIFO, opened read-write. A named FIFO needs no handshake to share. The read-write mode never blocks on open. It also never reports end-of-file when the peer goes away. The Go runtime polls a FIFO. A read therefore parks the goroutine and hands its thread back to the scheduler.

The constructor proves that property rather than assumes it. It sets a read deadline, which only a polled handle accepts. It fails with `ErrNotPollable` otherwise.

A signal writes one byte per waiter through a raw non-blocking write. A full pipe already holds more pending wakeups than there are waiters. A dropped write therefore costs nothing.

A waiter reads the FIFO itself, through a handle of its own. It takes that handle from a free list and returns it on the way out. Cancellation then sets a read deadline on the private handle, which aborts this read and touches no other reader. A deadline on a shared handle aborts every reader of it.

A token is consumed only by a read that returns it. A cancelled wait therefore swallows no wakeup. A signal that arrives before any waiter stays in the pipe until a waiter reads it.

An earlier design put a reader goroutine per event in front of the waiters and handed tokens on over a channel. That code is gone. It cost a pair of goroutine handoffs on every wakeup, measured at several microseconds per round trip on the development machine. It also carried a defect that the current design cannot express. A reader that ran while its own process had no waiter took a wakeup that a waiter in another process needed, and stranded it.

On Windows an event is a named semaphore. A waiter calls `WaitForMultipleObjects` over that semaphore, a shared close handle, and a cancel handle of its own. This wait does occupy a thread for its duration, which the Unix poller avoids. The Go runtime hands the processor to another thread meanwhile, so other goroutines keep running.

A cosmo binary on a Windows host has no FIFOs, because `mkfifo` fails there. Its events use a Unix socket instead (`event_sock.go`). The creator listens on the socket and keeps the token count. A waiter connects and sends a wait request. The creator answers with a token when one is free. A signal connects and adds tokens. A cancelled waiter sends a cancel. The creator answers it only while the waiter is still queued, so no token is lost. The socket name is a hash of the event path, because a socket path must fit in `sun_path`.

When the creator exits or closes, every waiter connection breaks. The wait then reports `ErrPeerGone`, because nobody can signal the event after that. A signal to a dead creator is dropped, the same as a write to a FIFO that nobody reads. An open checks the path with `Lstat`. A dial cannot tell a dead creator from a missing path, because Windows refuses both. `Lstat` fails with `ENOENT` only when the path is gone.

## Closing

Every queue operation registers against the mapping before it touches shared memory. `Close` sets a closing flag. The receiving handle then clears `consumer` and wakes the parked senders, which find `ErrPeerGone`. `Close` cancels its watches. It closes both events, which releases whatever parked on them. It waits for the in-flight count to reach zero. It returns its claim slots and unmaps the segment. Last, it releases the name.

Without that guard, a `Close` on one goroutine pulls the mapping out from under a `Send` parked on another. The process then faults on the next atomic. The ordering argument is the one the wakeup protocol uses. The closing flag and the in-flight counter are both sequentially consistent. An operation therefore either backs out or gets waited for.

A claim is the exception that the API cannot police. It points into the mapping. The caller holds it. The caller must commit it or abort it before the queue closes.

## Failure modes

A producer that dies holding a claim is covered in "Claim slots and dead producers". A reader or a channel peer that dies is covered in "Peers".

A corrupt length makes `Read` return `ErrCorrupt` rather than a slice out of bounds. A length is corrupt when it runs past the committed cursor, runs past the end of the data region, or is too small to hold a header.

A plain queue has no peer. Its receiver waits for new senders for as long as it runs. A context is the way to bound that wait.

A sender that dies while it is parked leaves `sendWaiters` one too high. The reader then makes a signal call that nobody needs each time it frees space. That costs time and loses nothing. It lasts until the queue is replaced.
