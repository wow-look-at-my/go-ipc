# Design

This document covers the memory layout, the record format and the wakeup protocol. The API itself is in `go doc`.

## Layers

A `Queue` is a shared memory segment that holds a `Ring`, plus a pair of `Event` handles. The ring moves the bytes. The events move a goroutine between the run state and the parked state.

```
Queue "jobs"
  /dev/shm/go-shm-jobs            segment: [ ring header 512B | data 2^n B ]
  /dev/shm/go-ipc-jobs.ne.event   FIFO: the receiver parks here
  /dev/shm/go-ipc-jobs.nf.event   FIFO: the senders park here
```

A `Channel` is a pair of queues with opposite directions. A `Conn` adds stream framing to a channel.

## Ring layout

The control block is 512 bytes. That is four cache lines of 128 bytes each. Each cursor that a different party writes gets a line of its own.

Put a pair of such cursors on one line and every producer claim invalidates the consumer's line. The reverse also happens. That false sharing costs more than the work either side does. A build-time assertion fails if the struct size drifts.

| offset | field | written by |
| --- | --- | --- |
| 0 | magic, version, capacity | the creator, once |
| 128 | `tail` | producers, by compare-and-swap |
| 256 | `head` | the consumer |
| 384 | `headCache`, `recvWaiters`, `sendWaiters` | both sides |

A cursor is 64 bits wide and never wraps in practice. So `tail - head` gives the byte count in flight with no empty-or-full ambiguity. An index into the data region is `cursor & (capacity - 1)`. That is why the capacity is a power of 2.

`headCache` lets a producer decide it has room without a read of the consumer's line. A producer refreshes it only when the cached value reports the ring full.

## Record format

A record is a header of 8 bytes and then its payload. The next record starts at the following 8-byte boundary.

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

The consumer zeroes each header before it advances `head`. No producer can claim that space until `head` moves. The zero is therefore guaranteed to land before anything reuses the slot. That zero is what makes the next lap's "nobody claimed it" state true.

## Wrapping

A record never straddles the end of the data region. The reader must be able to take one contiguous slice.

A claim that crosses the end is handled in one step. The producer claims the remaining bytes as well. It fills them with a padding record, which carries the reserved type `0xFFFFFFFF`. The reader skips padding and does not report it.

`Abort` uses the same mechanism on an unwanted claim. The maximum message is half the capacity. A claim and its worst-case padding therefore always fit in an empty ring.

## Multiple producers

A producer claims by compare-and-swap on `tail`. A failed swap means that another producer claimed first. The retry is therefore bounded by real progress somewhere. This is a lock-free retry, not a wait. No producer blocks on another producer that the scheduler stopped in the middle of a claim.

There is exactly one consumer. It needs no atomic operation to find the next record beyond the acquire load of the length. It advances `head` with a single store.

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

An earlier design put a reader goroutine per event in front of the waiters and handed tokens on over a channel. That code is gone. It cost a pair of goroutine handoffs on every wakeup, measured at about 11 microseconds per round trip on the development machine. It also carried a defect that the current design cannot express. A reader that ran while its own process had no waiter took a wakeup that a waiter in another process needed, and stranded it.

On Windows an event is a named semaphore. A waiter calls `WaitForMultipleObjects` over that semaphore, a shared close handle, and a cancel handle of its own. This wait does occupy a thread for its duration, which the Unix poller avoids. The Go runtime hands the processor to another thread meanwhile, so other goroutines keep running.

## Closing

Every queue operation registers against the mapping before it touches shared memory. `Close` sets a closing flag. It closes both events, which releases whatever parked on them. It waits for the in-flight count to reach zero. Only then does it unmap the segment.

Without that guard, a `Close` on one goroutine pulls the mapping out from under a `Send` parked on another. The process then faults on the next atomic. The ordering argument is the one the wakeup protocol uses. The closing flag and the in-flight counter are both sequentially consistent. An operation therefore either backs out or gets waited for.

A claim is the exception that the API cannot police. It points into the mapping. The caller holds it. The caller must commit it or abort it before the queue closes.

## Failure modes

A producer can die between the claim of a record and the commit of it. That leaves a negative length in the ring. The consumer stops there, because it cannot tell that case from a producer still at work. The negative marker makes the cases distinguishable to a diagnostic tool, even though the reader cannot separate them.

A corrupt length makes `Read` return `ErrCorrupt` rather than a slice out of bounds. A length is corrupt when it runs past the committed cursor, or when it is too small to hold a header.

A peer that never opens the queue is not detected. These primitives carry no liveness signal. That is a deliberate limit. A queue is a data structure, not a connection.
