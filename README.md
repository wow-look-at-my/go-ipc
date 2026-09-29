# go-ipc

Shared-memory interprocess communication for Go. There is no cgo, no polling loop and no sleep anywhere. A blocked send or receive parks the goroutine on a kernel wait and releases its thread.

It builds on [go-shm](https://github.com/wow-look-at-my/go-shm) for the segment and on [go-mmap](https://github.com/wow-look-at-my/go-mmap) for the mapping.

## Install

```sh
go get github.com/wow-look-at-my/go-ipc
```

## Messages

A process creates the queue and reads it:

```go
q, err := ipc.CreateQueue("jobs")
if err != nil {
    return err
}
defer q.Close()
defer q.Unlink()

for {
    _, msg, err := q.Recv(ctx)
    if err != nil {
        return err
    }
    handle(msg)
}
```

Any number of other processes open the same name and write to it:

```go
q, err := ipc.OpenQueue("jobs")
if err != nil {
    return err
}
defer q.Close()

err = q.Send(ctx, []byte("work item"))
```

`TrySend` and `TryRecv` are the non-blocking forms. `ReadBatch` drains a burst in a single call. Only the handle `CreateQueue` returns can receive.

## When a process dies

- A sender that dies in the middle of a message does not wedge the queue. The receiver skips its unfinished message.
- A sender whose receiver exits or closes gets `ErrPeerGone`. So does `OpenQueue` on a queue whose receiver is gone.
- On a `Channel` or `Conn`, the other side gets every message the dead peer sent, then `ErrPeerGone`.
- `CreateQueue` returns `ErrInUse` while a live process holds the name. It replaces an instance whose creator died, and the first create in a process sweeps what crashed processes left behind.

Death is detected by a kernel wait on the process, never by a poll or a timeout. See [docs/design.md](docs/design.md).

## Streams

`Listen` and `Dial` give you a `net.Conn`. Anything that speaks `io.ReadWriter` then works unchanged:

```go
conn, err := ipc.Listen("rpc")   // one side
conn, err := ipc.Dial("rpc")     // the other

enc := gob.NewEncoder(conn)
dec := gob.NewDecoder(conn)
```

## Build a message in place

`Claim` hands back the ring bytes themselves. Build the message where the reader will find it, rather than in a buffer that the send must copy:

```go
c, err := q.Claim(ctx, 0, len(record))
if err != nil {
    return err
}
record.MarshalTo(c.Bytes)
q.Commit(c)
```

`ReadBatch` is the receiving counterpart. The payload it passes to the callback aliases shared memory and costs nothing to deliver.

## Events

`Event` is the wake primitive on its own. Use it to coordinate around state this package does not own:

```go
e, err := ipc.CreateEvent("ready")   // one process
e, err := ipc.OpenEvent("ready")     // another

err = e.Wait(ctx)   // parks the goroutine
err = e.Signal()    // releases one waiter
```

The event keeps a signal that arrives before its waiter. A check-then-wait sequence therefore never loses a wakeup.

## What it costs

A send into a queue with room makes no system call. Neither does a receive from a queue that holds a message. Both are a few atomic operations on shared memory. A system call happens only when a side has to sleep, or when a sleeping peer has to be woken.

Nothing spins, by design. A spin in user space burns a whole timeslice whenever the peer that holds the data is not running. That is the case a busy IPC path hits under load. See [this thread](https://www.realworldtech.com/forum/?threadid=189711&curpostid=189723) for the argument in full.

The test suite holds this package to that claim. `TestBlockedEndpointsConsumeNoCPU` measures the processor time of a process whose endpoints are all parked. `TestParkedWaitersHoldNoThreads` counts the OS threads that parked waiters occupy.

`latency_test.go` carries the baselines to read a round trip against, because a parked round trip cannot beat the kernel underneath it. From one `go-toolchain` benchmark run in a 4-CPU Linux container:

```
RingClaimCommit/256      58 ns/op   0 allocs/op   # no system call at all
QueueThroughput/256     817 ns/op   1 allocs/op   # batched, parks when drained
GoChannelPingPong       525 ns/op   0 allocs/op   # goroutine handoff floor
PipePingPong           2802 ns/op   0 allocs/op   # kernel round-trip floor
EventPingPong          3911 ns/op   4 allocs/op   # the wake primitive alone
QueuePingPong          4748 ns/op   4 allocs/op   # parks on every message
```

The ring is the fast path and costs no system call. Everything above a microsecond is the wakeup, and a wakeup happens whenever a side finds nothing to do. A consumer that outruns its producer therefore parks on almost every message, and pays for it. `ReadBatch` is the answer when a burst exists, because it amortizes one wakeup over the whole batch.

## Platforms

Linux, macOS and Windows, and CI runs the full suite on all three. An event uses a FIFO on Unix, which the Go runtime polls. It uses a named semaphore on Windows. Exit detection uses a Unix socket per process, on all three.

## Documentation

- [docs/design.md](docs/design.md) covers the memory layout, the record format and the wakeup protocol.
- `go doc github.com/wow-look-at-my/go-ipc` covers the API.
