# go-ipc wire specification

This directory is the contract that every implementation of go-ipc follows. The Go package, the C library, the C++ header and the Python binding are all bound to it. Each test suite loads `wire.json` and the files in `vectors/`, and fails when its own constants or its own byte layout drift from them.

`docs/design.md` explains why the protocol has this shape. This file states what the protocol is. Where the two disagree, this file and `wire.json` win.

Scope: Linux. All integers on the wire are little-endian. All atomic operations named here are sequentially consistent unless the text says otherwise.

## Files

| file | content |
| --- | --- |
| `wire.json` | every constant and offset below, in machine-readable form |
| `vectors/ring/manifest.json` | ring operations to replay, plus the expected records |
| `vectors/ring/*.bin` | the exact ring image that each replay must produce |
| `schema.md` | the `ipcgen` schema language and its wire encoding |
| `peer.md` | the command-line contract that the cross-language tests drive |

## Names

A name is a non-empty string. It must not contain `/` or `\`. It must not be `.` or `..`. An implementation rejects any other name before it touches the file system.

## Ring

A ring is a buffer of `512 + capacity` bytes. The buffer starts on an 8-byte boundary. `capacity` is a power of 2 and at least 4096.

### Control block

| offset | size | field | type |
| --- | --- | --- | --- |
| 0 | 8 | `magic` | u64, `0x676F2D6970632D31` |
| 8 | 4 | `version` | u32, `1` |
| 12 | 4 | `flags` | u32, `0` |
| 16 | 8 | `capacity` | u64 |
| 128 | 8 | `tail` | u64, atomic |
| 256 | 8 | `head` | u64, atomic |
| 384 | 8 | `head_cache` | u64, atomic |
| 392 | 4 | `recv_waiters` | i32, atomic |
| 396 | 4 | `send_waiters` | i32, atomic |

Every other byte of the control block is zero.

**Init.** Zero the whole buffer. Set `capacity` to the largest power of 2 that is not more than `len - 512`. Set `version`. Store `magic` last, with an atomic store.

**Attach.** Load `magic` atomically. Reject the buffer when `magic` or `version` differ, when `capacity` is below 4096 or not a power of 2, or when the buffer is shorter than `512 + capacity`. Attach never writes.

### Records

The data region starts at byte 512. Byte `i` of the region is addressed by `cursor & (capacity - 1)`. `tail` and `head` are byte cursors that only grow.

A record is an 8-byte header and then the payload:

| offset | size | field |
| --- | --- | --- |
| 0 | 4 | `len`, i32, atomic. The header plus the payload, before alignment. |
| 4 | 4 | `type`, u32. Plain access; the `len` store orders it. |

The next record starts at `align8(len)`. The type `0xFFFFFFFF` is padding. A sender must not use it.

`len` is the publication flag. `0` means free. A negative value means that a producer holds a claim. A positive value means the record is readable.

The largest payload is `min(capacity / 2 - 8, 2^31 - 1 - 8)`.

### Claim (any number of producers)

For a payload of `n` bytes, `rec = 8 + n` and `need = align8(rec)`.

1. Load `tail`. Load `head_cache`.
2. `index = tail & mask` and `to_end = capacity - index`. When `to_end < need`, the claim also covers a padding record over the remaining bytes, so `need = align8(rec) + to_end`.
3. When `capacity - (tail - head_cache) < need`, load `head`. When it is still too small, report full. Otherwise store `head` into `head_cache`.
4. Compare-and-swap `tail` from `tail` to `tail + need`. On failure, go back to step 1.
5. When the claim covers padding: store `type = 0xFFFFFFFF` at `index`, then store `len = to_end` at `index`. The record then starts at index 0.
6. Store `len = -rec` at the record. Store `type`.

**Commit:** store `len = +rec`. **Abort:** store `type = 0xFFFFFFFF`, then store `len = +rec`.

### Read (exactly one consumer)

1. Load `head`. Load `tail`. `available = tail - head`.
2. At `(head + consumed) & mask`, load `len`. Stop when `len <= 0`.
3. When `len < 8` or `align8(len) > available - consumed`, report corrupt.
4. Deliver the record unless its type is padding. Store `len = 0`. Add `align8(len)` to `consumed`.
5. After the loop, when `consumed > 0`, store `head + consumed` into `head`.

A receive that finds the next record too large for the caller's buffer does not consume it.

## Event

An event is a FIFO at `/dev/shm/go-ipc-{name}.event`, mode `0600`.

- **Create:** remove any file at the path, then `mkfifo` it. Open it.
- **Open:** open the path. Every handle is `O_RDWR | O_NONBLOCK | O_CLOEXEC`. Read-write mode keeps a writer attached, so a reader never sees end-of-file.
- **Signal n:** cap `n` at 4096. Write `n` bytes of any value in one non-blocking `write`. `EAGAIN` is success, because a full pipe already holds more wakeups than there are waiters. A signal never blocks.
- **Wait:** a waiter consumes exactly one byte. It blocks in the kernel (a poller, `poll`, or a blocking read) until the FIFO is readable, the event closes, or the timeout ends. `EAGAIN` on the read means that another waiter took the byte, so the waiter blocks again. A waiter that times out or sees a close has consumed nothing.
- **Unlink:** remove the path. A missing file is not an error.

No implementation may spin, yield in a loop, or sleep for a guessed interval on any blocking path.

## Queue

A queue named `q` is files:

| file | content |
| --- | --- |
| `/dev/shm/go-shm-q` | the ring, exactly `512 + capacity` bytes |
| `/dev/shm/go-ipc-q.ne.event` | not-empty event; the consumer waits here |
| `/dev/shm/go-ipc-q.nf.event` | not-full event; producers wait here |

**Create** makes the `.ne` event, then the `.nf` event, then the segment (`O_CREAT | O_RDWR | O_TRUNC`, mode `0600`, `ftruncate`, shared mapping), then runs ring init. A peer that finds the segment therefore finds both events. The default capacity is 1048576.

**Open** maps the segment at its file size, attaches the ring, then opens both events.

**Park.** A side that cannot proceed runs this sequence, with `waiters` being `recv_waiters` for the consumer and `send_waiters` for a producer:

1. Try. Return on success.
2. Atomically add 1 to `waiters`.
3. Try again. On success, subtract 1 and return.
4. Wait on the event. Subtract 1. On a wait error, return it. Otherwise go to step 1.

**Wake.** After a commit or an abort, a producer loads `recv_waiters`. It signals `.ne` once when the value is above 0. After a read that moved `head`, the consumer loads `send_waiters` as `w`. It signals `.nf` with `n = w` when `w > 0`. A read that only stepped over padding still moved `head`, so it still wakes.

**Close** releases this process's handles and waits for this process's in-flight operations to leave the mapping. It does not remove files. **Unlink** removes the segment and both event files.

## Channel

A channel named `c` is queues. The creator creates `c.c2o` and then `c.o2c`. It sends on `c.c2o` and receives on `c.o2c`. The opener opens both and swaps the roles.

## Conn

A conn is a byte stream over a channel. `Listen` creates the channel. `Dial` opens it.

- A write splits its input into messages of at most the maximum message size, with type `0`.
- A read returns bytes from the current message, then receives the next one. A message of type `1` is end-of-stream. Every read after it reports end-of-stream.
- CloseWrite sends one type `1` message with an empty payload through a blocking send. Later writes fail as closed. Reads go on.
- Close sends that message best effort and non-blocking, unless CloseWrite already sent it. It then closes the channel.

## Conformance

An implementation conforms when it passes checks:

1. Its constants match `wire.json`.
2. It replays every case in `vectors/ring/manifest.json` on a zeroed buffer and produces the matching `.bin` byte for byte. It also attaches to each `.bin` and reads exactly the expected records.
3. It passes the cross-language matrix in `interop/` against every other implementation, in both directions.
