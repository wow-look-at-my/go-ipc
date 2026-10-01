# go-ipc wire specification

This directory is the contract that every implementation of go-ipc follows. The Go package, the C library, the C++ header and the Python binding are all bound to it. Each test suite loads `wire.json` and the files in `vectors/`, and fails when its own constants or its own byte layout drift from them.

`docs/design.md` explains why the protocol has this shape. This file states what the protocol is. Where the two disagree, this file and `wire.json` win.

This is version 2 of the protocol. A version 2 process does not interoperate with a version 1 process. The ring `version` field tells them apart.

Scope: Linux. All integers on the wire are little-endian. All atomic operations named here are sequentially consistent unless the text says otherwise. The Go package also runs on macOS and Windows, and as a cosmo binary. Its socket event backend for a cosmo binary on a Windows host is Go-only and out of scope here.

## Files

| file | content |
| --- | --- |
| `wire.json` | every constant, offset and path below, in machine-readable form |
| `vectors/ring/manifest.json` | ring operations to replay, plus the expected records |
| `vectors/ring/*.bin` | the exact ring image that each replay must produce |
| `schema.md` | the `ipcgen` schema language and its wire encoding |
| `peer.md` | the command-line contract that the cross-language tests drive |

## Names

A name is a non-empty string. It must not contain `/` or `\`. It must not be `.` or `..`. An implementation rejects any other name with the invalid-name error before it touches the file system.

## Paths

Every file lives in the runtime directory `/dev/shm`. Every file that this protocol creates has mode `0600`. The placeholders are these:

| placeholder | format |
| --- | --- |
| `{name}` | a queue name |
| `{id}` | an instance id: 8 bytes from a cryptographic random source, as 16 lowercase hexadecimal digits |
| `{procid}` | a procID as 16 lowercase hexadecimal digits, with leading zeros |

| path | kind | content |
| --- | --- | --- |
| `/dev/shm/go-ipc-{name}.name` | regular file | The name lock. The content is unused and stays empty. |
| `/dev/shm/go-ipc-{name}.inc` | regular file | The id of the current instance: exactly 16 lowercase hexadecimal digits, with no newline. |
| `/dev/shm/go-shm-{name}.{id}` | regular file | The ring of the instance. |
| `/dev/shm/go-ipc-{name}.{id}.ne.event` | FIFO | The not-empty event of the instance. The consumer waits here. |
| `/dev/shm/go-ipc-{name}.{id}.nf.event` | FIFO | The not-full event of the instance. Producers wait here. |
| `/dev/shm/go-ipc-life-{procid}.sock` | Unix socket | The life socket of the process `{procid}`. |
| `/dev/shm/go-ipc-life-{procid}.sock.tmp` | Unix socket | A life socket before its process renames it into place. |
| `/dev/shm/go-ipc-{event}.event` | FIFO | A standalone event named `{event}`. A queue event is this pattern with `{event}` = `{name}.{id}.ne` or `{name}.{id}.nf`. |

The instance name is `{name}.{id}`. A new instance always gets a new id.

## Process identity

### procID

A procID is a u64 that names one process for its whole life.

- A process fills all of the procID from a cryptographic random source. It then sets bit 62 and bit 63.
- Bit 63 means that a life socket stands behind the procID. A process that cannot listen on its life socket clears bit 63 and keeps every other bit.
- Bit 62 is always set. So a procID never equals 0 or 1, which have their own meaning in the `consumer` field.

A process makes its procID once, the first time it creates or opens a queue, a channel or a conn. It keeps it until it exits. Its life socket listens before the procID first reaches shared memory. Otherwise a peer can find no socket and judge the process dead.

### Life socket

**Listen.**

1. Create a socket with `AF_UNIX`, `SOCK_STREAM` and `SOCK_CLOEXEC`.
2. Bind it to the life socket path plus `.tmp`.
3. Listen on it.
4. Change the mode of the `.tmp` path to `0600`.
5. Rename the `.tmp` path to the life socket path.

On any failure, close the socket, remove the `.tmp` path, and run without a life socket: clear bit 63 of the procID. The rename comes after the listen because a bound socket that does not listen refuses a dial. The sweep removes every life socket that refuses. The sweep never touches a `.tmp` name.

**Serve.** Accept every connection. Hold each connection open and read from it until end-of-file or an error, discarding the bytes, then close it. Never write to a connection. Never remove the socket file on exit. The sweep removes it. The kernel closes every connection to the socket when the process exits, however it exits. A connection that waits in the backlog ends with the process too. A process must still accept, because the backlog is bounded.

A child that a process forks without an exec inherits the listening socket. The life of the parent then ends when the last of them exits. `SOCK_CLOEXEC` keeps an exec from inheriting it.

**Check.** A check asks whether the process `p` is gone. When bit 63 of `p` is clear, `p` counts as alive. Otherwise connect to the life socket of `p`. On success, close the connection: `p` is alive. When the connect fails with `ENOENT` or `ECONNREFUSED`, `p` is gone. Any other error means alive.

**Watch.** A watch reports the exit of the process `p`. Only a procID with bit 63 set can be watched.

1. Connect to the life socket of `p`. On `ENOENT` or `ECONNREFUSED`, `p` is already gone. On any other error, the watch fails with that error.
2. Read from the connection, parked in the kernel: a poller, `poll`, or a blocking read on a thread of its own. Discard any bytes and read again.
3. A read that returns end-of-file means that `p` exited.
4. A read that fails runs a check of `p`. When `p` is gone, it exited. Otherwise the watch fails with the read error.

Closing the connection cancels the watch. One connection per watched process is enough, however many parts of the process watch it.

Process ids, start times and pid namespaces play no part in this. A timeout never decides that a process is gone.

## Ring

A ring is a buffer of `16896 + capacity` bytes. The buffer starts on an 8-byte boundary. `capacity` is a power of 2 and at least 4096.

### Control block

The control fields come first. The claim slot table follows them, from byte 512 up to `header_size`, which is 16896.

| offset | size | field | type |
| --- | --- | --- | --- |
| 0 | 8 | `magic` | u64, `0x676F2D6970632D31` |
| 8 | 4 | `version` | u32, `2` |
| 12 | 4 | `flags` | u32, `0` |
| 16 | 8 | `capacity` | u64 |
| 24 | 8 | `consumer` | u64, atomic |
| 128 | 8 | `tail` | u64, atomic |
| 256 | 8 | `head` | u64, atomic |
| 384 | 8 | `head_cache` | u64, atomic |
| 392 | 4 | `recv_waiters` | i32, atomic |
| 396 | 4 | `send_waiters` | i32, atomic |
| 512 | 16384 | claim slots | 256 slots of 64 bytes |

Claim slot `i` starts at byte `512 + 64 * i`:

| offset | size | field | type |
| --- | --- | --- | --- |
| 0 | 8 | `owner` | u64, atomic. The procID that holds the slot, or 0 when the slot is free. |
| 8 | 8 | `at` | u64, atomic. The cursor of the claim that the owner makes now, or `NO_INTENT`. |
| 16 | 8 | `size` | u64, atomic. The bytes of that claim, padding included. |

`NO_INTENT` is `0xFFFFFFFFFFFFFFFF`. Every other byte of the control block is zero.

`consumer` holds the procID of the process that reads the ring:

| value | meaning |
| --- | --- |
| 0 | Nobody reads the ring any more: the reader closed. A raw ring also holds 0. |
| 1 | A channel direction whose peer has not connected yet. |
| any other | The procID of the reader. |

**Init** takes the buffer and a consumer value. Zero the whole buffer. Set `capacity` to the largest power of 2 that is not more than `len - 16896`. Set `version`. Store the consumer value into `consumer`. Store `NO_INTENT` into `at` of all slots. Store `magic` last, with an atomic store. A raw ring passes consumer 0.

**Attach.** Load `magic` atomically. Reject the buffer as a bad layout when `magic` or `version` differ, when `capacity` is below 4096 or not a power of 2, or when the buffer is shorter than `16896 + capacity`. A buffer shorter than `16896 + 4096` bytes is too small. A buffer whose first byte is not 8-byte aligned is unaligned. Attach never writes.

### Records

The data region starts at byte 16896. Byte `i` of the region is addressed by `cursor & (capacity - 1)`. `tail` and `head` are byte cursors that only grow.

A record is an 8-byte header and then the payload:

| offset | size | field |
| --- | --- | --- |
| 0 | 4 | `len`, i32, atomic. The header plus the payload, before alignment. |
| 4 | 4 | `type`, u32. Plain access; the `len` store orders it. |

The next record starts at `align8(len)`. The type `0xFFFFFFFF` is padding. A sender must not use it.

`len` is the publication flag. `0` means free. A negative value means that a producer holds a claim. A positive value means the record is readable.

The largest payload is `min(capacity / 2 - 8, 2^31 - 1 - 8)`.

### Claim slots

A queue producer attributes each claim to a claim slot. The slot names the process behind the claim. A reader that stops at a claim can then tell a claim in progress from a claim whose producer died. A claim on a raw ring has no slot. Nothing recovers it.

**Acquire.** The process `self` takes a slot:

1. Free path: for `i` from 0 to 255, compare-and-swap `owner` of slot `i` from 0 to `self`. On the first success, store `NO_INTENT` into `at` of slot `i`. The slot is `i`.
2. Dead path: load `head`. For `i` from 0 to 255, load `owner` of slot `i` as `o`. Skip the slot when `o` is 0 or `self`. Load `at`. When `at` is not `NO_INTENT`, load `size`. Skip the slot when `at + size > head`, because its claim still lies in front of the reader. Skip it when a check does not find `o` gone. Then compare-and-swap `owner` from `o` to `self`. On success, store `NO_INTENT` into `at`. The slot is `i`.
3. Otherwise report too-many-claims.

**Pool.** A handle keeps the slots it acquired. A claim takes an idle slot of the handle, and acquires a new one only when none is idle. After the claim commits, aborts or fails, the slot goes back to the idle list of the handle. The handle still owns it. The handle gives up its slots only at close: for each slot, store `NO_INTENT` into `at`, then compare-and-swap `owner` from `self` to 0.

A slot that the owner holds idle cannot be taken while the owner lives. So too-many-claims also reports slots that live handles keep idle. Go keeps a pool per queue handle.

### Claim (any number of producers)

For a payload of `n` bytes, `rec = 8 + n` and `aligned = align8(rec)`. A queue claim uses the slot `s` from the pool. A raw ring claim has no slot, and skips every step on `s`.

1. Load `tail`. Load `head_cache`.
2. `index = tail & mask` and `to_end = capacity - index`. `need = aligned`. When `to_end < aligned`, the claim also covers a padding record over the remaining bytes, so `need = aligned + to_end`.
3. The cached head has room when `head_cache <= tail`, `tail - head_cache <= capacity` and `capacity - (tail - head_cache) >= need`. When it has no room, load `head`. When `head > tail`, go back to step 1, because other producers moved `tail` and the reader followed. When `head` has no room by the same test, store `NO_INTENT` into `at` of `s` and report full. Otherwise store `head` into `head_cache`.
4. Store `need` into `size` of `s`. Then store `tail` into `at` of `s`.
5. Compare-and-swap `tail` from `tail` to `tail + need`. On failure, go back to step 1. The slot intent stays until the next pass rewrites it.
6. When the claim covers padding: store `type = 0xFFFFFFFF` at `index`, then store `len = to_end` at `index`. The record then starts at index 0.
7. Store `type` at the record. Then store `len = -rec` at the record.

**Commit:** store `len = +rec`. Then store `NO_INTENT` into `at` of `s`. **Abort:** store `type = 0xFFFFFFFF`, then store `len = +rec`. Then store `NO_INTENT` into `at` of `s`. `size` keeps its last value. The vectors check it.

Step 4 comes before the swap in step 5, and the commit clears `at` after the length. So the intent of a live producer covers its claim from before `tail` moves until after the record is readable. A reader that sees `tail` past a claim also sees that intent.

The type store comes before the length store in step 6, because the positive length publishes the padding record. In step 7 no reader can observe the order, since a negative length stops the reader. Go stores the type first, and the vectors check the bytes either way.

### Read (exactly one consumer)

1. Load `head`. Load `tail`. `available = tail - head`. `consumed = 0`.
2. At `index = (head + consumed) & mask`, load `len`. Stop when `len <= 0`.
3. When `len < 8`, `align8(len) > available - consumed` or `index + align8(len) > capacity`, report corrupt.
4. Deliver the record unless its type is padding. Zero bytes 4 up to `align8(len)` of the record. Then store `len = 0` atomically. Add `align8(len)` to `consumed`.
5. After the loop, when `consumed > 0`, store `head + consumed` into `head`.

A receive that finds the next record too large for the caller's buffer does not consume it.

Every byte of the data region outside `[head, tail)` is zero. The reader keeps this true, because a producer advances `tail` before it stores its header. In that window the header word must read as `0`, so that the reader stops. A word that held old payload can read as a length.

A cached head can be more than a lap old. A producer that a scheduler stops between its load of `head` and its store into `head_cache` stores an old value. The room test in step 3 rejects it, because `tail - head_cache` then exceeds the capacity.

### Recovery of a dead producer's claim

The consumer of a queue runs this after a read delivered no record. It finds the claim that stops the reader at `head`, and turns the claim of a dead producer into padding.

1. Load `head`. Load `tail`. When they are equal, nothing stops the reader.
2. Load `len` at `head & mask` as `L`. When `L > 0`, nothing stops the reader.
3. Collect every slot whose `at` is not `NO_INTENT` and covers `head`: `at <= head < at + size`. Load `at` before `size`.
4. Visit the collected slots in index order. Load `owner`, `at` and `size` again. Skip the slot when `owner` is 0, `at` is `NO_INTENT`, or the range no longer covers `head`.
   - When `owner` is the procID of the reader's own process, or bit 63 of `owner` is clear, stop. Nothing is recovered.
   - When a check does not find `owner` gone, start a watch on `owner`, unless the queue handle already watches it. Stop. Nothing is recovered.
   - `owner` is gone. When an earlier visited slot was also gone and its `at + size` differs from this one, report corrupt. Dead producers that disagree on the claim leave no way to tell which claim is real.
   - Remember `end = at + size` and the slot.
5. When no slot was gone, nothing is recovered.
6. `index = head & mask`. `size = end - head`. When `size > capacity - index`, `size = capacity - index`.
7. Compare-and-swap `len` at `index` from `L` to `size`. On failure, nothing is recovered this time: the claim changed, and the next read sees it.
8. Store `type = 0xFFFFFFFF` at `index`. When `head + size == end`, store `NO_INTENT` into `at` of every remembered slot.
9. Read again. The claim is now a padding record, which the read skips and zeroes.

A claim that crossed the wrap point comes back one lap segment at a time. The first pass pads up to the end of the data region. The next stall is at index 0, inside the same intent, and the next pass pads the rest and clears the slots.

The compare-and-swap in step 7 is what keeps a live producer safe. A producer that commits after the load of `L` changes `len`, so the swap fails.

A watch on a producer signals the `.ne` event once when the producer exits. The parked consumer then wakes and runs the recovery again. A watch that fails makes every later receive that needs it report the failure.

A producer that dies after a failed swap in step 5 of the claim leaves a stale intent behind it. The intent stays in place until a slot acquire takes the slot. A later dead claim inside that range then reports corrupt, because the intents disagree. A queue must not mix unattributed claims with queue claims. A stale dead intent can cover an unattributed claim, and the recovery then pads over that live claim.

## Event

An event is a FIFO at `/dev/shm/go-ipc-{event}.event`, mode `0600`.

- **Create:** remove any file at the path, then `mkfifo` it. Open it.
- **Open:** open the path. Every handle is `O_RDWR | O_NONBLOCK | O_CLOEXEC`. Read-write mode keeps a writer attached, so a reader never sees end-of-file.
- **Signal n:** cap `n` at 4096. Write `n` bytes of any value in one non-blocking `write`. `EAGAIN` is success, because a full pipe already holds more wakeups than there are waiters. A signal never blocks.
- **Wait:** a waiter consumes exactly one byte. It blocks in the kernel (a poller, `poll`, or a blocking read) until the FIFO is readable, the event closes, or the timeout ends. `EAGAIN` on the read means that another waiter took the byte, so the waiter blocks again. A waiter that times out or sees a close has consumed nothing.
- **Close:** close every handle of this process. A waiter of this process reports closed.
- **Unlink:** remove the path. A missing file is not an error.

No implementation may spin, yield in a loop, or sleep for a guessed interval on any blocking path.

## Queue

A queue named `{name}` is a name file, an instance file, and an instance: a segment and its pair of events. The section "Paths" gives every path. The default capacity is 1048576.

**Lock the name.**

1. Open the name file with `O_RDWR | O_CREAT | O_CLOEXEC`, mode `0600`.
2. Take `flock(LOCK_EX | LOCK_NB)` on it. `EWOULDBLOCK` means that a live process holds the name: report in-use. Report any other error.
3. `fstat` the open file and `stat` the path. When the path is gone or names a different file (device and inode differ), close the file and go back to step 1. Another process removed or replaced the file between the open and the lock.

The holder keeps the file open. The lock lasts until the holder closes it or exits.

**Create** takes a name, a capacity and a consumer value:

1. Validate the name and the capacity.
2. Run the sweep, the first time this process creates a queue.
3. Lock the name.
4. Read the instance file. When it holds a valid id, remove that instance: unlink its segment and both of its events. A missing file is not an error. Any other failure ends the create.
5. Make a new instance id.
6. Create the `.ne` event, then the `.nf` event.
7. Create the segment: open it with `O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC` and mode `0600`. `ftruncate` it to `16896 + capacity`. Map it shared.
8. Init the ring with the consumer value. A queue passes its own procID. The creator-to-opener queue of a channel passes 1.
9. Publish the id: open the instance file with `O_RDWR | O_CREAT | O_CLOEXEC` and mode `0600`. Write the id at offset 0 in one `write`, then truncate the file to the id's length.

The id is published last, so an opener that reads it finds a complete instance. When a later step fails after the lock, release what the create made, remove the new instance, and release the lock. A create never reuses an instance, because the senders of an instance may still write to it.

**Open** takes a name:

1. Validate the name.
2. When the name file does not exist, report not-found.
3. Read the instance file. When it is missing or does not hold a valid id, report not-ready.
4. Open the segment with `O_RDWR`. Map it at its file size. Attach the ring. A missing segment is not-found.
5. Open the `.ne` event, then the `.nf` event.
6. Check the peer, as a send does. When the receiver is gone, close and report peer-gone.

An opener never takes the lock. The capacity comes from the segment.

A valid id is exactly the length in `instance_id_hex_digits`, and all of it is hexadecimal digits.

**Unlink** removes the segment and both events of this handle's instance. Then, while the instance file still holds this handle's id, it removes the instance file and then the name file. A newer instance under the same name keeps its files.

**Sweep.** A process sweeps the runtime directory once, at its first queue create, before it locks the name. A queue create inside a channel or a conn counts. The sweep looks at every entry of `/dev/shm`:

- `go-ipc-life-*.sock`: connect to it. When the connect fails with `ECONNREFUSED`, remove the file. Otherwise leave it, and close a connection that succeeded.
- `go-ipc-{name}.name` or `go-ipc-{name}.inc`, for a valid `{name}`: lock the name. When the lock fails for any reason, leave the name. While the sweep holds the lock, read the instance file. When it holds a valid id, remove that instance. When that removal fails, release the lock and leave the name. Remove the instance file, then the name file, then release the lock.
- Anything else: leave it. That includes segments, events and `.tmp` sockets.

The sweep reports no error. A creator that races the sweep on a stale name can get in-use. It succeeds when it tries again.

**Park.** A side that cannot proceed runs this sequence, with `waiters` being `recv_waiters` for the consumer and `send_waiters` for a producer:

1. Try. Return on success.
2. Atomically add 1 to `waiters`.
3. Try again. On success, subtract 1 and return.
4. Wait on the event. Subtract 1. On a wait error, return it. Otherwise go to step 1.

Each try of a send or a claim checks the peer first. Each try of a receive on a channel checks the channel peer first.

**Wake.** After a send, a commit or an abort, a producer loads `recv_waiters`. It signals `.ne` once when the value is above 0. A claim alone does not wake. After a read that moved `head`, the consumer loads `send_waiters` as `w`. It signals `.nf` with `n = w` when `w > 0`. A read that only stepped over padding still moved `head`, so it still wakes. A recovery that pads a dead claim moves `head` on the next read, and that read wakes.

**Receive.** Only the handle that created the queue receives. A receive on any other handle reports not-consumer. A handle runs its receives one at a time, so concurrent receives on it are safe.

### Peers

The receiver of a queue is the process in `consumer`. A send or a claim checks the receiver before each try:

| `consumer` | result |
| --- | --- |
| 0 | Report peer-gone. |
| 1 | Go on. The message waits in the ring for a channel peer. |
| this process's own procID | Go on. |
| a procID with bit 63 clear | Go on. Nothing can judge that process. |
| any other procID | See below. |

For any other procID `p`, the handle keeps one watched receiver:

1. When the handle already follows `p`: report peer-gone when the watch saw `p` exit, report the watch failure when the watch failed, and go on otherwise.
2. When `p` is new to the handle: cancel the watch on the earlier receiver. Run a check of `p`. When `p` is gone, remember that and report peer-gone. Otherwise start a watch on `p`, and go on. A watch that cannot start is reported as an error.

When the watch sees `p` exit, the handle marks the receiver gone. It then signals `.nf` with the current `send_waiters`, so every parked sender wakes, tries again, and finds peer-gone. A handle that sees a wait end with peer-gone keeps reporting peer-gone.

A plain queue whose receiver exits keeps the name file, because nobody removed it. An open of that name then finds the dead procID in `consumer`, and reports peer-gone.

### Close

**Close** of a queue handle runs these steps in order:

1. Mark the handle closing. A second close reports closed. Every later operation on the handle reports closed.
2. When the handle is the receiver, compare-and-swap `consumer` from its procID to 0. Signal `.nf` with the current `send_waiters`. Parked senders in every process wake and find peer-gone.
3. Cancel the receiver watch and every producer watch of the handle.
4. Close both events. Waiters of this process wake with closed.
5. Wait until no operation of this handle is inside the mapping. Every operation registers before it touches shared memory, and backs out when it sees the closing mark.
6. Give up every claim slot of the handle.
7. Unmap and close the segment.
8. When the handle created the queue, close the name file. That releases the lock.

Close removes no file. A claim must be committed or aborted before close, because it points into the mapping.

## Channel

A channel named `c` is a pair of queues. `c.c2o` carries messages from the creator to the opener. `c.o2c` carries them back.

**Create.** Validate the name. Create `c.c2o` with consumer 1. The creator does not receive on it. Then create `c.o2c` with the creator's procID as consumer. When the second create fails, close and unlink the first. The creator sends on `c.c2o` and receives on `c.o2c`.

**Open.**

1. Open `c.o2c` as a queue, with the peer check. A gone creator reports peer-gone.
2. Open `c.c2o` without the peer check.
3. Compare-and-swap `consumer` of `c.c2o` from 1 to the opener's procID. When the swap fails, close both and report in-use. The opener is now the receiver of `c.c2o`.
4. Signal `.ne` of `c.o2c` once. A creator parked in a receive wakes and starts to watch the opener.
5. Signal `.nf` of `c.c2o` with its `send_waiters`.

A channel connects once. A peer that exited leaves its procID in `consumer`. As a result, a later opener gets in-use.

**Peer.** The peer of a channel side is the receiver of its outbound queue. A send checks it as a queue send does. A receive also checks it, before each read. When the read delivers nothing and the check found the peer gone, the receive reports peer-gone instead of empty. So every message that the peer sent before it closed or exited is delivered first. When the watch on the peer sees it exit, it also signals the inbound `.ne` once, so a parked receive wakes.

**Close.** Close the inbound queue first. Signal `.ne` of the outbound queue once, so a peer parked in a receive wakes and finds this side gone. Then close the outbound queue.

## Conn

A conn is a byte stream over a channel. `Listen` creates the channel. `Dial` opens it.

- A write splits its input into messages of at most the maximum message size, with type `0`.
- A read returns bytes from the current message, then receives the next one. A message of type `1` is end-of-stream. Every read after it reports end-of-stream.
- CloseWrite sends one type `1` message with an empty payload through a blocking send. Later writes fail as closed. Reads go on.
- Close sends that message best effort and non-blocking, unless CloseWrite already sent it. It then closes the channel.
- A read after the peer exited or closed without an end-of-stream message reports peer-gone, once every byte the peer sent is read.

## Errors

A conforming implementation reports each of these conditions as its own error. The Go name is in brackets.

| error | reported when |
| --- | --- |
| closed (`ErrClosed`) | An operation runs on a closed handle. A second close. A wait on an event that this process closed. |
| full (`ErrFull`) | A non-blocking send or claim finds no room. |
| empty (`ErrEmpty`) | A non-blocking receive finds no record ready. |
| too-large (`ErrMessageTooLarge`) | A payload is negative or above the largest payload. |
| reserved-type (`ErrReservedType`) | A send or claim uses the type `0xFFFFFFFF`. |
| invalid-name (`ErrInvalidName`) | A name breaks the rules in "Names". |
| invalid-capacity (`ErrInvalidCapacity`) | A create asks for a capacity that is not a power of 2, or is below 4096. |
| too-small (`ErrTooSmall`) | A ring buffer is shorter than `16896 + 4096` bytes. |
| unaligned (`ErrUnaligned`) | A ring buffer does not start on an 8-byte boundary. |
| bad-layout (`ErrBadLayout`) | Attach finds a wrong `magic`, `version` or `capacity`. |
| corrupt (`ErrCorrupt`) | A read finds an impossible length. Dead producers disagree on a claim. |
| peer-gone (`ErrPeerGone`) | `consumer` is 0. The watched receiver exited. An open finds a gone receiver or a gone channel creator. A channel receive finds nothing left from a gone peer. |
| in-use (`ErrInUse`) | A create finds the name locked. A channel open finds a peer already connected. |
| not-consumer (`ErrNotConsumer`) | A receive runs on a handle that is not the receiver. |
| too-many-claims (`ErrTooManyClaims`) | A claim finds no slot to acquire. |
| not-found (`fs.ErrNotExist`) | An open finds no name file, or no segment. |
| not-ready (Go wraps a private error) | An open finds a name file but no valid instance id. |

A wait or a blocking operation that its caller cancels, or that runs out of time, reports that in the language's own way. A failed system call reports its `errno`. A failed watch reports why it failed.

## Conformance

An implementation conforms when it passes checks:

1. Its constants match `wire.json`.
2. It replays every case in `vectors/ring/manifest.json` on a zeroed buffer and produces the matching `.bin` byte for byte. It also attaches to each `.bin`, finds the listed `consumer`, `head` and `tail`, and reads exactly the expected records.
3. It passes the cross-language matrix in `interop/` against every other implementation, in both directions. That matrix covers dead-producer recovery, a receiver that closes or exits, and a channel peer that closes or exits.
