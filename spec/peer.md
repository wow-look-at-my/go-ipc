# Peer contract

Every implementation ships a peer program. The cross-language suite in `interop/` runs peers in different languages against each other. A peer takes a role and its arguments:

```
peer <role> <args...>
```

A peer exits 0 on success. On any failure it prints the reason to stderr and exits 1. Each role gives up and fails after one minute. A role that creates an endpoint prints `ready` and a newline to stdout, and flushes, once the endpoint exists. The suite waits for that line before it starts the other side.

| role | arguments | behavior |
| --- | --- | --- |
| `recv` | `<name> <total> <capacity>` | Create queue `name` with `capacity`. Print `ready`. Receive `total` messages. Each payload is `<sender>:<seq>` in ASCII decimal, and the type equals `seq`. For each sender, `seq` must run 0, 1, 2 and so on with no gap. Print `ok <total>`. Close and unlink. |
| `send` | `<name> <sender> <count>` | Open queue `name`. Send `count` messages with type `i` and payload `<sender>:<i>`, for `i` from 0. Close. |
| `listen-echo` | `<name> <capacity>` | Listen on `name` with `capacity`. Print `ready`. Copy every byte read back to the writer until end-of-stream. Close, which sends end-of-stream back. Unlink. |
| `dial-check` | `<name> <bytes>` | Dial `name`. Write `bytes` bytes, where byte `i` is `(i * 31 + 7) mod 256`, then CloseWrite. At the same time, read until end-of-stream. Require exactly `bytes` bytes back, equal to what was written. Close. |
| `typed-send` | `<name>` | Open queue `name`. For each entry `i` of `spec/vectors/schema/values.json`, in order: build the value as the language's generated type, encode it with the generated encoder, and send it with the message's generated type ID as the queue record type. Close. |
| `typed-recv` | `<name> <capacity>` | Create queue `name` with `capacity`. Print `ready`. Receive one message per entry of `values.json`. For entry `i`, the record type must equal that message's type ID. Decode the payload with the generated decoder that the type ID selects, then encode the decoded value again. The bytes must equal `spec/vectors/schema/<i>.bin`. Print `ok <count>`, where `count` is the number of entries. Close and unlink. |
| `claim-and-die` | `<name> <length>` | Open queue `name`. Claim `length` payload bytes with type 1 through the queue's blocking claim, so that a claim slot names this process as the owner. Fill the payload with `0xAB`. Then exit at once with status 0. Do not commit, abort or close, and run no other cleanup, as `_exit(0)` does. Print nothing. |
| `send-until-gone` | `<name> <sender>` | Open queue `name`. Send messages with type `i` and payload `<sender>:<i>`, for `i` from 0, through the blocking send. Stop at the first send that reports peer-gone. Print `gone <n>`, where `n` is the number of sends that succeeded. Close. Any other error, peer-gone from the open included, is a failure. |
| `recv-then-stop` | `<name> <count> <capacity> <mode>` | Create queue `name` with `capacity`. Print `ready`. Receive `count` messages, checked as `recv` checks them. Print `ok <count>`. With mode `close`: close the queue, unlink it, and exit 0. With mode `exit`: unlink the queue, then exit at once with status 0, without a close, as `_exit(0)` does. |
| `chan-recv-until-gone` | `<name> <capacity> <count>` | Create channel `name` with `capacity`. Print `ready`. Receive until a receive reports peer-gone. Message `i` must have type `i` and payload `0:<i>`. Require exactly `count` messages. Then require that a send on the channel reports peer-gone. Print `ok <count>`. Close and unlink. |
| `chan-send-then-stop` | `<name> <count> <mode>` | Open channel `name`. Send `count` messages, message `i` with type `i` and payload `0:<i>`. With mode `close`: close the channel and exit 0. With mode `exit`: exit at once with status 0, without a close. |

| `claim-and-die` | `<name> <length>` | Open queue `name`. Claim `length` payload bytes with type 1 through the queue's blocking claim, so that a claim slot names this process as the owner. Fill the payload with `0xAB`. Then exit at once with status 0. Do not commit, abort or close, and run no other cleanup, as `_exit(0)` does. Print nothing. | | `send-until-gone` | `<name> <sender>` | Open queue `name`. Send messages with type `i` and payload `<sender>:<i>`, for `i` from 0, through the blocking send. Stop at the first send that reports peer-gone. Print `gone <n>`, where `n` is the number of sends that succeeded. Close. Any other error, peer-gone from the open included, is a failure. | | `recv-then-stop` | `<name> <count> <capacity> <mode>` | Create queue `name` with `capacity`. Print `ready`. Receive `count` messages, checked as `recv` checks them. Print `ok <count>`. With mode `close`: close the queue, unlink it, and exit 0. With mode `exit`: unlink the queue, then exit at once with status 0, without a close, as `_exit(0)` does. | | `chan-recv-until-gone` | `<name> <capacity> <count>` | Create channel `name` with `capacity`. Print `ready`. Receive until a receive reports peer-gone. Message `i` must have type `i` and payload `0:<i>`. Require exactly `count` messages. Then require that a send on the channel reports peer-gone. Print `ok <count>`. Close and unlink. | | `chan-send-then-stop` | `<name> <count> <mode>` | Open channel `name`. Send `count` messages, message `i` with type `i` and payload `0:<i>`. With mode `close`: close the channel and exit 0. With mode `exit`: exit at once with status 0, without a close. |

`dial-check` writes and reads at the same time. The ring holds less than the stream, so a peer that writes everything before it reads deadlocks.

An exit "at once" skips every exit handler, destructor and close. The roles that use it stand in for a process that dies. Its life socket closes with it. That is the only signal its peers get.

The suite runs these roles in the following cells, for every ordered pair of languages A and B. Each cell uses a capacity of 4096.

| cell | sequence |
| --- | --- |
| recover | `recv` in B for `2k` messages. Then, one after another, each to its exit: `claim-and-die` in A with length 64, `send` in A as sender 0 with `k` messages, `claim-and-die` in A with length 1500, `send` in B as sender 1 with `k` messages. B prints `ok <2k>`. |
| receiver gone | `recv-then-stop` in A for `k` messages, in mode `close` and in mode `exit`. `send-until-gone` in B. A prints `ok <k>`. B prints `gone <n>` with `n` of at least `k`. |
| channel peer gone | `chan-recv-until-gone` in A for `k` messages. `chan-send-then-stop` in B, in mode `close` and in mode `exit`. A prints `ok <k>`. |

The recover cell places its claims at fixed cursors, whatever the timing:

| step | ring bytes | `tail` after it |
| --- | --- | --- |
| `claim-and-die`, length 64 | 72 | 72 |
| `send` sender 0, `k` = 200 messages of 16 bytes each | 3200 | 3272 |
| `claim-and-die`, length 1500 | 824 of padding, then 1512 at index 0 | 5608 |

So the second dead claim always crosses the wrap point. The receiver must pad both dead claims and deliver every message.

The `typed-*` roles find `values.json` through the `GOIPC_SPEC_DIR` environment variable, which names the `spec` directory.
