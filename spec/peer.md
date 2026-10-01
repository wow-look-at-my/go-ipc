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

`dial-check` writes and reads at the same time. The ring holds less than the stream, so a peer that writes everything before it reads deadlocks.

The `typed-*` roles find `values.json` through the `GOIPC_SPEC_DIR` environment variable, which names the `spec` directory.
