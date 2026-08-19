# tephra-go

A Go client for a [tephra](https://github.com/tqwewe/tephra) event store, speaking its
length-prefixed protobuf-over-TCP protocol. It is wire-compatible with `tephra-server` and mirrors
the design of the reference Rust `tephra-client`: a single, concurrent-safe `Client` that
multiplexes many requests over a control socket plus a pool of bulk read sockets.

```sh
go get github.com/tephradb/tephra-go
```

Requires Go 1.23+ (uses `iter.Seq2`); developed against Go 1.26.

## Quick start

```go
ctx := context.Background()

client, err := tephra.Dial(ctx, "127.0.0.1:9000")
if err != nil {
    log.Fatal(err)
}
defer client.Close()

event, err := tephra.NewEvent("Enrolled", []string{"course:c1", "student:s1"}, []byte(`{}`))
if err != nil {
    log.Fatal(err)
}
if _, err := client.Append(ctx, []tephra.Event{event}, nil); err != nil {
    log.Fatal(err)
}

events, watermark, err := client.ReadAll(ctx, tephra.QueryAll(), tephra.Zero, nil)
```

## Concepts

- **Event**: a type, a set of tags, and an opaque payload. Build one with `NewEvent`, which
  validates the type and tags (non-empty, at most 65535 bytes each, no duplicate tags) exactly as
  the server does.
- **Position**: a dense, 1-based global order. `Zero` is before everything (the start cursor);
  `Max` is the "from the tip" cursor for a backward read.
- **Query**: `QueryAll()` matches everything; `QueryItems(items...)` OR's items, where each item
  AND's its tags and OR's its types (an empty item set matches nothing, distinct from the
  catch-all). Build items with `OfTypes`, `WithTags`, or `NewQueryItem`.
- **AppendCondition**: a dynamic consistency boundary. Reject the append if any event after
  `After` matches the query. `After == Zero` considers the whole log.

## Reads and pagination

`Read` returns a `ReadStream`; drive it with `Next`/`Event`, check `Err`, and read `Watermark`
once it ends. `ReadAll` drains one into a slice. `ReadBack`/`ReadAllBack` are the newest-first
duals.

`after` (exclusive) and `limit` compose into a stateless pagination cursor: read a page, then read
again with `after` set to the last position returned:

```go
page, _, err := client.ReadAll(ctx, query, cursor, tephra.Limit(100))
// ... process page ...
if n := len(page); n > 0 {
    cursor = page[n-1].Position // next page starts here, no gap or duplicate
}
```

A range-over-func helper is also provided:

```go
for event, err := range client.ReadSeq(ctx, tephra.QueryAll(), tephra.Zero, nil) {
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(event.Position, event.Type())
}
```

## Subscriptions

`Subscribe` catches up on matching events, then tails new ones live, delivering a caught-up marker
each time it reaches the live edge:

```go
sub, err := client.Subscribe(ctx, tephra.QueryAll(), tephra.Zero)
if err != nil {
    log.Fatal(err)
}
defer sub.Close()

for sub.Next() {
    item := sub.Item()
    if item.IsCaughtUp() {
        continue
    }
    handle(item.Event)
}
```

Cancel a stream by calling `Close`, or by cancelling the `context.Context` passed to `Read` /
`Subscribe`. Either sends a best-effort cancel to the server so it stops producing frames.

## Errors

- `*ServerError`: the server returned an error. `Code` is an `ErrorCode`; `Retryable` marks an
  advisory same-batch append conflict (safe to retry); `ConflictPosition` is set for a durable
  append conflict. Match with `errors.As`.
- `*ProtocolError`: the peer sent something outside the protocol.
- `*ConnError`: the connection failed with requests in flight; every in-flight request is failed
  with it (never left hanging). Unwraps to the underlying cause.
- `ErrClosed`: the client was closed.

The client does no automatic retries or reconnection; it surfaces the error and leaves policy to
you.

## Configuration and design

`Dial` takes options; the defaults mirror the reference Rust client:

| Option | Default | Meaning |
| --- | --- | --- |
| `WithBulkConnections(n)` | 4 | Dedicated bulk sockets for reads/subscriptions. `0` folds reads onto the control socket. |
| `WithMaxInflightRequests(n)` | 1024 | Outstanding requests per socket before backpressure. |
| `WithRequestQueueDepth(n)` | 256 | Outbound queue depth per socket. |
| `WithMaxFrameLen(n)` | 16 MiB | Largest frame accepted or produced. |
| `WithDialer(d)` | n/a | Custom `net.Dialer`. `TCP_NODELAY` is always set. |
| `WithTLS(cfg)` | off | Wrap each connection in a TLS client session (implicit TLS). `ServerName` defaults to the dial host; pass `RootCAs`, client `Certificates` (mTLS), or `MinVersion` via the config. |

A `Client` is safe for concurrent use. Internally each socket runs a reader goroutine (which
demultiplexes responses by request id) and a writer goroutine (which coalesces queued frames into
one flush per burst). Appends, stats, and cancels ride the **control** socket; reads and
subscriptions round-robin across the **bulk** pool. Splitting the lanes keeps a large read response
from delaying a small append (head-of-line blocking), and each stream buffers its frames
unboundedly so a slow consumer never stalls the shared socket; backpressure comes instead from the
per-socket in-flight budget.

### TLS

The tephra server can serve implicit TLS (TLS 1.3, server-authenticated). Enable it on the client
with `WithTLS`, passing a standard `*tls.Config`:

```go
import "crypto/tls"

// Verify against the system roots (public CA):
client, err := tephra.Dial(ctx, "tephra.example.com:9000", tephra.WithTLS(&tls.Config{}))

// Or trust a private CA, and/or present a client certificate for mutual TLS:
client, err = tephra.Dial(ctx, "tephra.internal:9000", tephra.WithTLS(&tls.Config{
    RootCAs:      privateCAs,
    Certificates: []tls.Certificate{clientCert},
}))
```

`ServerName` defaults to the host in the dial address, so verifying a hostname certificate needs no
extra configuration. The TLS session is established before the first frame; the wire protocol is
unchanged, so everything else behaves identically to a plaintext connection.

## Development

The wire types are generated from `proto/tephra/v1/tephra.proto` into `internal/tephrapb`
(committed, so consumers need no toolchain). To regenerate (requires `protoc` and `protoc-gen-go`,
both provided by the devenv shell):

```sh
go generate ./...
```

Run the tests:

```sh
go test -race ./...                    # unit tests (no server needed)
go test -tags integration -timeout 300s ./...   # integration: builds and runs tephra-server
```

The integration tests build `tephra-server` from a sibling `../tephra` checkout (override with
`TEPHRA_REPO`, or point `TEPHRA_SERVER_BIN` at a prebuilt binary).

## License

Apache-2.0.
