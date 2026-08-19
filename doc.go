// Package tephra is a Go client for a tephra event store, speaking its length-prefixed
// protobuf-over-TCP protocol.
//
// Tephra is a single-writer, append-only event store with dynamic consistency boundaries. An
// event carries a type, a set of tags, and an opaque payload, and is assigned a dense, 1-based
// global Position. A [Query] selects events (types OR'd, tags AND'd within an item, items OR'd),
// and the same query shape guards an append via [AppendCondition].
//
// # Client
//
// [Dial] returns a [Client] that is safe for concurrent use from many goroutines. Internally it
// multiplexes requests over a control socket (appends, stats, cancels) and a pool of bulk sockets
// (reads, subscriptions), correlating each response to its request. Splitting the lanes keeps a
// large read response from delaying a small append, and the bulk pool keeps concurrent reads from
// serializing on one connection. Tune the pool and budgets with the [Option] values; the defaults
// mirror the reference Rust client (16 MiB frames, 1024 in-flight requests per socket, 4 bulk
// sockets).
//
// Every socket opens with a mandatory Hello handshake that negotiates the protocol version and,
// for a server that requires authentication, presents a bearer token supplied via [WithAuthToken];
// a version mismatch or a rejected token fails [Dial]. Pair it with [WithTLS] so the token is not
// sent in the clear.
//
// Every operation takes a context.Context for cancellation and deadlines. The client performs no
// automatic retries or reconnection: on a durable failure it surfaces the error (a [ServerError]
// carries the server's code and a Retryable hint), leaving policy to the caller.
//
// # Reads and subscriptions
//
//	client, err := tephra.Dial(ctx, "127.0.0.1:9000")
//	if err != nil { /* ... */ }
//	defer client.Close()
//
//	events, watermark, err := client.ReadAll(ctx, tephra.QueryAll(), tephra.Zero, nil)
//
// A [ReadStream] (from [Client.Read]) yields events until a terminating watermark; a
// [SubscribeStream] (from [Client.Subscribe]) yields events and re-armed caught-up markers
// indefinitely. Drive either with Next, then check Err, and Close it when done. Closing a
// partially consumed stream cancels it server-side.
package tephra
