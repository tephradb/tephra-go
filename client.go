package tephra

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"iter"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tephradb/tephra-go/internal/tephrapb"
)

// unattributedRequestID is the request id the server uses for an error it cannot attribute to a
// specific request (a frame it rejected before decode). Client request ids start at 1, so this
// sentinel never collides with a real one; the reader captures such an error as the failure
// reason for the connection.
const unattributedRequestID = 0

// Default tuning, matching the Rust AsyncClient (tephra-client AsyncClientConfig).
const (
	defaultRequestQueueDepth   = 256
	defaultMaxInflightRequests = 1024
	defaultBulkConnections     = 4
)

// config is the resolved tuning for a Client, built from options.
type config struct {
	maxFrameLen         uint32
	requestQueueDepth   int
	maxInflightRequests int
	bulkConnections     int
	dialer              *net.Dialer
	tlsConfig           *tls.Config
	authToken           string
}

func defaultConfig() config {
	return config{
		maxFrameLen:         DefaultMaxFrameLen,
		requestQueueDepth:   defaultRequestQueueDepth,
		maxInflightRequests: defaultMaxInflightRequests,
		bulkConnections:     defaultBulkConnections,
	}
}

func (c *config) normalize() {
	if c.maxFrameLen == 0 {
		c.maxFrameLen = DefaultMaxFrameLen
	}
	if c.requestQueueDepth < 1 {
		c.requestQueueDepth = 1
	}
	if c.maxInflightRequests < 1 {
		c.maxInflightRequests = 1
	}
	if c.bulkConnections < 0 {
		c.bulkConnections = 0
	}
}

// An Option configures a Client at Dial time.
type Option func(*config)

// WithMaxFrameLen sets the largest single frame accepted or produced (default DefaultMaxFrameLen,
// 16 MiB). It must match or exceed the server's limit, or a large read batch is rejected.
func WithMaxFrameLen(n uint32) Option { return func(c *config) { c.maxFrameLen = n } }

// WithRequestQueueDepth sets the depth of each socket's outbound request queue (default 256). Once
// full, an operation awaits room to send, bounding how far a fast producer outruns a slow socket.
func WithRequestQueueDepth(n int) Option { return func(c *config) { c.requestQueueDepth = n } }

// WithMaxInflightRequests sets the most requests that may be outstanding (sent but not fully
// answered) at once per socket (default 1024). At the limit, an operation awaits a free slot
// (backpressure). Many requests still run concurrently; only the total in flight is bounded.
func WithMaxInflightRequests(n int) Option { return func(c *config) { c.maxInflightRequests = n } }

// WithBulkConnections sets the number of dedicated bulk sockets carrying streaming reads and
// subscriptions, separate from the control socket carrying appends, stats, and cancels (default
// 4). Fanning reads across a small pool keeps them from serializing on one byte stream and keeps a
// large read response off the control lane (head-of-line blocking). 0 folds reads onto the control
// socket (the legacy hazard); 1 is a control/bulk split with no read fan-out.
func WithBulkConnections(n int) Option { return func(c *config) { c.bulkConnections = n } }

// WithDialer sets the net.Dialer used to establish each connection (for a custom timeout,
// keep-alive, or local address). TCP_NODELAY is always enabled regardless.
func WithDialer(d *net.Dialer) Option { return func(c *config) { c.dialer = d } }

// WithTLS makes every connection a TLS client session over the TCP socket (the tephra server
// serves implicit TLS: the session is established before the first frame). The given config is
// used as-is except that, if its ServerName is empty, it defaults to the host part of the dial
// address for certificate verification. Pass RootCAs to trust a private CA, Certificates for
// mutual TLS, or a stricter MinVersion; the server currently requires TLS 1.3. Passing nil, or
// omitting this option, uses a plaintext connection.
func WithTLS(cfg *tls.Config) Option { return func(c *config) { c.tlsConfig = cfg } }

// WithAuthToken sets the bearer token presented in each socket's opening Hello, for a server that
// requires authentication. Every socket (control and bulk) authenticates with it independently, so
// a rejected token fails Dial (a ServerError with ErrCodeUnauthenticated) rather than the first
// request. An empty token (the default) connects unauthenticated, which the server accepts only
// when it has no tokens configured. The token is sent regardless of transport; the server, not the
// client, enforces any TLS requirement, so pair this with WithTLS to avoid sending it in the clear.
func WithAuthToken(token string) Option { return func(c *config) { c.authToken = token } }

// Client is a multiplexing, concurrent-safe client for a tephra event store. It opens a control
// socket for appends, stats, and cancels plus a pool of bulk sockets for streaming reads and
// subscriptions (see WithBulkConnections). Many operations may run concurrently from different
// goroutines; each is correlated to its response by a request id. A Client must be closed with
// Close when no longer needed.
type Client struct {
	control   *conn
	bulkConns []*conn
	nextBulk  atomic.Uint64
	closeOnce sync.Once
}

// Dial connects to a tephra server at addr (host:port) and returns a ready Client. It opens the
// control socket plus WithBulkConnections bulk sockets (dialed concurrently); if any fails, all
// are closed and the error is returned. ctx bounds the dial only, not later operations.
func Dial(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.normalize()

	control, err := dialConn(ctx, addr, cfg)
	if err != nil {
		return nil, fmt.Errorf("tephra: dial control socket: %w", err)
	}

	bulk := make([]*conn, cfg.bulkConnections)
	errs := make([]error, cfg.bulkConnections)
	var wg sync.WaitGroup
	for i := range bulk {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bulk[i], errs[i] = dialConn(ctx, addr, cfg)
		}(i)
	}
	wg.Wait()
	for i, dialErr := range errs {
		if dialErr != nil {
			control.shutdown(ErrClosed)
			for _, b := range bulk {
				if b != nil {
					b.shutdown(ErrClosed)
				}
			}
			return nil, fmt.Errorf("tephra: dial bulk socket %d: %w", i, dialErr)
		}
	}

	return &Client{control: control, bulkConns: bulk}, nil
}

// Close tears down every connection and fails any in-flight request with ErrClosed. It is
// idempotent and safe to call from any goroutine.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.control.shutdown(ErrClosed)
		for _, b := range c.bulkConns {
			b.shutdown(ErrClosed)
		}
	})
	return nil
}

// pickBulk returns the connection a streaming read or subscription runs on: round-robin across the
// bulk pool, or the control connection when no bulk sockets are configured.
func (c *Client) pickBulk() *conn {
	if len(c.bulkConns) == 0 {
		return c.control
	}
	i := c.nextBulk.Add(1) - 1
	return c.bulkConns[int(i%uint64(len(c.bulkConns)))]
}

// Append appends events as one atomic batch, optionally guarded by cond, and returns the position
// range the batch was assigned. It blocks until the server replies or ctx is done. Many appends
// may run concurrently, bounded by WithMaxInflightRequests.
func (c *Client) Append(ctx context.Context, events []Event, cond *AppendCondition) (AppendResult, error) {
	ap := &tephrapb.AppendRequest{Events: make([]*tephrapb.Event, len(events))}
	for i := range events {
		ap.Events[i] = events[i].toPB()
	}
	if cond != nil {
		ap.Condition = cond.toPB()
	}
	req := &tephrapb.Request{Kind: &tephrapb.Request_Append{Append: ap}}

	resp, err := c.control.roundTrip(ctx, req, sinkAppend)
	if err != nil {
		return AppendResult{}, err
	}
	switch k := resp.GetKind().(type) {
	case *tephrapb.Response_Append:
		return AppendResult{First: Position(k.Append.GetFirst()), Last: Position(k.Append.GetLast())}, nil
	case *tephrapb.Response_Error:
		return AppendResult{}, serverErrorFromPB(k.Error)
	default:
		return AppendResult{}, protocolErrorf("unexpected response to append: %T", k)
	}
}

// Stats fetches a snapshot of the server's operational state. It blocks until the server replies
// or ctx is done.
func (c *Client) Stats(ctx context.Context) (Stats, error) {
	req := &tephrapb.Request{Kind: &tephrapb.Request_Stats{Stats: &tephrapb.StatsRequest{}}}
	resp, err := c.control.roundTrip(ctx, req, sinkStats)
	if err != nil {
		return Stats{}, err
	}
	switch k := resp.GetKind().(type) {
	case *tephrapb.Response_Stats:
		return statsFromPB(k.Stats), nil
	case *tephrapb.Response_Error:
		return Stats{}, serverErrorFromPB(k.Error)
	default:
		return Stats{}, protocolErrorf("unexpected response to stats: %T", k)
	}
}

// Read starts a forward read, returning a ReadStream over the matching events in ascending
// position order. after is an exclusive lower bound (Zero reads from the start); limit caps the
// matched events (nil = unlimited). Combined, after and limit form a stateless pagination cursor.
// Close the stream when done; a partially consumed read is cancelled server-side on Close.
func (c *Client) Read(ctx context.Context, query Query, after Position, limit *uint64) (*ReadStream, error) {
	return c.startRead(ctx, query, after, false, limit)
}

// ReadBack is the newest-first dual of Read: a ReadStream over matching events in descending
// position order, strictly before before (an exclusive upper bound). ReadBack(ctx, q, Max, limit)
// streams from the tip. limit caps the events from the tip down.
func (c *Client) ReadBack(ctx context.Context, query Query, before Position, limit *uint64) (*ReadStream, error) {
	return c.startRead(ctx, query, before, true, limit)
}

func (c *Client) startRead(ctx context.Context, query Query, cursor Position, reverse bool, limit *uint64) (*ReadStream, error) {
	rr := &tephrapb.ReadRequest{Query: query.toPB(), After: uint64(cursor), Reverse: reverse}
	if limit != nil {
		l := *limit
		rr.Limit = &l
	}
	req := &tephrapb.Request{Kind: &tephrapb.Request_Read{Read: rr}}

	cn := c.pickBulk()
	rs := &ReadStream{streamBase: streamBase[SequencedEvent]{cn: cn, ctx: ctx, st: newStreamState[SequencedEvent]()}}
	id, err := cn.startStream(ctx, req, &sink{kind: sinkRead, delivery: rs})
	if err != nil {
		return nil, err
	}
	rs.id = id
	return rs, nil
}

// ReadAll drains a forward read fully into a slice, returning the events and the watermark the
// read was pinned to. See Read for the after and limit semantics.
func (c *Client) ReadAll(ctx context.Context, query Query, after Position, limit *uint64) ([]SequencedEvent, Position, error) {
	rs, err := c.Read(ctx, query, after, limit)
	if err != nil {
		return nil, 0, err
	}
	return drainRead(rs)
}

// ReadAllBack is the newest-first dual of ReadAll (descending by position). See ReadBack.
func (c *Client) ReadAllBack(ctx context.Context, query Query, before Position, limit *uint64) ([]SequencedEvent, Position, error) {
	rs, err := c.ReadBack(ctx, query, before, limit)
	if err != nil {
		return nil, 0, err
	}
	return drainRead(rs)
}

// ReadSeq is a range-over-func convenience for a forward read: it yields each event with a nil
// error, or a single zero event with a non-nil error if the read fails. The underlying stream is
// closed when iteration ends (including an early break).
func (c *Client) ReadSeq(ctx context.Context, query Query, after Position, limit *uint64) iter.Seq2[SequencedEvent, error] {
	return func(yield func(SequencedEvent, error) bool) {
		rs, err := c.Read(ctx, query, after, limit)
		if err != nil {
			yield(SequencedEvent{}, err)
			return
		}
		defer rs.Close()
		for rs.Next() {
			if !yield(rs.Event(), nil) {
				return
			}
		}
		if err := rs.Err(); err != nil {
			yield(SequencedEvent{}, err)
		}
	}
}

// Subscribe opens a live subscription over query, resuming strictly after after: it streams all
// matching events already durable, then tails new ones as they are committed, delivering a
// SubEventCaughtUp marker each time it reaches the live edge. The subscription runs until Close,
// ctx cancellation, an error, or the connection closing. Close the stream when done.
func (c *Client) Subscribe(ctx context.Context, query Query, after Position) (*SubscribeStream, error) {
	sr := &tephrapb.SubscribeRequest{Query: query.toPB(), After: uint64(after)}
	req := &tephrapb.Request{Kind: &tephrapb.Request_Subscribe{Subscribe: sr}}

	cn := c.pickBulk()
	ss := &SubscribeStream{streamBase: streamBase[SubEvent]{cn: cn, ctx: ctx, st: newStreamState[SubEvent]()}}
	id, err := cn.startStream(ctx, req, &sink{kind: sinkSubscribe, delivery: ss})
	if err != nil {
		return nil, err
	}
	ss.id = id
	return ss, nil
}

// ---------------------------------------------------------------------------
// Connection actor
// ---------------------------------------------------------------------------

// sinkKind identifies how a pending request's responses are delivered.
type sinkKind int

const (
	sinkAppend sinkKind = iota
	sinkStats
	sinkRead
	sinkSubscribe
)

// reply carries a one-shot response (append/stats) to the waiting caller: either the response
// frame or a connection-level error.
type reply struct {
	resp *tephrapb.Response
	err  error
}

// sink is where the reader delivers a pending request's responses. A one-shot request (append,
// stats) uses once; a streaming request (read, subscribe) uses delivery.
type sink struct {
	kind     sinkKind
	once     chan reply
	delivery streamDelivery
}

func (s *sink) fail(err error) {
	switch s.kind {
	case sinkAppend, sinkStats:
		// Buffered(1) and the reader is the only other sender, so this never blocks.
		select {
		case s.once <- reply{err: err}:
		default:
		}
	default:
		s.delivery.deliverError(err)
	}
}

// conn is one multiplexed socket: a request registry plus a reader and a writer goroutine.
type conn struct {
	netConn net.Conn
	cfg     config

	idCounter atomic.Uint64

	mu          sync.Mutex
	pending     map[uint64]*sink
	closed      bool
	closeReason error

	inflight chan struct{}          // in-flight budget: a slot per outstanding request
	outbound chan *tephrapb.Request // writer queue
	dead     chan struct{}          // closed once the connection is torn down

	closeOnce sync.Once
}

func dialConn(ctx context.Context, addr string, cfg config) (*conn, error) {
	var dialer net.Dialer
	if cfg.dialer != nil {
		dialer = *cfg.dialer
	}
	netConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	if tcp, ok := netConn.(*net.TCPConn); ok {
		// Set on the raw socket before any TLS wrapping; a *tls.Conn is not a *net.TCPConn.
		_ = tcp.SetNoDelay(true)
	}
	if cfg.tlsConfig != nil {
		netConn, err = tlsClient(ctx, netConn, addr, cfg.tlsConfig)
		if err != nil {
			return nil, err
		}
	}
	cn := &conn{
		netConn:  netConn,
		cfg:      cfg,
		pending:  make(map[uint64]*sink),
		inflight: make(chan struct{}, cfg.maxInflightRequests),
		outbound: make(chan *tephrapb.Request, cfg.requestQueueDepth),
		dead:     make(chan struct{}),
	}
	// Complete the mandatory opening Hello synchronously, before the reader/writer actor tasks
	// start, so a version mismatch or a rejected token fails the dial and the reply never reaches
	// the request registry.
	if err := cn.handshake(ctx); err != nil {
		_ = netConn.Close()
		return nil, err
	}
	go cn.readLoop()
	go cn.writeLoop()
	return cn, nil
}

// handshake sends the mandatory opening Hello and awaits the server's HelloAck. It runs directly on
// the raw connection before the reader/writer goroutines start, writing and reading exactly one
// frame apiece (no buffering), so the actor's bufio reader/writer begin frame-aligned. A dial
// deadline on ctx bounds it, matching Dial's "ctx bounds the dial only" contract.
func (cn *conn) handshake(ctx context.Context) error {
	if dl, ok := ctx.Deadline(); ok {
		_ = cn.netConn.SetDeadline(dl)
		defer cn.netConn.SetDeadline(time.Time{})
	}
	// The Hello borrows the first id; its reply is consumed here and never registered.
	req := helloRequest(cn.nextID(), cn.cfg.authToken)
	if err := writeFrame(cn.netConn, req, cn.cfg.maxFrameLen); err != nil {
		return err
	}
	var resp tephrapb.Response
	if err := readFrame(cn.netConn, cn.cfg.maxFrameLen, &resp); err != nil {
		if errors.Is(err, io.EOF) {
			// A clean close before the ack is a torn handshake, not an orderly end-of-stream.
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	return verifyHelloAck(&resp)
}

// tlsClient wraps conn in a TLS client session and completes the handshake under ctx. When the
// config has no ServerName it defaults to the host part of addr, so verifying a certificate while
// dialing by hostname works without the caller restating the name.
func tlsClient(ctx context.Context, conn net.Conn, addr string, cfg *tls.Config) (net.Conn, error) {
	if cfg.ServerName == "" {
		if host, _, err := net.SplitHostPort(addr); err == nil {
			cfg = cfg.Clone()
			cfg.ServerName = host
		}
	}
	tlsConn := tls.Client(conn, cfg)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		conn.Close()
		return nil, err
	}
	return tlsConn, nil
}

func (cn *conn) nextID() uint64 { return cn.idCounter.Add(1) }

// acquire takes an in-flight slot, awaiting one when the budget is exhausted (backpressure). It
// returns early if ctx is done or the connection dies.
func (cn *conn) acquire(ctx context.Context) error {
	select {
	case cn.inflight <- struct{}{}:
		return nil
	case <-cn.dead:
		return cn.reason()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (cn *conn) release() { <-cn.inflight }

// register adds a pending request, failing if the connection has closed.
func (cn *conn) register(id uint64, s *sink) error {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	if cn.closed {
		return cn.closeReasonLocked()
	}
	cn.pending[id] = s
	return nil
}

// finalize removes a pending request and releases its in-flight slot, returning the sink if it was
// still registered (nil if already removed or the connection is closed). It is the single place a
// slot is released, so a slot is released exactly once per request.
func (cn *conn) finalize(id uint64) *sink {
	cn.mu.Lock()
	s, ok := cn.pending[id]
	if ok {
		delete(cn.pending, id)
	}
	cn.mu.Unlock()
	if ok {
		cn.release()
	}
	return s
}

func (cn *conn) reason() error {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	return cn.closeReasonLocked()
}

func (cn *conn) closeReasonLocked() error {
	if cn.closeReason != nil {
		return cn.closeReason
	}
	return ErrClosed
}

// send enqueues a request for the writer, awaiting queue room (backpressure) and returning early
// if ctx is done or the connection dies.
func (cn *conn) send(ctx context.Context, req *tephrapb.Request) error {
	select {
	case cn.outbound <- req:
		return nil
	case <-cn.dead:
		return cn.reason()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sendCancel best-effort enqueues a fire-and-forget CancelRequest for target on this connection.
// It never blocks: if the queue is full the cancel is dropped (the server reaps on close anyway).
func (cn *conn) sendCancel(target uint64) {
	req := &tephrapb.Request{
		RequestId: cn.nextID(),
		Kind:      &tephrapb.Request_Cancel{Cancel: &tephrapb.CancelRequest{Target: target}},
	}
	select {
	case cn.outbound <- req:
	default:
	}
}

// roundTrip sends a one-shot request (append or stats) and awaits its single response.
func (cn *conn) roundTrip(ctx context.Context, req *tephrapb.Request, kind sinkKind) (*tephrapb.Response, error) {
	if err := cn.acquire(ctx); err != nil {
		return nil, err
	}
	id := cn.nextID()
	req.RequestId = id
	once := make(chan reply, 1)
	if err := cn.register(id, &sink{kind: kind, once: once}); err != nil {
		cn.release()
		return nil, err
	}
	if err := cn.send(ctx, req); err != nil {
		cn.finalize(id)
		return nil, err
	}
	select {
	case r := <-once:
		if r.err != nil {
			return nil, r.err
		}
		return r.resp, nil
	case <-ctx.Done():
		cn.finalize(id)
		return nil, ctx.Err()
	}
}

// startStream sends a streaming request (read or subscribe) and registers its sink. The in-flight
// slot is held for the stream's whole life and released when the stream is finalized.
func (cn *conn) startStream(ctx context.Context, req *tephrapb.Request, s *sink) (uint64, error) {
	if err := cn.acquire(ctx); err != nil {
		return 0, err
	}
	id := cn.nextID()
	req.RequestId = id
	if err := cn.register(id, s); err != nil {
		cn.release()
		return 0, err
	}
	if err := cn.send(ctx, req); err != nil {
		cn.finalize(id)
		return 0, err
	}
	return id, nil
}

// readLoop reads response frames and routes them by request id until the connection closes. On EOF
// or error it tears the connection down, failing every in-flight request.
func (cn *conn) readLoop() {
	reader := bufio.NewReader(cn.netConn)
	var lastErr error // an unattributed server error, used as the close reason if the peer closes
	for {
		var resp tephrapb.Response
		if err := readFrame(reader, cn.cfg.maxFrameLen, &resp); err != nil {
			if errors.Is(err, io.EOF) {
				cn.shutdown(closeReason(lastErr, nil))
			} else {
				cn.shutdown(closeReason(lastErr, err))
			}
			return
		}
		id := resp.GetRequestId()
		if id == unattributedRequestID {
			// A frame the server rejected before decode (for example over-limit). Remember it as
			// the reason to report if the connection then closes.
			if e, ok := resp.GetKind().(*tephrapb.Response_Error); ok {
				lastErr = serverErrorFromPB(e.Error)
			}
			continue
		}
		// An attributed response proves the connection is still healthily serving, so a stale
		// unattributed error must not be reported as the cause of a later clean close.
		lastErr = nil
		cn.route(id, &resp)
	}
}

func (cn *conn) route(id uint64, resp *tephrapb.Response) {
	cn.mu.Lock()
	s, ok := cn.pending[id]
	cn.mu.Unlock()
	if !ok {
		// Unknown id: a late frame for a request already finalized (for example cancelled). Drop it.
		return
	}
	switch s.kind {
	case sinkAppend, sinkStats:
		if cn.finalize(id) != nil {
			s.once <- reply{resp: resp}
		}
	default:
		cn.routeStream(id, s, resp)
	}
}

func (cn *conn) routeStream(id uint64, s *sink, resp *tephrapb.Response) {
	d := s.delivery
	switch k := resp.GetKind().(type) {
	case *tephrapb.Response_ReadEvents:
		if err := d.deliverEvents(k.ReadEvents.GetEvents()); err != nil {
			// A malformed event: deliverEvents already terminated the stream; the server still
			// thinks the request is live, so cancel it too.
			cn.finalizeAndCancel(id)
		}
	case *tephrapb.Response_ReadEnd:
		if rs, ok := d.(*ReadStream); ok {
			rs.end(Position(k.ReadEnd.GetWatermark()))
			cn.finalize(id) // the server ended the read; nothing to cancel
		} else {
			cn.abortStream(id, d, "unexpected read-end during subscribe")
		}
	case *tephrapb.Response_CaughtUp:
		if ss, ok := d.(*SubscribeStream); ok {
			ss.caughtUp(Position(k.CaughtUp.GetWatermark()))
		} else {
			cn.abortStream(id, d, "unexpected caught-up marker during read")
		}
	case *tephrapb.Response_Error:
		d.deliverError(serverErrorFromPB(k.Error))
		cn.finalize(id) // the server ended the stream; nothing to cancel
	default:
		cn.abortStream(id, d, fmt.Sprintf("unexpected response during stream: %T", k))
	}
}

// finalizeAndCancel finalizes a stream and, because the server still considers the request live,
// sends a best-effort cancel so it stops producing frames the client would only drop.
func (cn *conn) finalizeAndCancel(id uint64) {
	if cn.finalize(id) != nil {
		cn.sendCancel(id)
	}
}

// abortStream terminates a stream the client is rejecting for a protocol violation, then cancels
// it server-side. Without the cancel the server would keep streaming (forever, for a subscription)
// while every frame is dropped client-side.
func (cn *conn) abortStream(id uint64, d streamDelivery, message string) {
	d.deliverError(&ProtocolError{Message: message})
	cn.finalizeAndCancel(id)
}

// writeLoop drains the outbound queue, coalescing everything currently queued into one flush per
// burst. An oversized frame fails only its own request; any other write error is fatal to the
// connection.
func (cn *conn) writeLoop() {
	writer := bufio.NewWriter(cn.netConn)
	for {
		var req *tephrapb.Request
		select {
		case req = <-cn.outbound:
		case <-cn.dead:
			return
		}
		if cn.writeOne(writer, req) {
			return
		}
		// Drain anything already queued before paying for a flush.
		drained := false
		for !drained {
			select {
			case req = <-cn.outbound:
				if cn.writeOne(writer, req) {
					return
				}
			default:
				drained = true
			}
		}
		if err := writer.Flush(); err != nil {
			cn.shutdown(&ConnError{Reason: "connection error", cause: err})
			return
		}
	}
}

// writeOne writes one frame. It returns true only on a fatal (connection-ending) error. An
// oversized frame is reported to just that request and the connection continues, since the frame
// is rejected before any byte reaches the wire so the stream stays aligned.
func (cn *conn) writeOne(writer *bufio.Writer, req *tephrapb.Request) (fatal bool) {
	err := writeFrame(writer, req, cn.cfg.maxFrameLen)
	if err == nil {
		return false
	}
	var tooLarge *FrameTooLargeError
	if errors.As(err, &tooLarge) {
		if s := cn.finalize(req.GetRequestId()); s != nil {
			s.fail(err)
		}
		return false
	}
	cn.shutdown(&ConnError{Reason: "connection error", cause: err})
	return true
}

// shutdown tears the connection down once: it stops accepting new requests, unblocks the reader
// and writer, and fails every still-pending request with reason.
func (cn *conn) shutdown(reason error) {
	cn.closeOnce.Do(func() {
		cn.mu.Lock()
		cn.closed = true
		if cn.closeReason == nil {
			cn.closeReason = reason
		}
		pending := cn.pending
		cn.pending = nil
		cn.mu.Unlock()

		close(cn.dead)
		_ = cn.netConn.Close()

		for _, s := range pending {
			s.fail(reason)
		}
	})
}

// closeReason builds the reason to report when the connection ends. An unattributed server error
// (captured from the wire) wins; otherwise a transport error; otherwise an orderly close.
func closeReason(lastErr, ioErr error) error {
	switch {
	case lastErr != nil:
		return &ConnError{Reason: "connection closed", cause: lastErr}
	case ioErr != nil:
		return &ConnError{Reason: "connection error", cause: ioErr}
	default:
		return &ConnError{Reason: "server closed the connection"}
	}
}
