package tephra

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/tephradb/tephra-go/internal/tephrapb"
	"google.golang.org/protobuf/proto"
)

// fakeServer is an in-memory tephra server that speaks the frame protocol. It dispatches each
// request to the test's handler in its own goroutine, emulating the real server's concurrent
// per-connection handling, so responses may be produced out of order.
type fakeServer struct {
	ln      net.Listener
	handler func(*fakeSession, *tephrapb.Request)
	wg      sync.WaitGroup

	// authToken, when non-empty, is the bearer token the opening Hello must present; a mismatch
	// is rejected with ERROR_CODE_UNAUTHENTICATED. Empty accepts any (or no) token.
	authToken string
	// ackVersion is the protocol version returned in the HelloAck (0 means ProtocolVersion), so a
	// test can force a version mismatch.
	ackVersion uint32

	mu    sync.Mutex
	conns []net.Conn
}

type fakeSession struct {
	mu sync.Mutex
	w  *bufio.Writer
}

func (s *fakeSession) send(resp *tephrapb.Response) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = writeFrame(s.w, resp, DefaultMaxFrameLen)
	_ = s.w.Flush()
}

func newFakeServer(t *testing.T, handler func(*fakeSession, *tephrapb.Request)) *fakeServer {
	t.Helper()
	return startFakeServer(t, &fakeServer{handler: handler})
}

// startFakeServer binds a listener on fs and starts serving, so a caller can pre-set fields like
// authToken or ackVersion before any connection is accepted.
func startFakeServer(t *testing.T, fs *fakeServer) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fs.ln = ln
	fs.wg.Add(1)
	go fs.accept()
	t.Cleanup(fs.close)
	return fs
}

func (fs *fakeServer) addr() string { return fs.ln.Addr().String() }

func (fs *fakeServer) accept() {
	defer fs.wg.Done()
	for {
		c, err := fs.ln.Accept()
		if err != nil {
			return
		}
		fs.mu.Lock()
		fs.conns = append(fs.conns, c)
		fs.mu.Unlock()
		fs.wg.Add(1)
		go fs.serve(c)
	}
}

func (fs *fakeServer) serve(c net.Conn) {
	defer fs.wg.Done()
	sess := &fakeSession{w: bufio.NewWriter(c)}
	r := bufio.NewReader(c)
	established := false
	for {
		var req tephrapb.Request
		if err := readFrame(r, DefaultMaxFrameLen, &req); err != nil {
			return
		}
		if !established {
			// The first frame must be the mandatory Hello; answer it here rather than in the
			// test handler, so every handler only sees real requests.
			if !fs.handleHello(sess, &req) {
				return
			}
			established = true
			continue
		}
		reqCopy := proto.Clone(&req).(*tephrapb.Request)
		fs.wg.Add(1)
		go func() {
			defer fs.wg.Done()
			fs.handler(sess, reqCopy)
		}()
	}
}

// handleHello answers the opening Hello, returning whether the connection may proceed. A non-Hello
// first frame or a token mismatch (when authToken is set) is rejected with ERROR_CODE_UNAUTHENTICATED;
// otherwise it acks with ackVersion (defaulting to ProtocolVersion).
func (fs *fakeServer) handleHello(sess *fakeSession, req *tephrapb.Request) bool {
	hello := req.GetHello()
	if hello == nil {
		sess.send(errResp(req.GetRequestId(), tephrapb.ErrorCode_ERROR_CODE_UNAUTHENTICATED, "first frame must be a hello"))
		return false
	}
	if fs.authToken != "" && hello.GetAuthToken() != fs.authToken {
		sess.send(errResp(req.GetRequestId(), tephrapb.ErrorCode_ERROR_CODE_UNAUTHENTICATED, "invalid or missing auth token"))
		return false
	}
	version := fs.ackVersion
	if version == 0 {
		version = ProtocolVersion
	}
	sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_HelloAck{HelloAck: &tephrapb.HelloAck{ProtocolVersion: version, ServerVersion: "fake"}}})
	return true
}

func (fs *fakeServer) close() {
	fs.ln.Close()
	fs.mu.Lock()
	for _, c := range fs.conns {
		c.Close()
	}
	fs.mu.Unlock()
	fs.wg.Wait()
}

func errResp(id uint64, code tephrapb.ErrorCode, msg string) *tephrapb.Response {
	return &tephrapb.Response{RequestId: id, Kind: &tephrapb.Response_Error{Error: &tephrapb.ErrorResponse{Code: code, Message: msg}}}
}

func dialTest(t *testing.T, addr string, opts ...Option) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, addr, opts...)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestAppendAndReadRoundTrip(t *testing.T) {
	fs := newFakeServer(t, func(sess *fakeSession, req *tephrapb.Request) {
		switch req.GetKind().(type) {
		case *tephrapb.Request_Append:
			n := uint64(len(req.GetAppend().GetEvents()))
			sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_Append{Append: &tephrapb.AppendResponse{First: 1, Last: n}}})
		case *tephrapb.Request_Read:
			sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_ReadEvents{ReadEvents: &tephrapb.ReadEvents{Events: []*tephrapb.SequencedEvent{
				{Position: 1, Event: &tephrapb.Event{Type: "Enrolled", Tags: []string{"course:c1"}, Payload: []byte("a")}},
				{Position: 2, Event: &tephrapb.Event{Type: "Enrolled", Tags: []string{"course:c1"}, Payload: []byte("b")}},
			}}}})
			sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_ReadEnd{ReadEnd: &tephrapb.ReadEnd{Watermark: 2}}})
		}
	})
	c := dialTest(t, fs.addr())
	ctx := context.Background()

	ev, err := NewEvent("Enrolled", []string{"course:c1"}, []byte("a"))
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	res, err := c.Append(ctx, []Event{ev}, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if res.First != 1 || res.Last != 1 {
		t.Fatalf("append result = %+v, want {1 1}", res)
	}

	events, watermark, err := c.ReadAll(ctx, QueryAll(), Zero, nil)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(events) != 2 || events[0].Position != 1 || events[1].Position != 2 {
		t.Fatalf("read events = %+v, want positions 1,2", events)
	}
	if events[0].Type() != "Enrolled" {
		t.Fatalf("event type = %q, want Enrolled", events[0].Type())
	}
	if watermark != 2 {
		t.Fatalf("watermark = %d, want 2", watermark)
	}
}

func TestServerErrorSurfaced(t *testing.T) {
	fs := newFakeServer(t, func(sess *fakeSession, req *tephrapb.Request) {
		pos := uint64(9)
		sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_Error{Error: &tephrapb.ErrorResponse{
			Code: tephrapb.ErrorCode_ERROR_CODE_CONFLICT, Message: "boom", Retryable: true, ConflictPosition: &pos,
		}}})
	})
	c := dialTest(t, fs.addr())

	_, err := c.Append(context.Background(), nil, nil)
	var se *ServerError
	if !errors.As(err, &se) {
		t.Fatalf("Append error = %v, want *ServerError", err)
	}
	if se.Code != ErrCodeConflict || !se.Retryable || se.ConflictPosition == nil || *se.ConflictPosition != 9 {
		t.Fatalf("server error = %+v, want conflict/retryable/pos 9", se)
	}
}

func TestAuthTokenAccepted(t *testing.T) {
	// The server requires a token; the client presents the matching one, so the Hello succeeds and
	// an append round-trips over both the control and bulk sockets.
	fs := startFakeServer(t, &fakeServer{authToken: "s3cret", handler: func(sess *fakeSession, req *tephrapb.Request) {
		if _, ok := req.GetKind().(*tephrapb.Request_Append); ok {
			sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_Append{Append: &tephrapb.AppendResponse{First: 1, Last: 1}}})
		}
	}})
	c := dialTest(t, fs.addr(), WithAuthToken("s3cret"))

	if _, err := c.Append(context.Background(), nil, nil); err != nil {
		t.Fatalf("append after authenticated dial: %v", err)
	}
}

func TestAuthTokenRejected(t *testing.T) {
	// The server requires a token; the client presents the wrong one, so Dial fails with an
	// unauthenticated ServerError rather than connecting.
	fs := startFakeServer(t, &fakeServer{authToken: "right", handler: func(*fakeSession, *tephrapb.Request) {}})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := Dial(ctx, fs.addr(), WithBulkConnections(0), WithAuthToken("wrong"))
	var se *ServerError
	if !errors.As(err, &se) {
		t.Fatalf("Dial with a bad token = %v, want *ServerError", err)
	}
	if se.Code != ErrCodeUnauthenticated {
		t.Fatalf("error code = %v, want unauthenticated", se.Code)
	}
}

func TestMissingAuthTokenRejected(t *testing.T) {
	// The server requires a token; the client presents none, so Dial fails as unauthenticated.
	fs := startFakeServer(t, &fakeServer{authToken: "needed", handler: func(*fakeSession, *tephrapb.Request) {}})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := Dial(ctx, fs.addr(), WithBulkConnections(0))
	var se *ServerError
	if !errors.As(err, &se) || se.Code != ErrCodeUnauthenticated {
		t.Fatalf("Dial without a required token = %v, want unauthenticated *ServerError", err)
	}
}

func TestHelloVersionMismatch(t *testing.T) {
	// The server acks with a protocol version the client does not speak, so Dial fails with a
	// ProtocolError rather than proceeding on an incompatible protocol.
	fs := startFakeServer(t, &fakeServer{ackVersion: ProtocolVersion + 1, handler: func(*fakeSession, *tephrapb.Request) {}})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := Dial(ctx, fs.addr(), WithBulkConnections(0))
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("Dial against a version-mismatched server = %v, want *ProtocolError", err)
	}
}

func TestConcurrentMultiplexingDemux(t *testing.T) {
	// Each read echoes its `after` cursor back as the watermark, so a caller that gets the wrong
	// watermark caught a response meant for another request (a demux bug).
	fs := newFakeServer(t, func(sess *fakeSession, req *tephrapb.Request) {
		if rd, ok := req.GetKind().(*tephrapb.Request_Read); ok {
			sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_ReadEnd{ReadEnd: &tephrapb.ReadEnd{Watermark: rd.Read.GetAfter()}}})
		}
	})
	c := dialTest(t, fs.addr())

	const n = 64
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, watermark, err := c.ReadAll(context.Background(), QueryAll(), Position(i), nil)
			if err != nil {
				errs[i] = err
				return
			}
			if watermark != Position(i) {
				errs[i] = fmt.Errorf("read %d got watermark %d", i, watermark)
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}
}

// TestNoReadHeadOfLineBlocking asserts that an unconsumed streaming read on a socket does not
// stall a concurrent append multiplexed on the same socket. This is the regression the Rust client
// fixed; the Go client avoids it with an unbounded per-stream delivery buffer, so the connection
// reader never blocks on a slow consumer.
func TestNoReadHeadOfLineBlocking(t *testing.T) {
	fs := newFakeServer(t, func(sess *fakeSession, req *tephrapb.Request) {
		switch req.GetKind().(type) {
		case *tephrapb.Request_Read:
			// Flood the read with events and never terminate it. The client buffers them all.
			for f := 0; f < 10; f++ {
				batch := make([]*tephrapb.SequencedEvent, 100)
				for i := range batch {
					batch[i] = &tephrapb.SequencedEvent{Position: uint64(f*100 + i + 1), Event: &tephrapb.Event{Type: "T"}}
				}
				sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_ReadEvents{ReadEvents: &tephrapb.ReadEvents{Events: batch}}})
			}
		case *tephrapb.Request_Append:
			sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_Append{Append: &tephrapb.AppendResponse{First: 1, Last: 1}}})
		}
	})
	// Single socket: reads and appends share the control connection.
	c := dialTest(t, fs.addr(), WithBulkConnections(0))

	rs, err := c.Read(context.Background(), QueryAll(), Zero, nil)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer rs.Close()

	// The append must complete promptly even though the read is streaming and unconsumed.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Append(ctx, nil, nil); err != nil {
		t.Fatalf("append blocked behind an unconsumed read (head-of-line blocking): %v", err)
	}
}

func TestFailAllOnConnectionClose(t *testing.T) {
	// The server drops the connection instead of replying; the in-flight append must fail with a
	// connection error rather than hang.
	fs := newFakeServer(t, func(sess *fakeSession, req *tephrapb.Request) {
		// Never reply; the test drops the connection to force fail-all.
	})
	c := dialTest(t, fs.addr(), WithBulkConnections(0))

	// Kick a request, then close server conns to force a drop.
	done := make(chan error, 1)
	go func() {
		_, err := c.Append(context.Background(), nil, nil)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	fs.mu.Lock()
	for _, sc := range fs.conns {
		sc.Close()
	}
	fs.mu.Unlock()

	select {
	case err := <-done:
		var ce *ConnError
		if !errors.As(err, &ce) {
			t.Fatalf("append after drop = %v, want *ConnError", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("append did not fail after the connection dropped")
	}
}

func TestCloseStreamCancelsServerSide(t *testing.T) {
	readID := make(chan uint64, 1)
	cancelTarget := make(chan uint64, 1)
	fs := newFakeServer(t, func(sess *fakeSession, req *tephrapb.Request) {
		switch k := req.GetKind().(type) {
		case *tephrapb.Request_Read:
			select {
			case readID <- req.GetRequestId():
			default:
			}
			sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_ReadEvents{ReadEvents: &tephrapb.ReadEvents{Events: []*tephrapb.SequencedEvent{
				{Position: 1, Event: &tephrapb.Event{Type: "T"}},
			}}}})
			// Keep the read open (no ReadEnd) so closing it triggers a cancel.
		case *tephrapb.Request_Cancel:
			select {
			case cancelTarget <- k.Cancel.GetTarget():
			default:
			}
		}
	})
	c := dialTest(t, fs.addr(), WithBulkConnections(1))

	rs, err := c.Read(context.Background(), QueryAll(), Zero, nil)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !rs.Next() {
		t.Fatalf("expected one event, Err=%v", rs.Err())
	}
	if rs.Event().Position != 1 {
		t.Fatalf("event position = %d, want 1", rs.Event().Position)
	}

	id := <-readID
	rs.Close()

	select {
	case target := <-cancelTarget:
		if target != id {
			t.Fatalf("cancel target = %d, want read id %d", target, id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not receive a CancelRequest after Close")
	}
}

func TestServerEventsAcceptedVerbatim(t *testing.T) {
	// The server is the source of truth for stored events; the client must not re-validate and
	// reject them. A duplicate tag (which the client's own NewEvent would reject) must still read
	// back cleanly rather than failing the stream.
	fs := newFakeServer(t, func(sess *fakeSession, req *tephrapb.Request) {
		if _, ok := req.GetKind().(*tephrapb.Request_Read); ok {
			sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_ReadEvents{ReadEvents: &tephrapb.ReadEvents{Events: []*tephrapb.SequencedEvent{
				{Position: 1, Event: &tephrapb.Event{Type: "T", Tags: []string{"dup:1", "dup:1"}, Payload: []byte("p")}},
			}}}})
			sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_ReadEnd{ReadEnd: &tephrapb.ReadEnd{Watermark: 1}}})
		}
	})
	c := dialTest(t, fs.addr(), WithBulkConnections(1))

	events, _, err := c.ReadAll(context.Background(), QueryAll(), Zero, nil)
	if err != nil {
		t.Fatalf("ReadAll should accept server events verbatim, got %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if got := events[0].Tags(); len(got) != 2 {
		t.Fatalf("tags = %v, want both tags as sent", got)
	}
}

func TestProtocolViolationCancelsServer(t *testing.T) {
	// A caught-up marker is invalid on a read. The client must terminate the read with a protocol
	// error AND cancel it server-side, so the server stops streaming frames it would only drop.
	cancelTarget := make(chan uint64, 1)
	fs := newFakeServer(t, func(sess *fakeSession, req *tephrapb.Request) {
		switch k := req.GetKind().(type) {
		case *tephrapb.Request_Read:
			sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_CaughtUp{CaughtUp: &tephrapb.SubscribeCaughtUp{Watermark: 1}}})
		case *tephrapb.Request_Cancel:
			select {
			case cancelTarget <- k.Cancel.GetTarget():
			default:
			}
		}
	})
	c := dialTest(t, fs.addr(), WithBulkConnections(1))

	rs, err := c.Read(context.Background(), QueryAll(), Zero, nil)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer rs.Close()

	if rs.Next() {
		t.Fatal("Next should be false after a protocol violation")
	}
	var pe *ProtocolError
	if !errors.As(rs.Err(), &pe) {
		t.Fatalf("Err = %v, want *ProtocolError", rs.Err())
	}

	select {
	case target := <-cancelTarget:
		if target != rs.id {
			t.Fatalf("cancel target = %d, want read id %d", target, rs.id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not receive a cancel after the client rejected a protocol violation")
	}
}

func TestClosedClientRejectsRequests(t *testing.T) {
	fs := newFakeServer(t, func(sess *fakeSession, req *tephrapb.Request) {})
	c := dialTest(t, fs.addr(), WithBulkConnections(0))
	c.Close()
	_, err := c.Append(context.Background(), nil, nil)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("append after Close = %v, want ErrClosed", err)
	}
}

func TestContextCancelStopsRead(t *testing.T) {
	fs := newFakeServer(t, func(sess *fakeSession, req *tephrapb.Request) {
		if _, ok := req.GetKind().(*tephrapb.Request_Read); ok {
			sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_ReadEvents{ReadEvents: &tephrapb.ReadEvents{Events: []*tephrapb.SequencedEvent{
				{Position: 1, Event: &tephrapb.Event{Type: "T"}},
			}}}})
			// Never terminate; the client's context cancellation must end the stream.
		}
	})
	c := dialTest(t, fs.addr(), WithBulkConnections(1))

	ctx, cancel := context.WithCancel(context.Background())
	rs, err := c.Read(ctx, QueryAll(), Zero, nil)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer rs.Close()
	if !rs.Next() {
		t.Fatalf("expected one event, Err=%v", rs.Err())
	}
	cancel()
	if rs.Next() {
		t.Fatal("Next should return false after context cancellation")
	}
	if !errors.Is(rs.Err(), context.Canceled) {
		t.Fatalf("Err = %v, want context.Canceled", rs.Err())
	}
}

// ---------------------------------------------------------------------------
// TLS
// ---------------------------------------------------------------------------

// newSelfSigned returns an ephemeral self-signed certificate valid for 127.0.0.1 and localhost,
// serving as both the server's leaf and a client trust anchor, plus a pool that trusts it.
func newSelfSigned(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "tephra-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, pool
}

// newFakeTLSServer is newFakeServer over a TLS listener, returning a client tls.Config that trusts
// the server's ephemeral certificate.
func newFakeTLSServer(t *testing.T, handler func(*fakeSession, *tephrapb.Request)) (*fakeServer, *tls.Config) {
	t.Helper()
	serverCert, pool := newSelfSigned(t)
	tcpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fs := &fakeServer{
		ln:      tls.NewListener(tcpLn, &tls.Config{Certificates: []tls.Certificate{serverCert}}),
		handler: handler,
	}
	fs.wg.Add(1)
	go fs.accept()
	t.Cleanup(fs.close)
	return fs, &tls.Config{RootCAs: pool}
}

func TestTLSRoundTrip(t *testing.T) {
	fs, clientTLS := newFakeTLSServer(t, func(sess *fakeSession, req *tephrapb.Request) {
		switch req.GetKind().(type) {
		case *tephrapb.Request_Append:
			sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_Append{Append: &tephrapb.AppendResponse{First: 1, Last: 1}}})
		case *tephrapb.Request_Read:
			sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_ReadEvents{ReadEvents: &tephrapb.ReadEvents{Events: []*tephrapb.SequencedEvent{
				{Position: 1, Event: &tephrapb.Event{Type: "Secure", Payload: []byte("tls")}},
			}}}})
			sess.send(&tephrapb.Response{RequestId: req.GetRequestId(), Kind: &tephrapb.Response_ReadEnd{ReadEnd: &tephrapb.ReadEnd{Watermark: 1}}})
		}
	})
	// ServerName is left unset, so the client derives it (127.0.0.1) from the dial address and
	// verifies against the certificate's IP SAN.
	c := dialTest(t, fs.addr(), WithBulkConnections(0), WithTLS(clientTLS))
	ctx := context.Background()

	if _, err := c.Append(ctx, nil, nil); err != nil {
		t.Fatalf("append over TLS: %v", err)
	}
	events, watermark, err := c.ReadAll(ctx, QueryAll(), Zero, nil)
	if err != nil {
		t.Fatalf("read over TLS: %v", err)
	}
	if len(events) != 1 || events[0].Type() != "Secure" {
		t.Fatalf("events = %+v, want one 'Secure' event", events)
	}
	if watermark != 1 {
		t.Fatalf("watermark = %d, want 1", watermark)
	}
}

func TestTLSHandshakeFailsWhenUntrusted(t *testing.T) {
	// With an empty tls.Config the client falls back to the system roots, which do not trust the
	// server's self-signed certificate, so Dial must fail rather than connect insecurely.
	fs, _ := newFakeTLSServer(t, func(sess *fakeSession, req *tephrapb.Request) {})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := Dial(ctx, fs.addr(), WithBulkConnections(0), WithTLS(&tls.Config{})); err == nil {
		t.Fatal("Dial should fail when the server certificate is untrusted")
	}
}
