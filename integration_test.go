//go:build integration

// Package-level integration tests that run against a real tephra-server. Enable with:
//
//	go test -tags integration -timeout 300s ./...
//
// The server is built from a sibling ../tephra checkout (override with TEPHRA_REPO) or taken from
// TEPHRA_SERVER_BIN. Tests skip (rather than fail) when the server cannot be built or found.
package tephra_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	tephra "github.com/tqwewe/tephra-go"
)

var (
	buildOnce sync.Once
	builtBin  string
	buildErr  error
)

func serverBinary(t *testing.T) string {
	if bin := os.Getenv("TEPHRA_SERVER_BIN"); bin != "" {
		if _, err := os.Stat(bin); err != nil {
			t.Skipf("TEPHRA_SERVER_BIN=%s not found: %v", bin, err)
		}
		return bin
	}
	buildOnce.Do(func() {
		repo := os.Getenv("TEPHRA_REPO")
		if repo == "" {
			repo = "../tephra"
		}
		if _, err := os.Stat(repo); err != nil {
			buildErr = fmt.Errorf("tephra repo %q not found (set TEPHRA_REPO or TEPHRA_SERVER_BIN): %w", repo, err)
			return
		}
		cmd := exec.Command("cargo", "build", "-p", "tephra-server")
		cmd.Dir = repo
		var out bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &out
		if err := cmd.Run(); err != nil {
			buildErr = fmt.Errorf("cargo build tephra-server: %w\n%s", err, out.String())
			return
		}
		builtBin = filepath.Join(repo, "target", "debug", "tephra-server")
	})
	if buildErr != nil {
		t.Skipf("cannot build tephra-server: %v", buildErr)
	}
	return builtBin
}

// syncBuffer is a bytes.Buffer safe for concurrent use, so os/exec's stdout/stderr copy goroutines
// can write to it while a test reads the captured logs.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startServer launches a fresh plaintext server instance with its own data directory and free
// port, and returns its address. It is torn down when the test finishes.
func startServer(t *testing.T) string {
	addr, _ := launchServer(t, t.TempDir(), nil)
	return addr
}

// launchServer starts tephra-server with the given data directory and extra environment, wires up
// teardown and log capture, and waits until it accepts connections. It returns the address and the
// captured log buffer.
func launchServer(t *testing.T, dataDir string, extraEnv []string) (string, *syncBuffer) {
	bin := serverBinary(t)
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cmd := exec.Command(bin, "--bind", addr, "--data-dir", dataDir)
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	logs := &syncBuffer{}
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("server logs:\n%s", logs.String())
		}
	})
	waitReady(t, addr)
	return addr, logs
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitReady(t *testing.T, addr string) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
			c.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server at %s not ready in time", addr)
}

func dialIT(t *testing.T, addr string, opts ...tephra.Option) *tephra.Client {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := tephra.Dial(ctx, addr, opts...)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func mustEvent(t *testing.T, typ string, tags []string, payload string) tephra.Event {
	t.Helper()
	e, err := tephra.NewEvent(typ, tags, []byte(payload))
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	return e
}

func positions(events []tephra.SequencedEvent) []uint64 {
	out := make([]uint64, len(events))
	for i, e := range events {
		out[i] = uint64(e.Position)
	}
	return out
}

func TestIntegrationAppendReadForwardBack(t *testing.T) {
	c := dialIT(t, startServer(t))
	ctx := context.Background()

	batch := []tephra.Event{
		mustEvent(t, "Enrolled", []string{"course:c1"}, "a"),
		mustEvent(t, "Enrolled", []string{"course:c1"}, "b"),
		mustEvent(t, "Enrolled", []string{"course:c1"}, "c"),
	}
	res, err := c.Append(ctx, batch, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if res.First != 1 || res.Last != 3 {
		t.Fatalf("append result = %+v, want {1 3}", res)
	}

	fwd, watermark, err := c.ReadAll(ctx, tephra.QueryAll(), tephra.Zero, nil)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if got := positions(fwd); !equalU64(got, []uint64{1, 2, 3}) {
		t.Fatalf("forward positions = %v, want [1 2 3]", got)
	}
	if watermark != 3 {
		t.Fatalf("watermark = %d, want 3", watermark)
	}

	back, _, err := c.ReadAllBack(ctx, tephra.QueryAll(), tephra.Max, nil)
	if err != nil {
		t.Fatalf("ReadAllBack: %v", err)
	}
	if got := positions(back); !equalU64(got, []uint64{3, 2, 1}) {
		t.Fatalf("backward positions = %v, want [3 2 1]", got)
	}
}

func TestIntegrationPaginationSeam(t *testing.T) {
	c := dialIT(t, startServer(t))
	ctx := context.Background()

	var batch []tephra.Event
	for i := 0; i < 5; i++ {
		batch = append(batch, mustEvent(t, "N", []string{"k:v"}, fmt.Sprintf("%d", i)))
	}
	if _, err := c.Append(ctx, batch, nil); err != nil {
		t.Fatalf("Append: %v", err)
	}

	page1, _, err := c.ReadAll(ctx, tephra.QueryAll(), tephra.Zero, tephra.Limit(3))
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if got := positions(page1); !equalU64(got, []uint64{1, 2, 3}) {
		t.Fatalf("page1 = %v, want [1 2 3]", got)
	}
	cursor := page1[len(page1)-1].Position

	page2, _, err := c.ReadAll(ctx, tephra.QueryAll(), cursor, tephra.Limit(3))
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if got := positions(page2); !equalU64(got, []uint64{4, 5}) {
		t.Fatalf("page2 = %v, want [4 5] (no gap or duplicate at the seam)", got)
	}
}

func TestIntegrationSubscribeCatchUpAndLive(t *testing.T) {
	addr := startServer(t)
	c := dialIT(t, addr)
	appendCtx := context.Background()

	if _, err := c.Append(appendCtx, []tephra.Event{
		mustEvent(t, "N", []string{"k:v"}, "1"),
		mustEvent(t, "N", []string{"k:v"}, "2"),
	}, nil); err != nil {
		t.Fatalf("Append: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sub, err := c.Subscribe(ctx, tephra.QueryAll(), tephra.Zero)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	// Drain the catch-up burst up to the first live-edge marker.
	var caughtUp bool
	var seen []uint64
	for sub.Next() {
		item := sub.Item()
		if item.IsCaughtUp() {
			caughtUp = true
			break
		}
		seen = append(seen, uint64(item.Event.Position))
	}
	if !caughtUp {
		t.Fatalf("subscription ended before catching up: %v", sub.Err())
	}
	if !equalU64(seen, []uint64{1, 2}) {
		t.Fatalf("catch-up events = %v, want [1 2]", seen)
	}

	// A new append must arrive live.
	if _, err := c.Append(appendCtx, []tephra.Event{mustEvent(t, "N", []string{"k:v"}, "3")}, nil); err != nil {
		t.Fatalf("live Append: %v", err)
	}
	for sub.Next() {
		item := sub.Item()
		if item.IsCaughtUp() {
			continue
		}
		if item.Event.Position == 3 {
			return // observed the live event
		}
	}
	t.Fatalf("did not observe live event at position 3: %v", sub.Err())
}

func TestIntegrationAppendConditionConflict(t *testing.T) {
	c := dialIT(t, startServer(t))
	ctx := context.Background()

	if _, err := c.Append(ctx, []tephra.Event{mustEvent(t, "Registered", []string{"email:a@example.com"}, "{}")}, nil); err != nil {
		t.Fatalf("first append: %v", err)
	}

	guard, err := tephra.WithTags("email:a@example.com")
	if err != nil {
		t.Fatalf("WithTags: %v", err)
	}
	cond := tephra.NewAppendCondition(tephra.QueryItems(guard))
	_, err = c.Append(ctx, []tephra.Event{mustEvent(t, "Registered", []string{"email:a@example.com"}, "{}")}, &cond)

	var se *tephra.ServerError
	if !errors.As(err, &se) {
		t.Fatalf("guarded append error = %v, want *ServerError", err)
	}
	if se.Code != tephra.ErrCodeConflict {
		t.Fatalf("error code = %v, want conflict", se.Code)
	}
}

func TestIntegrationStats(t *testing.T) {
	c := dialIT(t, startServer(t))
	ctx := context.Background()

	if _, err := c.Append(ctx, []tephra.Event{
		mustEvent(t, "N", nil, "1"),
		mustEvent(t, "N", nil, "2"),
	}, nil); err != nil {
		t.Fatalf("Append: %v", err)
	}
	st, err := c.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.EventCount < 2 {
		t.Fatalf("event count = %d, want >= 2", st.EventCount)
	}
	if st.Version == "" {
		t.Fatal("stats version is empty")
	}
}

func TestIntegrationLargeMultiBatchRead(t *testing.T) {
	c := dialIT(t, startServer(t))
	ctx := context.Background()

	const total = 3000 // exceeds the server's per-frame read batch, so the read spans many frames
	for start := 0; start < total; start += 500 {
		var batch []tephra.Event
		for i := start; i < start+500 && i < total; i++ {
			batch = append(batch, mustEvent(t, "N", []string{"k:v"}, fmt.Sprintf("%d", i)))
		}
		if _, err := c.Append(ctx, batch, nil); err != nil {
			t.Fatalf("append chunk at %d: %v", start, err)
		}
	}

	events, watermark, err := c.ReadAll(ctx, tephra.QueryAll(), tephra.Zero, nil)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(events) != total {
		t.Fatalf("read %d events, want %d", len(events), total)
	}
	if watermark != tephra.Position(total) {
		t.Fatalf("watermark = %d, want %d", watermark, total)
	}
	for i, e := range events {
		if e.Position != tephra.Position(i+1) {
			t.Fatalf("event %d has position %d, want %d (non-contiguous read)", i, e.Position, i+1)
			break
		}
	}
}

func TestIntegrationConcurrentReads(t *testing.T) {
	c := dialIT(t, startServer(t))
	ctx := context.Background()

	const count = 100
	var batch []tephra.Event
	for i := 0; i < count; i++ {
		batch = append(batch, mustEvent(t, "N", []string{"k:v"}, fmt.Sprintf("%d", i)))
	}
	if _, err := c.Append(ctx, batch, nil); err != nil {
		t.Fatalf("Append: %v", err)
	}

	const readers = 50
	var wg sync.WaitGroup
	errs := make([]error, readers)
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			events, _, err := c.ReadAll(ctx, tephra.QueryAll(), tephra.Zero, nil)
			if err != nil {
				errs[r] = err
				return
			}
			if len(events) != count {
				errs[r] = fmt.Errorf("reader %d got %d events, want %d", r, len(events), count)
			}
		}(r)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func equalU64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// writeTestCertFiles generates an ephemeral self-signed certificate valid for 127.0.0.1, writes it
// and its key as PEM files in dir (for the server to load), and returns their paths plus a client
// tls.Config that trusts the certificate.
func writeTestCertFiles(t *testing.T, dir string) (certPath, keyPath string, clientCfg *tls.Config) {
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

	certPath = filepath.Join(dir, "cert.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}

	keyPath = filepath.Join(dir, "key.pem")
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return certPath, keyPath, &tls.Config{RootCAs: pool}
}

// startTLSServer launches a server configured for TLS via TEPHRA__TLS__CERT/KEY, and returns its
// address plus a client tls.Config that trusts the ephemeral certificate. It skips if the binary
// was built without the tls feature.
func startTLSServer(t *testing.T) (string, *tls.Config) {
	dir := t.TempDir()
	certPath, keyPath, clientCfg := writeTestCertFiles(t, dir)
	addr, logs := launchServer(t, dir, []string{
		"TEPHRA__TLS__CERT=" + certPath,
		"TEPHRA__TLS__KEY=" + keyPath,
	})
	if strings.Contains(logs.String(), "without the tls feature") {
		t.Skip("tephra-server was built without the tls feature; rebuild with --features tls")
	}
	return addr, clientCfg
}

func TestIntegrationTLS(t *testing.T) {
	addr, clientCfg := startTLSServer(t)
	c := dialIT(t, addr, tephra.WithTLS(clientCfg))
	ctx := context.Background()

	res, err := c.Append(ctx, []tephra.Event{mustEvent(t, "Secure", []string{"tls:1"}, "over-tls")}, nil)
	if err != nil {
		t.Fatalf("append over TLS: %v", err)
	}
	if res.First != 1 || res.Last != 1 {
		t.Fatalf("append result = %+v, want {1 1}", res)
	}

	events, watermark, err := c.ReadAll(ctx, tephra.QueryAll(), tephra.Zero, nil)
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
