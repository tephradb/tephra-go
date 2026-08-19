package tephra

import "github.com/tephradb/tephra-go/internal/tephrapb"

// ProtocolVersion is the wire protocol version this client announces in its opening Hello and
// expects the server to answer in its HelloAck. It is the single compatibility mechanism: the
// server rejects a version it does not support rather than inferring compatibility from field
// presence. It matches the tephra server and Rust client (tephra-proto's PROTOCOL_VERSION).
const ProtocolVersion uint32 = 1

// helloRequest builds the mandatory opening Hello frame: the current ProtocolVersion and, when
// authenticating, the bearer token. An empty token leaves the connection unauthenticated (the
// server accepts that only when it has no tokens configured).
func helloRequest(id uint64, token string) *tephrapb.Request {
	hello := &tephrapb.Hello{ProtocolVersion: ProtocolVersion}
	if token != "" {
		hello.AuthToken = &token
	}
	return &tephrapb.Request{RequestId: id, Kind: &tephrapb.Request_Hello{Hello: hello}}
}

// verifyHelloAck checks the server's reply to the opening Hello. A HelloAck with a matching
// protocol version means the handshake succeeded; a mismatch is a *ProtocolError (defensive
// against a non-conforming server, since the server negotiates the version by accepting or
// rejecting the Hello). An error frame surfaces as the server's *ServerError (an unauthenticated
// rejection carries ErrCodeUnauthenticated); any other kind is a *ProtocolError.
func verifyHelloAck(resp *tephrapb.Response) error {
	switch k := resp.GetKind().(type) {
	case *tephrapb.Response_HelloAck:
		if v := k.HelloAck.GetProtocolVersion(); v != ProtocolVersion {
			return protocolErrorf("server protocol version %d does not match client %d", v, ProtocolVersion)
		}
		return nil
	case *tephrapb.Response_Error:
		return serverErrorFromPB(k.Error)
	default:
		return protocolErrorf("unexpected response to hello: %T", k)
	}
}
