package tephra

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/tephradb/tephra-go/internal/tephrapb"
	"google.golang.org/protobuf/proto"
)

func TestFrameRoundTrip(t *testing.T) {
	req := &tephrapb.Request{
		RequestId: 42,
		Kind: &tephrapb.Request_Append{Append: &tephrapb.AppendRequest{
			Events: []*tephrapb.Event{{Type: "Enrolled", Tags: []string{"course:c1"}, Payload: []byte("{}")}},
		}},
	}
	var buf bytes.Buffer
	if err := writeFrame(&buf, req, DefaultMaxFrameLen); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	var got tephrapb.Request
	if err := readFrame(&buf, DefaultMaxFrameLen, &got); err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if !proto.Equal(req, &got) {
		t.Fatalf("round-trip mismatch:\n got %v\nwant %v", &got, req)
	}
	if buf.Len() != 0 {
		t.Fatalf("reader left %d trailing bytes", buf.Len())
	}
}

func TestFrameLengthPrefixIsBigEndian(t *testing.T) {
	// A payload chosen so the body length spans multiple bytes, making endianness observable.
	req := &tephrapb.Request{
		RequestId: 1,
		Kind:      &tephrapb.Request_Append{Append: &tephrapb.AppendRequest{Events: []*tephrapb.Event{{Type: "T", Payload: bytes.Repeat([]byte("x"), 300)}}}},
	}
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var buf bytes.Buffer
	if err := writeFrame(&buf, req, DefaultMaxFrameLen); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	header := buf.Bytes()[:4]
	want := []byte{byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body))}
	if !bytes.Equal(header, want) {
		t.Fatalf("length prefix = %v, want big-endian %v (body len %d)", header, want, len(body))
	}
	if binary.BigEndian.Uint32(header) != uint32(len(body)) {
		t.Fatalf("big-endian decode = %d, want %d", binary.BigEndian.Uint32(header), len(body))
	}
}

func TestReadFrameCleanEOFAtBoundary(t *testing.T) {
	// An empty reader is a clean close between frames: io.EOF, distinct from a torn frame.
	var got tephrapb.Response
	err := readFrame(bytes.NewReader(nil), DefaultMaxFrameLen, &got)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("readFrame on empty = %v, want io.EOF", err)
	}
}

func TestReadFrameTornPrefix(t *testing.T) {
	var got tephrapb.Response
	err := readFrame(bytes.NewReader([]byte{0x00, 0x01}), DefaultMaxFrameLen, &got)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("readFrame on torn prefix = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestReadFrameTornBody(t *testing.T) {
	// A prefix promising 10 bytes but only 3 present: a torn frame, never an orderly close.
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], 10)
	r := bytes.NewReader(append(header[:], 1, 2, 3))
	var got tephrapb.Response
	err := readFrame(r, DefaultMaxFrameLen, &got)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("readFrame on torn body = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestReadFrameRejectsOversizedBeforeAlloc(t *testing.T) {
	// A hostile length prefix far larger than max, with no body: rejected on the length, so the
	// huge body is never allocated.
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], 1<<20)
	var got tephrapb.Response
	err := readFrame(bytes.NewReader(header[:]), 1024, &got)
	var tooLarge *FrameTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("readFrame oversized = %v, want *FrameTooLargeError", err)
	}
	if tooLarge.Len != 1<<20 || tooLarge.Max != 1024 {
		t.Fatalf("FrameTooLargeError = %+v, want Len=1048576 Max=1024", tooLarge)
	}
}

func TestWriteFrameRejectsOversized(t *testing.T) {
	req := &tephrapb.Request{
		RequestId: 1,
		Kind:      &tephrapb.Request_Append{Append: &tephrapb.AppendRequest{Events: []*tephrapb.Event{{Type: "T", Payload: bytes.Repeat([]byte("x"), 2048)}}}},
	}
	var buf bytes.Buffer
	err := writeFrame(&buf, req, 1024)
	var tooLarge *FrameTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("writeFrame oversized = %v, want *FrameTooLargeError", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("writeFrame wrote %d bytes for an oversized frame; wire must stay untouched", buf.Len())
	}
}
