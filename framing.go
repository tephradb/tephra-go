package tephra

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"google.golang.org/protobuf/proto"
)

// DefaultMaxFrameLen is the default cap on a single frame's body length (16 MiB). It bounds
// per-frame memory and lets a hostile or corrupt length be rejected before it is allocated for.
// It matches the tephra server and Rust client default (tephra-proto's DEFAULT_MAX_FRAME_LEN).
const DefaultMaxFrameLen uint32 = 16 * 1024 * 1024

// A frame is a 4-byte big-endian uint32 length prefix followed by that many bytes of a
// serialized protobuf message. This is the whole wire protocol: no magic, no version, no
// handshake. The tephra server and client share exactly this framing.

// writeFrame serializes msg and writes it as one length-prefixed frame. It does not flush; the
// caller flushes at a send boundary. A body larger than maxFrameLen is rejected before any byte
// reaches the wire, so the stream stays frame-aligned.
func writeFrame(w io.Writer, msg proto.Message, maxFrameLen uint32) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal frame: %w", err)
	}
	if uint64(len(body)) > uint64(maxFrameLen) {
		return &FrameTooLargeError{Len: capUint32(len(body)), Max: maxFrameLen}
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(body)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.Write(body); err != nil {
		return err
	}
	return nil
}

// readFrame reads one length-prefixed frame from r and unmarshals it into msg.
//
// It distinguishes an orderly close from a torn frame, exactly as the Rust framing does:
//   - A clean EOF at a frame boundary (the peer closed between frames, before the length prefix)
//     returns io.EOF. Callers treat this as "no more frames".
//   - An EOF partway through the length prefix or body is a torn frame and returns
//     io.ErrUnexpectedEOF.
//
// A length exceeding maxFrameLen is rejected with *FrameTooLargeError before the body is
// allocated, bounding memory against a hostile or corrupt prefix.
func readFrame(r io.Reader, maxFrameLen uint32, msg proto.Message) error {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		// io.ReadFull maps a zero-byte read to io.EOF (clean boundary close) and a partial read
		// to io.ErrUnexpectedEOF (torn prefix); return both verbatim.
		return err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n > maxFrameLen {
		return &FrameTooLargeError{Len: n, Max: maxFrameLen}
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		// The prefix promised n bytes; a close now is a torn frame, never an orderly one.
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	return proto.Unmarshal(body, msg)
}

// capUint32 clamps a length to uint32 for reporting an oversized body without overflow.
func capUint32(n int) uint32 {
	if n > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(n)
}
