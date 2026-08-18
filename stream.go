package tephra

import (
	"context"
	"sync"

	"github.com/tqwewe/tephra-go/internal/tephrapb"
)

// streamDelivery is the uniform way the connection reader hands a streaming request its data:
// event batches (deliverEvents, which returns an error for a malformed batch) and terminal errors
// (deliverError). Kind-specific frames (a read's end watermark, a subscription's caught-up marker)
// are delivered by the reader through the concrete stream types instead (ReadStream.end,
// SubscribeStream.caughtUp), so a wrong-kind frame is a protocol error the reader handles.
type streamDelivery interface {
	deliverEvents(events []*tephrapb.SequencedEvent) error
	deliverError(err error)
}

// streamState is a stream's unbounded delivery buffer plus its terminal state.
//
// It is deliberately unbounded: the connection reader appends here and never blocks, so a slow
// stream consumer cannot stall the shared socket and delay unrelated requests (the head-of-line
// blocking the Rust client eliminated). Overall memory is bounded by the in-flight request budget
// (the number of concurrent streams), not by any single consumer's pace.
type streamState[T any] struct {
	mu           sync.Mutex
	items        []T
	head         int
	err          error // terminal error, or nil for a clean end
	done         bool  // a terminal frame arrived; no more items will come
	closed       bool  // the consumer stopped the stream (Close or ctx)
	watermark    Position
	hasWatermark bool
	notify       chan struct{} // buffered(1); pinged on any state change
}

func newStreamState[T any]() *streamState[T] {
	return &streamState[T]{notify: make(chan struct{}, 1)}
}

func (s *streamState[T]) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// push appends items unless the stream is already finished or closed.
func (s *streamState[T]) push(items ...T) {
	s.mu.Lock()
	if !s.closed && !s.done && s.err == nil {
		s.items = append(s.items, items...)
	}
	s.mu.Unlock()
	s.signal()
}

// terminate records a terminal error (first one wins) and marks the stream done.
func (s *streamState[T]) terminate(err error) {
	s.mu.Lock()
	if s.err == nil && !s.done {
		s.err = err
		s.done = true
	}
	s.mu.Unlock()
	s.signal()
}

// end marks a clean end, carrying the watermark the read was pinned to.
func (s *streamState[T]) end(watermark Position) {
	s.mu.Lock()
	if !s.done && s.err == nil {
		s.done = true
		s.watermark = watermark
		s.hasWatermark = true
	}
	s.mu.Unlock()
	s.signal()
}

// waitItem blocks until an item is available, the stream finishes, the consumer closes it, or ctx
// is done. It returns (item, true, nil) for an item; (zero, false, nil) when finished or closed;
// or (zero, false, ctx.Err()) on cancellation, without mutating terminal state.
func (s *streamState[T]) waitItem(ctx context.Context) (T, bool, error) {
	var zero T
	for {
		s.mu.Lock()
		if s.head < len(s.items) {
			item := s.items[s.head]
			s.items[s.head] = zero // release the reference for GC
			s.head++
			if s.head == len(s.items) {
				s.items = s.items[:0]
				s.head = 0
			}
			s.mu.Unlock()
			return item, true, nil
		}
		if s.closed || s.done || s.err != nil {
			s.mu.Unlock()
			return zero, false, nil
		}
		s.mu.Unlock()

		select {
		case <-s.notify:
		case <-ctx.Done():
			return zero, false, ctx.Err()
		}
	}
}

// streamBase carries the shared machinery of ReadStream and SubscribeStream.
type streamBase[T any] struct {
	cn        *conn
	id        uint64
	ctx       context.Context
	st        *streamState[T]
	cur       T
	closeOnce sync.Once
}

// Next advances to the next item, returning false when the stream ends, is closed, or ctx is
// cancelled. After it returns false, Err reports why (nil for a clean end).
func (b *streamBase[T]) Next() bool {
	item, ok, ctxErr := b.st.waitItem(b.ctx)
	if ok {
		b.cur = item
		return true
	}
	if ctxErr != nil {
		b.finish(ctxErr)
	}
	return false
}

// Err returns the stream's terminal error, or nil if it ended cleanly or is still open.
func (b *streamBase[T]) Err() error {
	b.st.mu.Lock()
	defer b.st.mu.Unlock()
	return b.st.err
}

// Close stops the stream and releases its resources. If the stream had not already finished, it
// cancels the request server-side (a best-effort CancelRequest on the same connection). It is
// idempotent and safe to call from any goroutine.
func (b *streamBase[T]) Close() error {
	b.finish(nil)
	return nil
}

// finish runs the teardown once. cause is the reason to record (a ctx error), or nil for an
// ordinary Close. It marks the stream closed, finalizes the request on the connection (releasing
// its in-flight slot if still pending), and cancels server-side unless the server already ended it.
func (b *streamBase[T]) finish(cause error) {
	b.closeOnce.Do(func() {
		b.st.mu.Lock()
		serverEnded := b.st.done || b.st.err != nil
		if cause != nil && b.st.err == nil && !b.st.done {
			b.st.err = cause
			b.st.done = true
		}
		b.st.closed = true
		b.st.mu.Unlock()
		b.st.signal()

		if b.cn.finalize(b.id) != nil && !serverEnded {
			b.cn.sendCancel(b.id)
		}
	})
}

// ReadStream is a streaming iterator over the events of one read, in ascending position order
// (descending for ReadBack). Drive it with Next/Event, then check Err. Watermark is available once
// the stream has ended cleanly. Close it when done; a partially consumed read is cancelled
// server-side.
//
// A ReadStream is not safe for concurrent use by multiple goroutines (aside from Close, which may
// be called from any goroutine).
type ReadStream struct {
	streamBase[SequencedEvent]
}

// Event returns the event yielded by the most recent Next.
func (rs *ReadStream) Event() SequencedEvent { return rs.cur }

// Watermark returns the watermark the read was pinned to and true, once the stream has ended
// cleanly; it returns (0, false) before then.
func (rs *ReadStream) Watermark() (Position, bool) {
	rs.st.mu.Lock()
	defer rs.st.mu.Unlock()
	return rs.st.watermark, rs.st.hasWatermark
}

func (rs *ReadStream) deliverEvents(events []*tephrapb.SequencedEvent) error {
	decoded := make([]SequencedEvent, 0, len(events))
	for _, e := range events {
		sev, err := sequencedFromPB(e)
		if err != nil {
			rs.st.terminate(err)
			return err
		}
		decoded = append(decoded, sev)
	}
	rs.st.push(decoded...)
	return nil
}

func (rs *ReadStream) deliverError(err error) { rs.st.terminate(err) }

// end marks the read complete at the given watermark. Called by the connection reader on ReadEnd.
func (rs *ReadStream) end(watermark Position) { rs.st.end(watermark) }

// SubscribeStream is a streaming iterator over a live subscription. It yields SubEvents (events
// and re-armed caught-up markers) indefinitely until Close, ctx cancellation, an error, or the
// connection closing. Drive it with Next/Item, then check Err.
//
// A SubscribeStream is not safe for concurrent use by multiple goroutines (aside from Close, which
// may be called from any goroutine).
type SubscribeStream struct {
	streamBase[SubEvent]
}

// Item returns the SubEvent yielded by the most recent Next.
func (ss *SubscribeStream) Item() SubEvent { return ss.cur }

func (ss *SubscribeStream) deliverEvents(events []*tephrapb.SequencedEvent) error {
	decoded := make([]SubEvent, 0, len(events))
	for _, e := range events {
		sev, err := sequencedFromPB(e)
		if err != nil {
			ss.st.terminate(err)
			return err
		}
		decoded = append(decoded, SubEvent{Kind: SubEventEvent, Event: sev})
	}
	ss.st.push(decoded...)
	return nil
}

func (ss *SubscribeStream) deliverError(err error) { ss.st.terminate(err) }

// caughtUp delivers a live-edge marker. Called by the connection reader on SubscribeCaughtUp.
func (ss *SubscribeStream) caughtUp(watermark Position) {
	ss.st.push(SubEvent{Kind: SubEventCaughtUp, Watermark: watermark})
}

// drainRead consumes a read fully, returning the events and the watermark it was pinned to.
func drainRead(rs *ReadStream) ([]SequencedEvent, Position, error) {
	defer rs.Close()
	var events []SequencedEvent
	for rs.Next() {
		events = append(events, rs.Event())
	}
	if err := rs.Err(); err != nil {
		return nil, 0, err
	}
	watermark, ok := rs.Watermark()
	if !ok {
		return nil, 0, protocolErrorf("read ended without a watermark")
	}
	return events, watermark, nil
}
