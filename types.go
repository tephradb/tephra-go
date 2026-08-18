package tephra

import (
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/tqwewe/tephra-go/internal/tephrapb"
)

// MaxNameLen is the maximum length, in bytes, of an event type or tag. The engine stores each
// with a fixed-width uint16 length, so the field capacity is the limit (u16::MAX).
const MaxNameLen = int(math.MaxUint16)

// Position is a 1-based global position in the log. Position 0 is "before everything".
type Position uint64

const (
	// Zero is the position before the first event: the start cursor for a forward read, and the
	// "consider the whole log" bound for an append condition.
	Zero Position = 0
	// Max is the largest representable position, the "from the tip" cursor for a backward read:
	// ReadBack(ctx, q, Max, limit) streams newest-first from the current durable tip.
	Max Position = math.MaxUint64
)

func (p Position) String() string { return strconv.FormatUint(uint64(p), 10) }

// Event is an event to append: a non-empty type, a set of tags, and an opaque payload. Build one
// with NewEvent, which validates the type and tags the same way the server does.
type Event struct {
	eventType string
	tags      []string // validated, sorted, duplicate-free
	payload   []byte
}

// NewEvent builds an event, validating the type and tags (each non-empty and at most MaxNameLen
// bytes) and rejecting a duplicate tag. Tags are stored sorted so identical sets encode identically.
func NewEvent(eventType string, tags []string, payload []byte) (Event, error) {
	if err := validateName(eventType, "event type"); err != nil {
		return Event{}, err
	}
	sorted, err := validatedTagSet(tags)
	if err != nil {
		return Event{}, err
	}
	return Event{eventType: eventType, tags: sorted, payload: payload}, nil
}

// Type is the event's type.
func (e Event) Type() string { return e.eventType }

// Tags are the event's tags, sorted. The returned slice is read-only; do not mutate it.
func (e Event) Tags() []string { return e.tags }

// Payload is the event's opaque payload. The returned slice is read-only; do not mutate it.
func (e Event) Payload() []byte { return e.payload }

// SequencedEvent is an event together with the position it was assigned in the global order. It
// embeds Event, so Type, Tags, and Payload are available directly.
type SequencedEvent struct {
	Position Position
	Event
}

// AppendResult is the outcome of a successful append: the position range the batch was assigned.
type AppendResult struct {
	First Position
	Last  Position
}

// Stats is a snapshot of a server's operational state, returned by Client.Stats.
type Stats struct {
	// EventCount is the total durable events, which (positions being dense and 1-based) is also
	// the tip position.
	EventCount uint64
	// SegmentCount is the number of on-disk log segments in the data directory.
	SegmentCount uint64
	// DiskBytes is the total bytes on disk in the data directory (log segments plus index sidecars).
	DiskBytes uint64
	// UptimeSeconds is the seconds since the server began accepting connections.
	UptimeSeconds uint64
	// ActiveConnections is the connections currently being served, including this one.
	ActiveConnections uint64
	// ActiveSubscriptions is the live subscriptions across all connections.
	ActiveSubscriptions uint64
	// ConnectionsRefused is the connections refused at the connection cap. Monotonic.
	ConnectionsRefused uint64
	// ConnectionsReaped is the connections reaped for a handshake, idle, or incomplete-frame
	// timeout. Monotonic.
	ConnectionsReaped uint64
	// MaxConnections is the server's configured maximum concurrent connections, or 0 when unlimited.
	MaxConnections uint64
	// Version is the server's crate version.
	Version string
}

// SubEventKind distinguishes the two kinds of item a subscription yields.
type SubEventKind int

const (
	// SubEventEvent is a matching event.
	SubEventEvent SubEventKind = iota
	// SubEventCaughtUp is a live-edge marker: the subscription drained everything up to Watermark
	// and is now tailing live. Re-armed after each subsequent catch-up burst.
	SubEventCaughtUp
)

// SubEvent is one item from a SubscribeStream: either a matching event (Kind == SubEventEvent,
// Event set) or a live-edge marker (Kind == SubEventCaughtUp, Watermark set).
type SubEvent struct {
	Kind      SubEventKind
	Event     SequencedEvent
	Watermark Position
}

// IsCaughtUp reports whether this item is a live-edge marker rather than an event.
func (s SubEvent) IsCaughtUp() bool { return s.Kind == SubEventCaughtUp }

// QueryItem is one alternative in a Query: a type constraint AND a tag constraint. An event
// matches when its type is one of Types (an empty type list matches any type) and its tags
// contain all of Tags. Build one with OfTypes, WithTags, or NewQueryItem.
type QueryItem struct {
	types []string
	tags  []string
}

// NewQueryItem builds an item constraining on both types (OR'd; empty means any type) and tags
// (AND'd; the event must contain all). Names are validated and a duplicate tag is rejected.
func NewQueryItem(types []string, tags []string) (QueryItem, error) {
	vt, err := validatedTypes(types)
	if err != nil {
		return QueryItem{}, err
	}
	vtags, err := validatedTagSet(tags)
	if err != nil {
		return QueryItem{}, err
	}
	return QueryItem{types: vt, tags: vtags}, nil
}

// OfTypes builds an item constraining only on type (matching any tags).
func OfTypes(types ...string) (QueryItem, error) { return NewQueryItem(types, nil) }

// WithTags builds an item constraining only on tags (matching any type).
func WithTags(tags ...string) (QueryItem, error) { return NewQueryItem(nil, tags) }

// Query selects which events a read, subscription, or append condition covers. It is either the
// catch-all (QueryAll) or a set of items OR'd together (QueryItems). An empty item set matches
// nothing, which is deliberately distinct from the catch-all.
type Query struct {
	all   bool
	items []QueryItem
}

// QueryAll is the catch-all query, matching every event.
func QueryAll() Query { return Query{all: true} }

// QueryItems is a query over a set of items OR'd together. With no items it matches nothing.
func QueryItems(items ...QueryItem) Query { return Query{items: items} }

// AppendCondition guards an append: the append is rejected if any event after After matches
// FailIfEventsMatch. After is an exclusive lower bound; Zero (the default) considers the whole
// log, i.e. fail if any event matches. Build one with NewAppendCondition, then optionally set After.
type AppendCondition struct {
	FailIfEventsMatch Query
	After             Position
}

// NewAppendCondition builds a condition checking the whole log (After == Zero): fail if any event
// matches. Set the After field afterward to only check events strictly after a position.
func NewAppendCondition(failIfEventsMatch Query) AppendCondition {
	return AppendCondition{FailIfEventsMatch: failIfEventsMatch}
}

// Limit returns a pointer to n, for passing an explicit result cap to Read/ReadBack. A nil limit
// means unlimited; Limit(0) is a real cap of zero (return nothing).
func Limit(n uint64) *uint64 { return &n }

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

func validateName(s, what string) error {
	if s == "" {
		return fmt.Errorf("tephra: %s must not be empty", what)
	}
	if len(s) > MaxNameLen {
		return fmt.Errorf("tephra: %s is %d bytes, exceeding the %d-byte maximum", what, len(s), MaxNameLen)
	}
	return nil
}

// validatedTagSet validates each tag, then returns a sorted, duplicate-free copy. A duplicate is
// an error rather than being silently deduped, so an event never round-trips to something the
// caller did not submit.
func validatedTagSet(tags []string) ([]string, error) {
	out := slices.Clone(tags)
	for _, tag := range out {
		if err := validateName(tag, "tag"); err != nil {
			return nil, err
		}
	}
	slices.Sort(out)
	for i := 1; i < len(out); i++ {
		if out[i] == out[i-1] {
			return nil, fmt.Errorf("tephra: duplicate tag %q", out[i])
		}
	}
	return out, nil
}

// validatedTypes validates each type name, preserving order (types are OR'd and low cardinality,
// so they are neither sorted nor deduped).
func validatedTypes(types []string) ([]string, error) {
	out := slices.Clone(types)
	for _, ty := range out {
		if err := validateName(ty, "event type"); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Wire conversions
// ---------------------------------------------------------------------------

func (e Event) toPB() *tephrapb.Event {
	return &tephrapb.Event{
		Type:    e.eventType,
		Tags:    e.tags,
		Payload: e.payload,
	}
}

func (q Query) toPB() *tephrapb.Query {
	out := &tephrapb.Query{}
	if q.all {
		out.All = true
		return out
	}
	out.Items = make([]*tephrapb.QueryItem, len(q.items))
	for i, item := range q.items {
		out.Items[i] = &tephrapb.QueryItem{
			Types: item.types,
			Tags:  item.tags,
		}
	}
	return out
}

func (c AppendCondition) toPB() *tephrapb.AppendCondition {
	return &tephrapb.AppendCondition{
		FailIfEventsMatch: c.FailIfEventsMatch.toPB(),
		After:             uint64(c.After),
	}
}

// sequencedFromPB builds a SequencedEvent from a wire message. The server is the source of truth
// for a stored event: it validated the event through tephra's own constructors on append and
// stores tags in canonical sorted order, so the fields are taken verbatim here rather than
// re-validated or re-sorted on the read hot path (that would waste a clone and sort per event, and
// could reject an event the server legitimately stored). A frame carrying no event is the one thing
// the server never sends, so it is treated as a protocol error.
func sequencedFromPB(se *tephrapb.SequencedEvent) (SequencedEvent, error) {
	ev := se.GetEvent()
	if ev == nil {
		return SequencedEvent{}, protocolErrorf("server sent a sequenced event with no event")
	}
	return SequencedEvent{
		Position: Position(se.GetPosition()),
		Event: Event{
			eventType: ev.GetType(),
			tags:      ev.GetTags(),
			payload:   ev.GetPayload(),
		},
	}, nil
}

func statsFromPB(s *tephrapb.StatsResponse) Stats {
	return Stats{
		EventCount:          s.GetEventCount(),
		SegmentCount:        s.GetSegmentCount(),
		DiskBytes:           s.GetDiskBytes(),
		UptimeSeconds:       s.GetUptimeSeconds(),
		ActiveConnections:   s.GetActiveConnections(),
		ActiveSubscriptions: s.GetActiveSubscriptions(),
		ConnectionsRefused:  s.GetConnectionsRefused(),
		ConnectionsReaped:   s.GetConnectionsReaped(),
		MaxConnections:      s.GetMaxConnections(),
		Version:             s.GetVersion(),
	}
}
