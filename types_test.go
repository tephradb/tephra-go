package tephra

import (
	"strings"
	"testing"

	"github.com/tephradb/tephra-go/internal/tephrapb"
	"google.golang.org/protobuf/proto"
)

func TestNewEventSortsTagsAndValidates(t *testing.T) {
	ev, err := NewEvent("Enrolled", []string{"student:s1", "course:c1", "admin:a1"}, []byte("{}"))
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	got := ev.Tags()
	want := []string{"admin:a1", "course:c1", "student:s1"}
	if len(got) != len(want) {
		t.Fatalf("tags = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("tags = %v, want sorted %v", got, want)
		}
	}
	if ev.Type() != "Enrolled" {
		t.Fatalf("type = %q, want Enrolled", ev.Type())
	}
}

func TestNewEventRejectsEmptyType(t *testing.T) {
	if _, err := NewEvent("", nil, nil); err == nil {
		t.Fatal("NewEvent with empty type: want error")
	}
}

func TestNewEventRejectsEmptyTag(t *testing.T) {
	if _, err := NewEvent("T", []string{""}, nil); err == nil {
		t.Fatal("NewEvent with empty tag: want error")
	}
}

func TestNewEventRejectsOversizedName(t *testing.T) {
	if _, err := NewEvent(strings.Repeat("x", MaxNameLen+1), nil, nil); err == nil {
		t.Fatal("NewEvent with oversized type: want error")
	}
	// Exactly at the maximum is accepted.
	if _, err := NewEvent(strings.Repeat("x", MaxNameLen), nil, nil); err != nil {
		t.Fatalf("NewEvent at max name length: unexpected error %v", err)
	}
}

func TestNewEventRejectsDuplicateTag(t *testing.T) {
	if _, err := NewEvent("T", []string{"course:c1", "course:c1"}, nil); err == nil {
		t.Fatal("NewEvent with duplicate tag: want error")
	}
}

func TestQueryAllMapsToAll(t *testing.T) {
	pb := QueryAll().toPB()
	if !pb.GetAll() {
		t.Fatal("QueryAll should set all=true")
	}
	if len(pb.GetItems()) != 0 {
		t.Fatalf("QueryAll should have no items, got %d", len(pb.GetItems()))
	}
}

func TestEmptyItemsIsNotAll(t *testing.T) {
	// An empty item set matches nothing and must not collapse to the catch-all.
	pb := QueryItems().toPB()
	if pb.GetAll() {
		t.Fatal("QueryItems() must not set all=true")
	}
	if len(pb.GetItems()) != 0 {
		t.Fatalf("QueryItems() should have no items, got %d", len(pb.GetItems()))
	}
}

func TestQueryItemsRoundTripShape(t *testing.T) {
	byType, err := OfTypes("Registered")
	if err != nil {
		t.Fatalf("OfTypes: %v", err)
	}
	both, err := NewQueryItem([]string{"Enrolled"}, []string{"student:s1", "course:c1"})
	if err != nil {
		t.Fatalf("NewQueryItem: %v", err)
	}
	pb := QueryItems(byType, both).toPB()

	// Marshal/unmarshal to confirm the wire shape survives a proto round-trip.
	raw, err := proto.Marshal(pb)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got tephrapb.Query
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.GetAll() {
		t.Fatal("items query must not be all")
	}
	if len(got.GetItems()) != 2 {
		t.Fatalf("items = %d, want 2", len(got.GetItems()))
	}
	if got.GetItems()[0].GetTypes()[0] != "Registered" {
		t.Fatalf("item 0 type = %q, want Registered", got.GetItems()[0].GetTypes()[0])
	}
	// Item tags are stored sorted.
	if tags := got.GetItems()[1].GetTags(); tags[0] != "course:c1" || tags[1] != "student:s1" {
		t.Fatalf("item 1 tags = %v, want sorted [course:c1 student:s1]", tags)
	}
}

func TestConditionToPB(t *testing.T) {
	item, err := WithTags("course:c1")
	if err != nil {
		t.Fatalf("WithTags: %v", err)
	}
	cond := NewAppendCondition(QueryItems(item))
	cond.After = 42
	pb := cond.toPB()
	if pb.GetAfter() != 42 {
		t.Fatalf("after = %d, want 42", pb.GetAfter())
	}
	if pb.GetFailIfEventsMatch().GetAll() {
		t.Fatal("condition query should be items, not all")
	}
}

func TestServerErrorFromPB(t *testing.T) {
	pos := uint64(7)
	pbErr := &tephrapb.ErrorResponse{
		Code:             tephrapb.ErrorCode_ERROR_CODE_CONFLICT,
		Message:          "conflict",
		Retryable:        true,
		ConflictPosition: &pos,
	}
	se := serverErrorFromPB(pbErr)
	if se.Code != ErrCodeConflict {
		t.Fatalf("code = %v, want conflict", se.Code)
	}
	if !se.Retryable {
		t.Fatal("retryable should be true")
	}
	if se.ConflictPosition == nil || *se.ConflictPosition != 7 {
		t.Fatalf("conflict position = %v, want 7", se.ConflictPosition)
	}
}
