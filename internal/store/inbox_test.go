package store

import (
	"fmt"
	"testing"
	"time"
)

func TestInboxKeepsOrderAndReadFlag(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, err := s.Inbox()
	if err != nil || len(got) != 0 {
		t.Fatalf("empty inbox = %v, %v", got, err)
	}
	now := time.Now()
	for i := 1; i <= 3; i++ {
		e := InboxEntry{ID: fmt.Sprintf("i-%d", i), Kind: InboxKindFailure, Title: "t", Message: "m", SentAt: now.Add(time.Duration(i) * time.Second)}
		if err := s.AddInboxEntry(e); err != nil {
			t.Fatalf("AddInboxEntry %d: %v", i, err)
		}
	}
	if err := s.UpdateInbox(func(list *[]InboxEntry) error {
		(*list)[1].Read = true
		return nil
	}); err != nil {
		t.Fatalf("UpdateInbox: %v", err)
	}
	got, err = s.Inbox()
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(got) != 3 || got[0].ID != "i-1" || got[2].ID != "i-3" {
		t.Fatalf("inbox order = %+v", got)
	}
	if got[0].Read || !got[1].Read || got[2].Read {
		t.Fatalf("read flags = %v %v %v, want only the second read", got[0].Read, got[1].Read, got[2].Read)
	}
}

func TestInboxRefusesDuplicateAndMissingIDs(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.AddInboxEntry(InboxEntry{Title: "no id"}); err == nil {
		t.Fatal("an entry without an id was accepted")
	}
	if err := s.AddInboxEntry(InboxEntry{ID: "i-1"}); err != nil {
		t.Fatalf("first add: %v", err)
	}
	if err := s.AddInboxEntry(InboxEntry{ID: "i-1"}); err == nil {
		t.Fatal("a duplicate id was accepted")
	}
}

func TestInboxDropsTheOldestBeyondTheLimit(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Seed a full inbox in one write; adding entry by entry would write the
	// file a thousand times.
	if err := s.UpdateInbox(func(list *[]InboxEntry) error {
		for i := 0; i < InboxLimit+4; i++ {
			*list = append(*list, InboxEntry{ID: fmt.Sprintf("i-%03d", i)})
		}
		return nil
	}); err != nil {
		t.Fatalf("seed inbox: %v", err)
	}
	if err := s.AddInboxEntry(InboxEntry{ID: fmt.Sprintf("i-%03d", InboxLimit+4)}); err != nil {
		t.Fatalf("AddInboxEntry: %v", err)
	}
	got, err := s.Inbox()
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(got) != InboxLimit {
		t.Fatalf("inbox holds %d entries, want %d", len(got), InboxLimit)
	}
	if got[0].ID != "i-005" || got[len(got)-1].ID != fmt.Sprintf("i-%03d", InboxLimit+4) {
		t.Fatalf("the oldest entries were not the ones dropped: first %q, last %q", got[0].ID, got[len(got)-1].ID)
	}
}

// An unread failure is the one notification that still asks for a look; it
// keeps its place however old it is, while read entries and other kinds make
// room for new ones.
func TestInboxKeepsUnreadFailuresBeyondTheLimit(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.UpdateInbox(func(list *[]InboxEntry) error {
		*list = append(*list,
			InboxEntry{ID: "fail-unread", Kind: InboxKindFailure},
			InboxEntry{ID: "fail-read", Kind: InboxKindFailure, Read: true},
			InboxEntry{ID: "artifact-unread", Kind: InboxKindArtifact},
		)
		for i := 0; i < InboxLimit+4; i++ {
			*list = append(*list, InboxEntry{ID: fmt.Sprintf("i-%04d", i), Kind: InboxKindSuccess})
		}
		return nil
	}); err != nil {
		t.Fatalf("seed inbox: %v", err)
	}
	if err := s.AddInboxEntry(InboxEntry{ID: fmt.Sprintf("i-%04d", InboxLimit+4), Kind: InboxKindSuccess}); err != nil {
		t.Fatalf("AddInboxEntry: %v", err)
	}
	got, err := s.Inbox()
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(got) != InboxLimit {
		t.Fatalf("inbox holds %d entries, want %d", len(got), InboxLimit)
	}
	if got[0].ID != "fail-unread" {
		t.Fatalf("first entry %q, want the unread failure kept", got[0].ID)
	}
	for _, e := range got {
		if e.ID == "fail-read" || e.ID == "artifact-unread" {
			t.Fatalf("%s should have made room", e.ID)
		}
	}
	if got[1].ID != "i-0006" || got[len(got)-1].ID != fmt.Sprintf("i-%04d", InboxLimit+4) {
		t.Fatalf("newest entries not kept: second %q, last %q", got[1].ID, got[len(got)-1].ID)
	}
}
