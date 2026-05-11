package baresip

import "testing"

func TestEventBufferRingWraps(t *testing.T) {
	b := NewEventBuffer(3)
	for _, typ := range []string{"a", "b", "c", "d", "e"} {
		b.Add(Event{Type: typ})
	}
	snap := b.Snapshot(0)
	if len(snap) != 3 {
		t.Fatalf("expected 3 events, got %d", len(snap))
	}
	got := []string{snap[0].Type, snap[1].Type, snap[2].Type}
	want := []string{"c", "d", "e"}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("order mismatch: got %v want %v", got, want)
		}
	}
}

func TestEventBufferLimit(t *testing.T) {
	b := NewEventBuffer(5)
	for _, typ := range []string{"a", "b", "c"} {
		b.Add(Event{Type: typ})
	}
	snap := b.Snapshot(2)
	if len(snap) != 2 {
		t.Fatalf("expected 2 events, got %d", len(snap))
	}
	if snap[0].Type != "b" || snap[1].Type != "c" {
		t.Fatalf("expected [b c], got [%s %s]", snap[0].Type, snap[1].Type)
	}
}

func TestEventBufferEmpty(t *testing.T) {
	b := NewEventBuffer(4)
	if got := b.Snapshot(0); len(got) != 0 {
		t.Fatalf("expected empty snapshot, got %d", len(got))
	}
}
