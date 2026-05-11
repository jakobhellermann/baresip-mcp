package baresip

import (
	"sync"
	"time"
)

// RecordedEvent is an Event plus the time it was received.
type RecordedEvent struct {
	At    time.Time      `json:"at"`
	Class string         `json:"class,omitempty"`
	Type  string         `json:"type,omitempty"`
	Param string         `json:"param,omitempty"`
	Extra map[string]any `json:"extra,omitempty"`
}

// EventBuffer is a fixed-capacity ring buffer of recent events.
type EventBuffer struct {
	mu   sync.Mutex
	buf  []RecordedEvent
	cap  int
	next int
	full bool
}

func NewEventBuffer(capacity int) *EventBuffer {
	if capacity <= 0 {
		capacity = 128
	}
	return &EventBuffer{buf: make([]RecordedEvent, capacity), cap: capacity}
}

func (b *EventBuffer) Add(ev Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf[b.next] = RecordedEvent{
		At:    time.Now().UTC(),
		Class: ev.Class,
		Type:  ev.Type,
		Param: ev.Param,
		Extra: ev.Extra,
	}
	b.next = (b.next + 1) % b.cap
	if b.next == 0 {
		b.full = true
	}
}

// Snapshot returns up to limit most recent events, oldest first.
// limit <= 0 returns all available events.
func (b *EventBuffer) Snapshot(limit int) []RecordedEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Always return a non-nil slice so JSON encoders emit [] rather than null.
	ordered := make([]RecordedEvent, 0, b.cap)
	if b.full {
		ordered = append(ordered, b.buf[b.next:]...)
		ordered = append(ordered, b.buf[:b.next]...)
	} else {
		ordered = append(ordered, b.buf[:b.next]...)
	}
	if limit > 0 && len(ordered) > limit {
		ordered = ordered[len(ordered)-limit:]
	}
	return ordered
}
