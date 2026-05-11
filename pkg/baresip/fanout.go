package baresip

import "sync"

// EventFanout multiplexes a single source of events to multiple
// subscribers. Each subscriber gets its own buffered channel; slow
// subscribers don't block the publisher (events are dropped for them).
type EventFanout struct {
	mu   sync.Mutex
	subs map[chan Event]struct{}
}

func NewEventFanout() *EventFanout {
	return &EventFanout{subs: map[chan Event]struct{}{}}
}

// Subscribe returns a channel of events plus an unsubscribe function.
// The channel is buffered; events are dropped if the consumer is slow.
func (f *EventFanout) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, 32)
	f.mu.Lock()
	f.subs[ch] = struct{}{}
	f.mu.Unlock()
	return ch, func() {
		f.mu.Lock()
		if _, ok := f.subs[ch]; ok {
			delete(f.subs, ch)
			close(ch)
		}
		f.mu.Unlock()
	}
}

// Publish broadcasts ev to all current subscribers without blocking.
func (f *EventFanout) Publish(ev Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for ch := range f.subs {
		select {
		case ch <- ev:
		default:
			// Drop for this slow subscriber.
		}
	}
}
