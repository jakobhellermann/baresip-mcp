package baresip

import (
	"testing"
	"time"
)

func TestFanoutDeliversToAllSubscribers(t *testing.T) {
	f := NewEventFanout()
	a, unsubA := f.Subscribe()
	b, unsubB := f.Subscribe()
	defer unsubA()
	defer unsubB()

	f.Publish(Event{Type: "X"})

	for _, ch := range []<-chan Event{a, b} {
		select {
		case got := <-ch:
			if got.Type != "X" {
				t.Fatalf("type = %q", got.Type)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for event")
		}
	}
}

func TestFanoutUnsubscribeStopsDelivery(t *testing.T) {
	f := NewEventFanout()
	ch, unsub := f.Subscribe()
	unsub()
	f.Publish(Event{Type: "X"})
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel closed after unsubscribe")
		}
	case <-time.After(100 * time.Millisecond):
		// Closed channel reads immediately; if we time out, channel is leaked.
		t.Fatal("channel not closed by unsubscribe")
	}
}

func TestFanoutSlowSubscriberDoesNotBlock(t *testing.T) {
	f := NewEventFanout()
	_, unsub := f.Subscribe() // never reads
	defer unsub()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			f.Publish(Event{Type: "X"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("publish blocked on slow subscriber")
	}
}
