package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sipgate/baresip-mcp/pkg/baresip"
)

// isUDPPortFree checks 0.0.0.0:<port> because baresip binds to 0.0.0.0,
// which conflicts with anything already bound to the same port on any
// local interface.
func isUDPPortFree(port int) bool {
	conn, err := net.ListenPacket("udp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// Fleet manages N baresip child processes — one per account in the
// user's accounts file — and a single aggregated event stream. Tools
// route by AOR; calls between two of our own accounts work because the
// SIP legs live in separate OS processes (separate sockets, separate
// state).
type Fleet struct {
	mu      sync.Mutex
	clients map[string]*baresip.Client      // aor -> client
	insts   map[string]*baresipInstance     // aor -> instance
	calls   map[string]string               // call_id -> aor
	aors    []string                        // accounts file order
	fanout  *baresip.EventFanout
	events  *baresip.EventBuffer
}

// aorRE finds <sip:user@host> style URIs in an account line.
var aorRE = regexp.MustCompile(`<(sips?:[^>]+)>`)

func extractAOR(accountLine string) string {
	m := aorRE.FindStringSubmatch(accountLine)
	if m == nil {
		return ""
	}
	uri := m[1]
	// strip any URI params/headers from the AOR (everything after first ';' or '?')
	for _, sep := range []string{";", "?"} {
		if i := strings.Index(uri, sep); i >= 0 {
			uri = uri[:i]
		}
	}
	return uri
}

// LoadAccounts returns the active (non-comment, non-empty) lines from
// the given accounts file in order.
func loadAccountLines(path string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var lines []string
	for _, l := range strings.Split(string(raw), "\n") {
		trim := strings.TrimSpace(l)
		if trim == "" || strings.HasPrefix(trim, "#") {
			continue
		}
		lines = append(lines, trim)
	}
	return lines, nil
}

func NewFleet(ctx context.Context, accountsPath string, bufSize int) (*Fleet, error) {
	lines, err := loadAccountLines(accountsPath)
	if err != nil {
		return nil, fmt.Errorf("read accounts file %s: %w", accountsPath, err)
	}
	if len(lines) == 0 {
		return nil, fmt.Errorf("no active (uncommented) accounts in %s", accountsPath)
	}

	f := &Fleet{
		clients: map[string]*baresip.Client{},
		insts:   map[string]*baresipInstance{},
		calls:   map[string]string{},
		fanout:  baresip.NewEventFanout(),
		events:  baresip.NewEventBuffer(bufSize),
	}

	port5060Taken := !isUDPPortFree(5060)
	if port5060Taken {
		log.Printf("fleet: udp/5060 is already in use; first instance will use a random port too")
	}
	for i, line := range lines {
		aor := extractAOR(line)
		if aor == "" {
			log.Printf("fleet: skipping account line without parseable AOR: %s", line)
			continue
		}
		// First instance prefers the well-known port 5060 to maximize NAT
		// pinhole stability; the rest get auto-assigned ports.
		sipPort := 0
		if i == 0 && !port5060Taken {
			sipPort = 5060
		}
		inst, err := spawnBaresip(spawnParams{
			sipPort:      sipPort,
			accountsLine: line,
		})
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("spawn baresip for %s: %w", aor, err)
		}
		client := baresip.New(inst.addr)
		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := client.Connect(dialCtx); err != nil {
			cancel()
			inst.Close()
			f.Close()
			return nil, fmt.Errorf("connect baresip for %s: %w", aor, err)
		}
		cancel()

		f.mu.Lock()
		f.clients[aor] = client
		f.insts[aor] = inst
		f.aors = append(f.aors, aor)
		f.mu.Unlock()
		log.Printf("fleet: spawned baresip for %s on ctrl_tcp=%s sip_port=%d tmpdir=%s", aor, inst.addr, sipPort, inst.tmpDir)

		go f.drainEvents(aor, client)
	}

	return f, nil
}

// drainEvents forwards one client's events into the shared ring buffer,
// fanout, and stderr log, while also tracking call_id → AOR so call-
// targeted tools (hangup, accept) can find the right baresip.
func (f *Fleet) drainEvents(aor string, c *baresip.Client) {
	for ev := range c.Events() {
		// Stamp the AOR onto every event for downstream consumers.
		if ev.Extra == nil {
			ev.Extra = map[string]any{}
		}
		ev.Extra["fleet_aor"] = aor

		if id, _ := ev.Extra["id"].(string); id != "" {
			f.mu.Lock()
			f.calls[id] = aor
			f.mu.Unlock()
		}

		f.events.Add(ev)
		f.fanout.Publish(ev)
		log.Printf("baresip[%s] event: class=%s type=%s param=%s", aor, ev.Class, ev.Type, ev.Param)
	}
}

func (f *Fleet) Close() {
	f.mu.Lock()
	for _, c := range f.clients {
		_ = c.Close()
	}
	for _, inst := range f.insts {
		inst.Close()
	}
	f.mu.Unlock()
}

// AccountAORs returns every AOR the fleet is hosting, in accounts-file order.
func (f *Fleet) AccountAORs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.aors))
	copy(out, f.aors)
	return out
}

// ClientFor returns the baresip client hosting the given AOR.
func (f *Fleet) ClientFor(aor string) (*baresip.Client, error) {
	f.mu.Lock()
	c, ok := f.clients[aor]
	f.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no baresip instance for AOR %s (known: %v)", aor, f.AccountAORs())
	}
	return c, nil
}

// ClientForCall looks up the baresip that owns the given call_id, based
// on the events we've observed so far.
func (f *Fleet) ClientForCall(callID string) (*baresip.Client, string, error) {
	f.mu.Lock()
	aor, ok := f.calls[callID]
	c := f.clients[aor]
	f.mu.Unlock()
	if !ok || c == nil {
		return nil, "", fmt.Errorf("unknown call id %s", callID)
	}
	return c, aor, nil
}

// All returns a snapshot of every client. Useful for broadcast
// operations like accept (only one baresip will have a ringing call).
func (f *Fleet) All() map[string]*baresip.Client {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]*baresip.Client, len(f.clients))
	for k, v := range f.clients {
		out[k] = v
	}
	return out
}

// Buffer exposes the ring buffer for recent_events.
func (f *Fleet) Buffer() *baresip.EventBuffer { return f.events }

// Fanout exposes the pub/sub used by wait_for_event.
func (f *Fleet) Fanout() *baresip.EventFanout { return f.fanout }
