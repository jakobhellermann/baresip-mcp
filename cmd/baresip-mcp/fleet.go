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

// Fleet manages a pool of baresip child processes, one per account in
// the user's accounts file. Children are spawned lazily on first use
// (dial / register / accept-with-aor) so the MCP server starts quickly
// and only the accounts you actually exercise eat memory + ports.
type Fleet struct {
	accounts []accountSpec // ordered as in the accounts file

	mu      sync.Mutex
	clients map[string]*baresip.Client
	insts   map[string]*baresipInstance
	calls   map[string]string // call_id -> aor
	fanout  *baresip.EventFanout
	events  *baresip.EventBuffer
	used5060 bool
}

type accountSpec struct {
	aor  string
	line string
}

// aorRE finds <sip:user@host> style URIs in an account line.
var aorRE = regexp.MustCompile(`<(sips?:[^>]+)>`)

func extractAOR(accountLine string) string {
	m := aorRE.FindStringSubmatch(accountLine)
	if m == nil {
		return ""
	}
	uri := m[1]
	for _, sep := range []string{";", "?"} {
		if i := strings.Index(uri, sep); i >= 0 {
			uri = uri[:i]
		}
	}
	return uri
}

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

// NewFleet parses the accounts file but does not spawn anything yet.
func NewFleet(_ context.Context, accountsPath string, bufSize int) (*Fleet, error) {
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
	for _, line := range lines {
		aor := extractAOR(line)
		if aor == "" {
			log.Printf("fleet: skipping account line without parseable AOR: %s", line)
			continue
		}
		f.accounts = append(f.accounts, accountSpec{aor: aor, line: line})
	}
	if len(f.accounts) == 0 {
		return nil, fmt.Errorf("no parseable AORs in %s", accountsPath)
	}
	log.Printf("fleet: %d account(s) configured (lazy spawn): %v", len(f.accounts), f.AccountAORs())
	return f, nil
}

// ensure spawns and connects the baresip for aor if it isn't already
// running. Idempotent.
func (f *Fleet) ensure(ctx context.Context, aor string) (*baresip.Client, error) {
	f.mu.Lock()
	if c, ok := f.clients[aor]; ok {
		f.mu.Unlock()
		return c, nil
	}
	var line string
	for _, a := range f.accounts {
		if a.aor == aor {
			line = a.line
			break
		}
	}
	if line == "" {
		f.mu.Unlock()
		return nil, fmt.Errorf("unknown AOR %s (configured: %v)", aor, f.aorsLocked())
	}

	sipPort := 0
	if !f.used5060 && isUDPPortFree(5060) {
		sipPort = 5060
		f.used5060 = true
	}
	f.mu.Unlock()

	inst, err := spawnBaresip(spawnParams{sipPort: sipPort, accountsLine: line})
	if err != nil {
		return nil, fmt.Errorf("spawn baresip for %s: %w", aor, err)
	}
	client := baresip.New(inst.addr)
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	if err := client.Connect(dialCtx); err != nil {
		cancel()
		inst.Close()
		return nil, fmt.Errorf("connect baresip for %s: %w", aor, err)
	}
	cancel()

	f.mu.Lock()
	// Race: someone else spawned while we were doing it. Drop ours.
	if existing, ok := f.clients[aor]; ok {
		f.mu.Unlock()
		_ = client.Close()
		inst.Close()
		return existing, nil
	}
	f.clients[aor] = client
	f.insts[aor] = inst
	f.mu.Unlock()

	log.Printf("fleet: spawned baresip for %s on ctrl_tcp=%s sip_port=%d tmpdir=%s", aor, inst.addr, sipPort, inst.tmpDir)
	go f.drainEvents(aor, client)
	return client, nil
}

func (f *Fleet) drainEvents(aor string, c *baresip.Client) {
	for ev := range c.Events() {
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
	clients := f.clients
	insts := f.insts
	f.clients = map[string]*baresip.Client{}
	f.insts = map[string]*baresipInstance{}
	f.mu.Unlock()
	for _, c := range clients {
		_ = c.Close()
	}
	for _, inst := range insts {
		inst.Close()
	}
}

func (f *Fleet) aorsLocked() []string {
	out := make([]string, 0, len(f.accounts))
	for _, a := range f.accounts {
		out = append(out, a.aor)
	}
	return out
}

// AccountAORs returns every AOR the fleet knows about (configured),
// whether or not its baresip is currently running.
func (f *Fleet) AccountAORs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.aorsLocked()
}

// SetAccountAttrs augments the stored account line for aor with the
// given baresip URI parameters (overwriting any existing values for
// those keys). If the baresip for that AOR is currently running, it is
// killed; the next ClientFor will respawn with the new attrs. Returns
// the new account line.
//
// The user's on-disk accounts file is never touched.
func (f *Fleet) SetAccountAttrs(aor string, attrs map[string]string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	idx := -1
	for i, a := range f.accounts {
		if a.aor == aor {
			idx = i
			break
		}
	}
	if idx < 0 {
		return "", fmt.Errorf("unknown AOR %s", aor)
	}
	line := f.accounts[idx].line
	for k, v := range attrs {
		line = replaceOrAppendAccountParam(line, k, v)
	}
	f.accounts[idx].line = line

	// Drop the running baresip so the next ClientFor respawns with the
	// new attrs. Active calls on that baresip end abruptly — acceptable
	// for a config change.
	if c, ok := f.clients[aor]; ok {
		_ = c.Close()
		delete(f.clients, aor)
	}
	if inst, ok := f.insts[aor]; ok {
		inst.Close()
		delete(f.insts, aor)
	}
	return line, nil
}

// replaceOrAppendAccountParam replaces ;key=...; ... or appends ;key=value.
func replaceOrAppendAccountParam(line, key, value string) string {
	prefix := ";" + key + "="
	i := strings.Index(line, prefix)
	if i < 0 {
		return line + prefix + value
	}
	start := i + len(prefix)
	end := start
	for end < len(line) && line[end] != ';' && line[end] != '?' {
		end++
	}
	return line[:start] + value + line[end:]
}

// LiveClients returns clients for currently-spawned baresips only.
// Used by aggregated tools (reginfo, list_calls) that should not force
// every account to spawn just to read state.
func (f *Fleet) LiveClients() map[string]*baresip.Client {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]*baresip.Client, len(f.clients))
	for k, v := range f.clients {
		out[k] = v
	}
	return out
}

// ClientFor returns the client for aor, spawning the baresip lazily if
// needed.
func (f *Fleet) ClientFor(ctx context.Context, aor string) (*baresip.Client, error) {
	return f.ensure(ctx, aor)
}

// ClientForCall looks up the baresip that owns the given call_id, based
// on observed events. Does not spawn — if the call_id is unknown, the
// caller should fall back to broadcast over LiveClients.
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

// Buffer exposes the ring buffer for recent_events.
func (f *Fleet) Buffer() *baresip.EventBuffer { return f.events }

// Fanout exposes the pub/sub used by wait_for_event.
func (f *Fleet) Fanout() *baresip.EventFanout { return f.fanout }

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
