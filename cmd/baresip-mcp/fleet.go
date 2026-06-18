package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jakobhellermann/baresip-mcp/pkg/baresip"
)

// Fleet manages a pool of baresip child processes, one per registered
// account. Accounts are introduced at runtime via the `register` MCP
// tool — the server does not read any on-disk accounts file. Children
// are spawned lazily on first use (dial / register / accept-with-aor)
// so only accounts you actually exercise eat memory + ports.
type Fleet struct {
	accounts []accountSpec // in registration order

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

// NewFleet returns an empty fleet. Accounts are added later via the
// `register` MCP tool.
func NewFleet(_ context.Context, bufSize int) *Fleet {
	return &Fleet{
		clients: map[string]*baresip.Client{},
		insts:   map[string]*baresipInstance{},
		calls:   map[string]string{},
		fanout:  baresip.NewEventFanout(),
		events:  baresip.NewEventBuffer(bufSize),
	}
}

// AddAccount registers (or replaces) the account spec for aor. Returns
// true if an existing entry was replaced. Does not spawn baresip — the
// first dial / uareg / accept for this AOR will lazy-spawn.
func (f *Fleet) AddAccount(aor, line string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, a := range f.accounts {
		if a.aor == aor {
			f.accounts[i].line = line
			return true
		}
	}
	f.accounts = append(f.accounts, accountSpec{aor: aor, line: line})
	return false
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
	// Synchronously unregister each live baresip so sipgate forgets our
	// bindings before we exit. baresip's own SIGTERM handler does this
	// too, but doesn't always wait for the 200 OK before tearing down —
	// firing uareg via ctrl_tcp and giving it 3s makes it deterministic.
	for _, c := range clients {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, _ = c.Do(ctx, "uareg", "0 0")
		cancel()
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

// AccountOverrides bundles the in-memory modifications register can
// apply to an account line. uri_params go inside the <sip:...> brackets
// (e.g. transport=tcp), addr_params go after the > (e.g. outbound,
// answermode, answerdelay).
type AccountOverrides struct {
	URIParams  map[string]string // inserted inside <sip:...;k=v>
	AddrParams map[string]string // appended after > as ;k=v
}

// SetAccountAttrs augments the stored account line for aor with the
// given URI and addr params (overwriting any existing values for those
// keys). If the baresip for that AOR is currently running, it is
// gracefully unregistered and killed; the next ClientFor will respawn
// with the new attrs. Returns the new account line.
//
// The user's on-disk accounts file is never touched.
func (f *Fleet) SetAccountAttrs(aor string, ov AccountOverrides) (string, error) {
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
	for k, v := range ov.URIParams {
		line = replaceOrAppendURIParam(line, k, v)
	}
	for k, v := range ov.AddrParams {
		line = replaceOrAppendAccountParam(line, k, v)
	}
	f.accounts[idx].line = line

	// Drop the running baresip so the next ClientFor respawns with the
	// new attrs. Before that, synchronously ask baresip to unregister
	// (uareg 0 0) so sipgate forgets this binding immediately. Otherwise
	// the stale binding lingers until Expires (~600s) and sipgate may
	// fork inbound calls to it, causing NO_ANSWER.
	if c, ok := f.clients[aor]; ok {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, _ = c.Do(ctx, "uareg", "0 0")
		cancel()
		_ = c.Close()
		delete(f.clients, aor)
	}
	if inst, ok := f.insts[aor]; ok {
		inst.Close()
		delete(f.insts, aor)
	}
	return line, nil
}

// replaceOrAppendAccountParam works on addr-params after the closing >.
// If the value contains ;, it is wrapped in double quotes (required by
// baresip's account parser, e.g. for outbound="sip:host;transport=tcp").
func replaceOrAppendAccountParam(line, key, value string) string {
	wrapped := value
	if strings.ContainsAny(value, ";? ") {
		wrapped = `"` + value + `"`
	}
	prefix := ";" + key + "="
	i := strings.Index(line, prefix)
	if i < 0 {
		return line + prefix + wrapped
	}
	start := i + len(prefix)
	// Account for an existing quoted value: skip until matching close-quote.
	if start < len(line) && line[start] == '"' {
		end := start + 1
		for end < len(line) && line[end] != '"' {
			end++
		}
		if end < len(line) {
			end++ // include closing quote
		}
		return line[:start] + wrapped + line[end:]
	}
	end := start
	for end < len(line) && line[end] != ';' && line[end] != '?' {
		end++
	}
	return line[:start] + wrapped + line[end:]
}

// replaceOrAppendURIParam edits URI parameters that live INSIDE the
// <sip:...> angle brackets (e.g. transport). baresip parses these as
// part of the SIP URI; transport in particular only takes effect there.
func replaceOrAppendURIParam(line, key, value string) string {
	open := strings.Index(line, "<")
	close := strings.Index(line, ">")
	if open < 0 || close < 0 || close < open {
		return line // can't parse; leave it alone
	}
	inner := line[open+1 : close]
	prefix := ";" + key + "="
	if i := strings.Index(inner, prefix); i >= 0 {
		start := i + len(prefix)
		end := start
		for end < len(inner) && inner[end] != ';' && inner[end] != '?' {
			end++
		}
		inner = inner[:start] + value + inner[end:]
	} else {
		inner = inner + prefix + value
	}
	return line[:open+1] + inner + line[close:]
}

// InstanceFor returns metadata about the running baresip for aor.
// Returns ok=false if no baresip is currently spawned for that AOR.
func (f *Fleet) InstanceFor(aor string) (tmpDir, logPath, ctrlAddr string, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	inst, found := f.insts[aor]
	if !found {
		return "", "", "", false
	}
	return inst.tmpDir, inst.logPath, inst.addr, true
}

// AccountLine returns the in-memory account line (possibly augmented
// via SetAccountAttrs) for aor.
func (f *Fleet) AccountLine(aor string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.accounts {
		if a.aor == aor {
			return a.line, true
		}
	}
	return "", false
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
