// Command baresip-mcp exposes a baresip ctrl_tcp connection as an MCP server.
//
// Accounts are introduced at runtime through the `register` MCP tool —
// the server reads no on-disk accounts file. Each registered account
// gets its own baresip child process so calls between two local
// accounts (e1 → e2 by external number) work: sipgate's hairpinned
// INVITE arrives at a different SIP socket than the outgoing leg,
// avoiding state-machine collisions.
//
// Each child baresip runs out of its own tmpdir
// (/tmp/baresip-mcp-<rand>/.baresip/) with a single-account accounts
// file built from the register call's inputs plus any per-account
// overrides (e.g. ;answerdelay=N from auto_answer_after_seconds). To
// inspect the actual per-child config, look at the tmpdir printed in
// stderr at spawn time.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/jakobhellermann/baresip-mcp/pkg/baresip"
)

type dialInput struct {
	URI     string            `json:"uri" jsonschema:"SIP URI to dial, e.g. sip:alice@example.com"`
	Account string            `json:"account" jsonschema:"AOR of the local account to call from, e.g. sip:1126226e1@proxy.dev.sipgate.de. Selects which baresip instance dials."`
	Headers map[string]string `json:"headers,omitempty" jsonschema:"optional SIP headers to attach to this specific outgoing INVITE, e.g. {\"X-Client-Correlation-ID\": \"abc-123\"}. Headers are scoped to this call only — set on the UA before dial, copied into the call by baresip, then removed."`
}

type empty struct{}

type rawInput struct {
	Command string `json:"command" jsonschema:"baresip long-form command name, e.g. dial, hangup, reginfo"`
	Params  string `json:"params,omitempty" jsonschema:"optional parameters appended to the command"`
	AOR     string `json:"aor,omitempty" jsonschema:"optional AOR — routes the command to the baresip instance hosting that account. If omitted, the command is sent to every instance and the first non-error response wins."`
}

type transferInput struct {
	URI    string `json:"uri" jsonschema:"target SIP URI to blind-transfer the current call to"`
	CallID string `json:"call_id,omitempty" jsonschema:"call id of the call to transfer; if omitted, sent to every instance"`
}

type dtmfInput struct {
	Digits string `json:"digits" jsonschema:"DTMF digits to send to the active call, e.g. 1234#"`
	CallID string `json:"call_id,omitempty" jsonschema:"call id; if omitted, sent to every instance"`
}

type muteInput struct {
	On     bool   `json:"on" jsonschema:"true to mute, false to un-mute the active call"`
	CallID string `json:"call_id,omitempty" jsonschema:"call id; if omitted, sent to every instance"`
}

type holdInput struct {
	On     bool   `json:"on" jsonschema:"true to put the active call on hold, false to resume"`
	CallID string `json:"call_id,omitempty" jsonschema:"call id; if omitted, sent to every instance"`
}

type hangupAllInput struct {
	Direction string `json:"direction,omitempty" jsonschema:"optional filter: 'in' or 'out'. Empty hangs up all calls."`
}

type hangupInput struct {
	CallID string `json:"call_id,omitempty" jsonschema:"call id of the call to hang up; if omitted, broadcast to every instance"`
}

type acceptInput struct {
	AOR string `json:"aor,omitempty" jsonschema:"optional AOR of the instance whose ringing call to accept; if omitted, broadcast to every instance (only one will have a ringing call)"`
}

type registerInput struct {
	AOR             string            `json:"aor" jsonschema:"AOR of the account to register, e.g. sip:1126226e1@proxy.dev.sipgate.de"`
	Password        string            `json:"password,omitempty" jsonschema:"SIP authentication password. Required the first time you register a given AOR; on subsequent register calls for the same AOR the stored line is reused if omitted. The password lives only in memory and in the spawned baresip child's tmpdir."`
	Username        string            `json:"username,omitempty" jsonschema:"SIP authentication username. Optional — defaults to the user part of the AOR, which is correct for sipgate extensions/voizas. Trunks typically need it set explicitly to the trunk-extension id."`
	ExtraParams     map[string]string `json:"extra_params,omitempty" jsonschema:"optional additional baresip account params appended after the AOR (e.g. {\"audio_codecs\": \"pcma\", \"stunserver\": \"stun:stun.sipgate.net\"}). Only applied when introducing a new AOR; ignored on re-registration of a known AOR."`
	Regint          int               `json:"regint,omitempty" jsonschema:"registration interval in seconds (default 60). Short by design so the NAT/VPN pinhole keeping the inbound path open is refreshed before consumer-router UDP mappings expire (typically 30–180s). Set to 0 to stop registering."`
	AutoAnswerAfter int               `json:"auto_answer_after_seconds,omitempty" jsonschema:"if >0, configure baresip to auto-answer incoming calls after this many seconds, giving the caller a ringback window. Appends ;answerdelay=N (in ms; baresip uses MIN_RINGTIME=1000) to this account's in-memory line. Triggers a respawn of the baresip for this AOR if one is already running."`
	Transport       string            `json:"transport,omitempty" jsonschema:"SIP transport: 'udp' (default), 'tcp', or 'tls'. Non-UDP options often need a matching outbound_proxy because the registrar host may only listen UDP on its default address. For sipgate use outbound_proxy=sip:sip.dev.sipgate.de (dev) or sip:sip.sipgate.de (prod) when transport is tcp/tls/ws."`
	OutboundProxy   string            `json:"outbound_proxy,omitempty" jsonschema:"explicit outbound SIP proxy (e.g. 'sip:sip.dev.sipgate.de'). Combined with transport to produce an outbound URI baresip uses for REGISTER + INVITE. Triggers a respawn if already running."`
}

type unregisterInput struct {
	AOR string `json:"aor" jsonschema:"AOR of the account to deregister"`
}

type recentEventsInput struct {
	Limit int `json:"limit,omitempty" jsonschema:"max number of most recent events to return (default 50)"`
}

type inspectAccountInput struct {
	AOR     string `json:"aor" jsonschema:"AOR of the account to inspect"`
	LogTail int    `json:"log_tail,omitempty" jsonschema:"number of trailing lines from the baresip child's log to include (default 40, 0 for none)"`
	LogGrep string `json:"log_grep,omitempty" jsonschema:"optional substring filter applied to the tail before returning; e.g. 'binding' or 'REGISTER'"`
}

type inspectAccountOutput struct {
	AOR             string `json:"aor"`
	AccountLine     string `json:"account_line"` // possibly augmented; auth_pass redacted
	Running         bool   `json:"running"`
	TmpDir          string `json:"tmpdir,omitempty"`
	LogPath         string `json:"log_path,omitempty"`
	CtrlTCPAddress  string `json:"ctrl_tcp_address,omitempty"`
	AccountsContent string `json:"spawned_accounts_file,omitempty"` // auth_pass redacted
	LogTail         string `json:"log_tail,omitempty"`
}

type waitForEventInput struct {
	Types           []string `json:"types,omitempty" jsonschema:"event types to match (e.g. CALL_ESTABLISHED, CALL_CLOSED). Empty matches any event."`
	CallID          string   `json:"call_id,omitempty" jsonschema:"optional baresip call id to filter on. If set, only events for that call match."`
	AOR             string   `json:"aor,omitempty" jsonschema:"optional AOR to filter on. If set, only events from that account's baresip match."`
	TimeoutSeconds  int      `json:"timeout_seconds,omitempty" jsonschema:"max seconds to wait (default 30)"`
	LookbackSeconds int      `json:"lookback_seconds,omitempty" jsonschema:"check the recent-events buffer for a matching event that already arrived this many seconds ago (default 30). Set to -1 to disable lookback."`
}

type waitForEventOutput struct {
	Event    *baresip.RecordedEvent `json:"event"`
	TimedOut bool                   `json:"timed_out"`
}

// version is the build version, overridden at release time via
// -ldflags "-X main.version=...".
var version = "dev"

func main() {
	bufSize := flag.Int("event-buffer", 256, "size of the recent-events ring buffer")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sweepOrphans()

	fleet := NewFleet(ctx, *bufSize)
	defer fleet.Close()
	log.Print("fleet ready (empty — accounts are added via the register tool)")

	server := mcp.NewServer(&mcp.Implementation{
		Name:    "baresip-mcp",
		Version: version,
	}, nil)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "dial",
		Description: "Place an outgoing SIP call to the given URI via baresip. 'from' selects which local account dials. 'headers' attaches per-call SIP headers (e.g. X-Client-Correlation-ID).",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in dialInput) (*mcp.CallToolResult, any, error) {
		return dialHandler(ctx, fleet, in)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "accept",
		Description: "Accept (answer) a ringing incoming call. Optionally scoped to one account by AOR.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in acceptInput) (*mcp.CallToolResult, any, error) {
		return broadcastOrTargeted(ctx, fleet, in.AOR, "", "accept", "")
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "hangup",
		Description: "Hang up the current call. Optionally scoped to one call by call_id.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in hangupInput) (*mcp.CallToolResult, any, error) {
		return broadcastOrTargeted(ctx, fleet, "", in.CallID, "hangup", "")
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "hangup_all",
		Description: "Hang up all active calls across every instance, optionally filtered by direction ('in' or 'out').",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in hangupAllInput) (*mcp.CallToolResult, any, error) {
		return broadcast(ctx, fleet, "hangupall", in.Direction)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_calls",
		Description: "List all active calls across every account.",
	}, listCallsHandler(fleet))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "call_status",
		Description: "Show status of the currently active call across every instance.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ empty) (*mcp.CallToolResult, any, error) {
		return broadcast(ctx, fleet, "callstat", "")
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "reginfo",
		Description: "Show registration status of all configured SIP accounts across every instance.",
	}, reginfoHandler(fleet))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "hold",
		Description: "Hold or resume a call. Optionally scoped to one call by call_id.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in holdInput) (*mcp.CallToolResult, any, error) {
		cmd := "hold"
		if !in.On {
			cmd = "resume"
		}
		return broadcastOrTargeted(ctx, fleet, "", in.CallID, cmd, "")
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "mute",
		Description: "Mute or un-mute a call. Optionally scoped to one call by call_id.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in muteInput) (*mcp.CallToolResult, any, error) {
		val := "false"
		if in.On {
			val = "true"
		}
		return broadcastOrTargeted(ctx, fleet, "", in.CallID, "mute", val)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "transfer",
		Description: "Blind-transfer a call to the given SIP URI.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in transferInput) (*mcp.CallToolResult, any, error) {
		return broadcastOrTargeted(ctx, fleet, "", in.CallID, "transfer", in.URI)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "dtmf",
		Description: "Send DTMF digits to a call.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in dtmfInput) (*mcp.CallToolResult, any, error) {
		return sendDTMF(ctx, fleet, in.CallID, in.Digits)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "register",
		Description: "Introduce a SIP account to the fleet and register it. Requires auth_pass the first time you call it for a given AOR; subsequent calls for the same AOR reuse the stored line if auth_pass is omitted. Pass regint=0 to stop registering. Pass auto_answer_after_seconds>0 to enable delayed auto-answer.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in registerInput) (*mcp.CallToolResult, any, error) {
		if _, known := fleet.AccountLine(in.AOR); !known {
			if in.Password == "" {
				return nil, nil, fmt.Errorf("password is required when registering a new AOR (%s)", in.AOR)
			}
			line, err := buildAccountLine(in.AOR, in.Password, in.Username, in.ExtraParams)
			if err != nil {
				return nil, nil, err
			}
			fleet.AddAccount(in.AOR, line)
		} else if in.Password != "" {
			// Caller passed a (possibly new) password for an already-known
			// AOR — rebuild the line from the new inputs so a credential
			// rotation works without restart.
			line, err := buildAccountLine(in.AOR, in.Password, in.Username, in.ExtraParams)
			if err != nil {
				return nil, nil, err
			}
			fleet.AddAccount(in.AOR, line)
		}

		uriParams := map[string]string{}
		addrParams := map[string]string{}
		if in.AutoAnswerAfter > 0 {
			// IMPORTANT: do NOT set ;answermode=auto. baresip's menu
			// short-circuits answermode=auto and answers immediately,
			// skipping the answerdelay timer entirely (see
			// modules/menu/menu.c BEVENT_CALL_INCOMING). The delayed-
			// answer path (check_delayed_answer → start_autoanswer →
			// play_incoming → tmr_start) only runs when answermode is
			// not auto but account_answerdelay > 0.
			//
			// baresip's account answerdelay is in *milliseconds* (cf.
			// MIN_RINGTIME=1000 in menu.c). Our tool param is seconds.
			addrParams["answerdelay"] = fmt.Sprintf("%d", in.AutoAnswerAfter*1000)
		}
		if in.Transport != "" {
			uriParams["transport"] = in.Transport
		}
		if in.OutboundProxy != "" {
			outbound := in.OutboundProxy
			if in.Transport != "" && !strings.Contains(outbound, "transport=") {
				outbound = outbound + ";transport=" + in.Transport
			}
			addrParams["outbound"] = outbound
		}
		if len(uriParams) > 0 || len(addrParams) > 0 {
			if _, err := fleet.SetAccountAttrs(in.AOR, AccountOverrides{
				URIParams:  uriParams,
				AddrParams: addrParams,
			}); err != nil {
				return nil, nil, err
			}
		}
		regint := in.Regint
		if regint == 0 {
			regint = 60
		}
		return uaregOn(ctx, fleet, in.AOR, regint)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "unregister",
		Description: "Deregister an account (regint=0).",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in unregisterInput) (*mcp.CallToolResult, any, error) {
		return uaregOn(ctx, fleet, in.AOR, 0)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "recent_events",
		Description: "Return recent asynchronous baresip events (call state, registration, messages) as JSON.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in recentEventsInput) (*mcp.CallToolResult, any, error) {
		limit := in.Limit
		if limit == 0 {
			limit = 50
		}
		snap := fleet.Buffer().Snapshot(limit)
		body, err := json.MarshalIndent(snap, "", "  ")
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(body)}},
		}, nil, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "wait_for_event",
		Description: "Block until a baresip event arrives matching the given filters, or until timeout. Use after dial/accept/hangup to observe call lifecycle without polling recent_events.",
	}, waitForEventHandler(fleet))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "command",
		Description: "Send an arbitrary baresip long-form command. Pass 'aor' to route to a specific instance, else broadcast.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in rawInput) (*mcp.CallToolResult, any, error) {
		if in.AOR != "" {
			c, err := fleet.ClientFor(ctx, in.AOR)
			if err != nil {
				return nil, nil, err
			}
			return runCmd(ctx, c, in.Command, in.Params)
		}
		return broadcast(ctx, fleet, in.Command, in.Params)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "inspect_account",
		Description: "Diagnostic: returns the in-memory account line (possibly augmented), the tmpdir + log path of the running baresip child (if any), and the last N lines of its log. Useful when something behaves unexpectedly and you need to verify what the spawned baresip actually saw.",
	}, inspectAccountHandler(fleet))

	if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil {
		log.Fatalf("mcp server: %v", err)
	}
}

// dialHandler issues uaaddheader/dial/uarmheader on the baresip hosting
// in.Account. Each baresip in the fleet has exactly one UA, so the UA
// index is always 0.
func dialHandler(ctx context.Context, f *Fleet, in dialInput) (*mcp.CallToolResult, any, error) {
	if in.Account == "" {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: "'account' is required (which account dials)"}},
		}, nil, nil
	}
	c, err := f.ClientFor(ctx, in.Account)
	if err != nil {
		return nil, nil, err
	}

	keys := make([]string, 0, len(in.Headers))
	for k := range in.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		arg := fmt.Sprintf("%s=%s 0", k, in.Headers[k])
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := c.Do(cctx, "uaaddheader", arg)
		cancel()
		if err != nil {
			return nil, nil, err
		}
		if !resp.OK {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("uaaddheader %s: %s", k, resp.Data)}},
			}, nil, nil
		}
	}

	dialResult, _, dialErr := runCmd(ctx, c, "dial", fmt.Sprintf("%s 0", in.URI))

	for _, k := range keys {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, _ = c.Do(cctx, "uarmheader", fmt.Sprintf("%s 0", k))
		cancel()
	}

	return dialResult, nil, dialErr
}

// uaregOn issues uareg on the right baresip and then waits up to ~5s
// for the matching REGISTERING → REGISTER_OK (or REGISTER_FAIL) event
// before returning, so the caller actually knows the registrar
// processed the change. The two pieces are stitched into a single
// human-readable response.
func uaregOn(ctx context.Context, f *Fleet, aor string, regint int) (*mcp.CallToolResult, any, error) {
	c, err := f.ClientFor(ctx, aor)
	if err != nil {
		return nil, nil, err
	}

	// Subscribe before issuing the command so we don't miss the event.
	ch, unsub := f.Fanout().Subscribe()
	defer unsub()

	resp, err := c.Do(ctx, "uareg", fmt.Sprintf("%d 0", regint))
	if err != nil {
		return nil, nil, err
	}

	deadline := time.After(5 * time.Second)
	var status string
waitLoop:
	for {
		select {
		case ev := <-ch:
			if ea, _ := ev.Extra["accountaor"].(string); ea != aor {
				continue
			}
			switch ev.Type {
			case "REGISTER_OK":
				status = fmt.Sprintf("REGISTER_OK (%s)", ev.Param)
				break waitLoop
			case "REGISTER_FAIL":
				status = fmt.Sprintf("REGISTER_FAIL (%s)", ev.Param)
				break waitLoop
			case "UNREGISTERING":
				// Mid-flight — keep waiting for the 200 OK.
			}
		case <-deadline:
			status = "no REGISTER_OK/FAIL within 5s"
			break waitLoop
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}

	text := strings.TrimRight(resp.Data, "\n") + "\n" + status
	return &mcp.CallToolResult{
		IsError: !resp.OK,
		Content: []mcp.Content{&mcp.TextContent{Text: text}},
	}, nil, nil
}

// broadcast runs the same command on every fleet client and returns the
// first non-error response (or the last error if all failed). For
// commands like callstat / hangupall this is the desired semantic.
func broadcast(ctx context.Context, f *Fleet, cmd, params string) (*mcp.CallToolResult, any, error) {
	var combined []string
	var lastErr error
	for aor, c := range f.LiveClients() {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := c.Do(cctx, cmd, params)
		cancel()
		if err != nil {
			lastErr = err
			combined = append(combined, fmt.Sprintf("[%s] error: %v", aor, err))
			continue
		}
		header := fmt.Sprintf("[%s]", aor)
		if !resp.OK {
			header += " (failed)"
		}
		body := strings.TrimRight(resp.Data, "\n")
		combined = append(combined, fmt.Sprintf("%s %s", header, body))
	}
	if lastErr != nil && len(combined) == 0 {
		return nil, nil, lastErr
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: strings.Join(combined, "\n")}},
	}, nil, nil
}

// dtmfInterDigitGap is the pause between consecutive RFC2833 events.
// baresip's `sndcode` issues a single trailing KEYCODE_REL no matter how
// many digits were passed, and `audio_send_digit` overwrites telev's
// cur_key per digit without releasing the previous one first — so a
// multi-digit `sndcode` call typically only delivers the last digit (or
// none). We work around it by issuing one `sndcode` per digit and waiting
// long enough that each event's redundant end-packets clear before the
// next event starts.
const dtmfInterDigitGap = 450 * time.Millisecond

// sendDTMF sends DTMF digits one at a time so each digit gets its own
// proper RFC2833 send-then-release pair. Returns the result of the final
// digit (any per-digit error short-circuits).
func sendDTMF(ctx context.Context, f *Fleet, callID, digits string) (*mcp.CallToolResult, any, error) {
	if digits == "" {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "ok: dtmf (no digits)"}},
		}, nil, nil
	}
	var last *mcp.CallToolResult
	for i := 0; i < len(digits); i++ {
		res, _, err := broadcastOrTargeted(ctx, f, "", callID, "sndcode", string(digits[i]))
		if err != nil {
			return nil, nil, err
		}
		last = res
		if res != nil && res.IsError {
			return res, nil, nil
		}
		if i < len(digits)-1 {
			select {
			case <-time.After(dtmfInterDigitGap):
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		}
	}
	return last, nil, nil
}

// broadcastOrTargeted routes to a specific baresip if aor or callID
// uniquely identifies one, otherwise broadcasts.
func broadcastOrTargeted(ctx context.Context, f *Fleet, aor, callID, cmd, params string) (*mcp.CallToolResult, any, error) {
	var c *baresip.Client
	var err error
	switch {
	case callID != "":
		c, _, err = f.ClientForCall(callID)
	case aor != "":
		c, err = f.ClientFor(ctx, aor)
	}
	if err != nil {
		return nil, nil, err
	}
	if c != nil {
		return runCmd(ctx, c, cmd, params)
	}
	return broadcast(ctx, f, cmd, params)
}

type listCallsOutput struct {
	UserAgents []baresip.UserAgentCalls `json:"user_agents"`
	Raw        string                   `json:"raw"`
}

func listCallsHandler(f *Fleet) func(context.Context, *mcp.CallToolRequest, empty) (*mcp.CallToolResult, listCallsOutput, error) {
	return func(ctx context.Context, _ *mcp.CallToolRequest, _ empty) (*mcp.CallToolResult, listCallsOutput, error) {
		var rawParts []string
		var uas []baresip.UserAgentCalls
		for aor, c := range f.LiveClients() {
			cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			resp, err := c.Do(cctx, "listcalls", "")
			cancel()
			if err != nil {
				return nil, listCallsOutput{}, err
			}
			rawParts = append(rawParts, fmt.Sprintf("--- %s ---\n%s", aor, resp.Data))
			uas = append(uas, baresip.ParseListCalls(resp.Data)...)
		}
		raw := strings.Join(rawParts, "\n")
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: raw}},
		}, listCallsOutput{UserAgents: uas, Raw: raw}, nil
	}
}

type reginfoOutput struct {
	Registrations []baresip.Registration `json:"registrations"`
	Raw           string                 `json:"raw"`
}

func reginfoHandler(f *Fleet) func(context.Context, *mcp.CallToolRequest, empty) (*mcp.CallToolResult, reginfoOutput, error) {
	return func(ctx context.Context, _ *mcp.CallToolRequest, _ empty) (*mcp.CallToolResult, reginfoOutput, error) {
		// Show live registrations from running instances only. Reading
		// reginfo should not force a spawn — that's reserved for tools
		// that actually need the baresip running (dial, register, …).
		live := f.LiveClients()
		var rawParts []string
		var regs []baresip.Registration
		for aor, c := range live {
			cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			resp, err := c.Do(cctx, "reginfo", "")
			cancel()
			if err != nil {
				return nil, reginfoOutput{}, err
			}
			rawParts = append(rawParts, fmt.Sprintf("--- %s ---\n%s", aor, resp.Data))
			regs = append(regs, baresip.ParseRegInfo(resp.Data)...)
		}

		var dormant []string
		for _, aor := range f.AccountAORs() {
			if _, running := live[aor]; !running {
				dormant = append(dormant, aor)
			}
		}
		if len(dormant) > 0 {
			rawParts = append(rawParts,
				fmt.Sprintf("--- dormant (configured but baresip not spawned yet) ---\n%s\n",
					strings.Join(dormant, "\n")))
		}

		raw := strings.Join(rawParts, "\n")
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: raw}},
		}, reginfoOutput{Registrations: regs, Raw: raw}, nil
	}
}

var authPassRE = regexp.MustCompile(`auth_pass=[^;]*`)

func redactAccountLine(s string) string {
	return authPassRE.ReplaceAllString(s, "auth_pass=REDACTED")
}

func tailFile(path string, n int) (string, error) {
	if n <= 0 {
		return "", nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	lines := strings.Split(string(data), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n"), nil
}

func inspectAccountHandler(f *Fleet) func(context.Context, *mcp.CallToolRequest, inspectAccountInput) (*mcp.CallToolResult, inspectAccountOutput, error) {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in inspectAccountInput) (*mcp.CallToolResult, inspectAccountOutput, error) {
		line, ok := f.AccountLine(in.AOR)
		if !ok {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("unknown AOR %s (configured: %v)", in.AOR, f.AccountAORs())}},
			}, inspectAccountOutput{}, nil
		}
		out := inspectAccountOutput{
			AOR:         in.AOR,
			AccountLine: redactAccountLine(line),
		}
		tmpDir, logPath, ctrlAddr, running := f.InstanceFor(in.AOR)
		out.Running = running
		if running {
			out.TmpDir = tmpDir
			out.LogPath = logPath
			out.CtrlTCPAddress = ctrlAddr
			if body, err := os.ReadFile(filepath.Join(tmpDir, ".baresip", "accounts")); err == nil {
				out.AccountsContent = redactAccountLine(strings.TrimRight(string(body), "\n"))
			}
			tail := in.LogTail
			if tail == 0 {
				tail = 40
			}
			if tailStr, err := tailFile(logPath, tail); err == nil {
				if in.LogGrep != "" {
					var kept []string
					for _, l := range strings.Split(tailStr, "\n") {
						if strings.Contains(l, in.LogGrep) {
							kept = append(kept, l)
						}
					}
					tailStr = strings.Join(kept, "\n")
				}
				out.LogTail = tailStr
			}
		}
		body, _ := json.MarshalIndent(out, "", "  ")
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(body)}},
		}, out, nil
	}
}

func waitForEventHandler(f *Fleet) func(context.Context, *mcp.CallToolRequest, waitForEventInput) (*mcp.CallToolResult, waitForEventOutput, error) {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in waitForEventInput) (*mcp.CallToolResult, waitForEventOutput, error) {
		timeout := time.Duration(in.TimeoutSeconds) * time.Second
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		var lookback time.Duration
		switch {
		case in.LookbackSeconds < 0:
			lookback = 0
		case in.LookbackSeconds == 0:
			lookback = 30 * time.Second
		default:
			lookback = time.Duration(in.LookbackSeconds) * time.Second
		}

		matches := func(ev baresip.Event) bool {
			if in.CallID != "" {
				id, _ := ev.Extra["id"].(string)
				if id != in.CallID {
					return false
				}
			}
			if in.AOR != "" {
				aor, _ := ev.Extra["fleet_aor"].(string)
				if aor != in.AOR {
					aor, _ = ev.Extra["accountaor"].(string)
					if aor != in.AOR {
						return false
					}
				}
			}
			if len(in.Types) == 0 {
				return true
			}
			for _, t := range in.Types {
				if ev.Type == t {
					return true
				}
			}
			return false
		}

		ch, unsub := f.Fanout().Subscribe()
		defer unsub()

		if lookback > 0 {
			cutoff := time.Now().Add(-lookback)
			snap := f.Buffer().Snapshot(0)
			for i := len(snap) - 1; i >= 0; i-- {
				if snap[i].At.Before(cutoff) {
					break
				}
				ev := baresip.Event{
					Class: snap[i].Class,
					Type:  snap[i].Type,
					Param: snap[i].Param,
					Extra: snap[i].Extra,
				}
				if matches(ev) {
					rec := snap[i]
					return &mcp.CallToolResult{}, waitForEventOutput{Event: &rec}, nil
				}
			}
		}

		wctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		for {
			select {
			case ev, ok := <-ch:
				if !ok {
					return &mcp.CallToolResult{}, waitForEventOutput{TimedOut: true}, nil
				}
				if matches(ev) {
					rec := baresip.RecordedEvent{
						At:    time.Now().UTC(),
						Class: ev.Class,
						Type:  ev.Type,
						Param: ev.Param,
						Extra: ev.Extra,
					}
					return &mcp.CallToolResult{}, waitForEventOutput{Event: &rec}, nil
				}
			case <-wctx.Done():
				return &mcp.CallToolResult{}, waitForEventOutput{TimedOut: true}, nil
			}
		}
	}
}

func runCmd(ctx context.Context, c *baresip.Client, cmd, params string) (*mcp.CallToolResult, any, error) {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := c.Do(cctx, cmd, params)
	if err != nil {
		return nil, nil, err
	}
	text := resp.Data
	if text == "" {
		if resp.OK {
			text = fmt.Sprintf("ok: %s", cmd)
		} else {
			text = fmt.Sprintf("failed: %s", cmd)
		}
	}
	return &mcp.CallToolResult{
		IsError: !resp.OK,
		Content: []mcp.Content{&mcp.TextContent{Text: text}},
	}, nil, nil
}

// buildAccountLine assembles a baresip-format accounts file line from
// the register tool's inputs. The line shape is
// <sip:user@host>;auth_pass=...;auth_user=...;k=v;...
// username defaults to the user part of the AOR (correct for sipgate
// extensions/voizas; trunks need it set explicitly).
func buildAccountLine(aor, password, username string, extra map[string]string) (string, error) {
	if !strings.HasPrefix(aor, "sip:") && !strings.HasPrefix(aor, "sips:") {
		return "", fmt.Errorf("aor must start with sip: or sips:, got %q", aor)
	}
	if password == "" {
		return "", fmt.Errorf("password is required")
	}
	var b strings.Builder
	b.WriteString("<")
	b.WriteString(aor)
	b.WriteString(">;auth_pass=")
	b.WriteString(password)
	if username != "" {
		b.WriteString(";auth_user=")
		b.WriteString(username)
	}
	// Default dtmfmode=info: this fleet's audio source (aufile playing a
	// short greeting WAV) stops feeding samples after the greeting ends,
	// which halts the RTP TX thread and strands any RFC2833 telephone-
	// event queued via telev — receivers see only the first DTMF digit
	// (or none). SIP INFO rides the signaling channel and is independent
	// of RTP TX state. Callers can override via extra_params
	// (e.g. dtmfmode=rtpevent or dtmfmode=auto) if the peer only handles
	// in-band/RFC2833 and a continuous audio source is in use.
	if _, set := extra["dtmfmode"]; !set {
		b.WriteString(";dtmfmode=info")
	}
	// Stable iteration order so a re-register with the same inputs produces
	// the same line (matters for the respawn-on-change comparison).
	keys := make([]string, 0, len(extra))
	for k := range extra {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString(";")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(extra[k])
	}
	return b.String(), nil
}
