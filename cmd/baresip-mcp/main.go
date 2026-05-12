// Command baresip-mcp exposes a baresip ctrl_tcp connection as an MCP server.
//
// Each account in ~/.baresip/accounts gets its own baresip child process
// so calls between two local accounts (e1 → e2 by external number) work
// — sipgate's hairpinned INVITE arrives at a different SIP socket than
// the outgoing leg, avoiding state-machine collisions.
//
// The user's ~/.baresip/accounts is read ONCE at startup and is never
// modified. Each child baresip runs out of its own tmpdir
// (/tmp/baresip-mcp-<rand>/.baresip/) with a single-account accounts
// file derived from the original line plus any per-account overrides
// added at runtime (e.g. ;answermode=auto;answerdelay=N from the
// register tool's auto_answer_after_seconds). To inspect the actual
// per-child config, look at the tmpdir printed in stderr at spawn time,
// not the user's accounts file.
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

	"github.com/sipgate/baresip-mcp/pkg/baresip"
)

type dialInput struct {
	URI     string            `json:"uri" jsonschema:"SIP URI to dial, e.g. sip:alice@example.com"`
	From    string            `json:"from" jsonschema:"AOR of the local account to call from, e.g. sip:1126226e1@proxy.dev.sipgate.de. Selects which baresip instance dials."`
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
	AOR             string `json:"aor" jsonschema:"AOR of the account to register, e.g. sip:1126226e1@proxy.dev.sipgate.de"`
	Regint          int    `json:"regint,omitempty" jsonschema:"registration interval in seconds (default 60). Short by design so the NAT/VPN pinhole keeping the inbound path open is refreshed before consumer-router UDP mappings expire (typically 30–180s). Set to 0 to stop registering."`
	AutoAnswerAfter int    `json:"auto_answer_after_seconds,omitempty" jsonschema:"if >0, configure baresip to auto-answer incoming calls after this many seconds, giving the caller a ringback window. Appends ;answermode=auto;answerdelay=N to this account's line in the fleet's in-memory account spec — the spawned baresip child reads the augmented line from its own tmpdir, the user's ~/.baresip/accounts is never touched. Triggers a respawn of the baresip for this AOR if one is already running."`
	Transport       string `json:"transport,omitempty" jsonschema:"SIP transport: 'udp' (default), 'tcp', or 'tls'. Non-UDP options often need a matching outbound_proxy because the registrar host may only listen UDP on its default address. For sipgate use outbound_proxy=sip:sip.dev.sipgate.de (dev) or sip:sip.sipgate.de (prod) when transport is tcp/tls/ws."`
	OutboundProxy   string `json:"outbound_proxy,omitempty" jsonschema:"explicit outbound SIP proxy (e.g. 'sip:sip.dev.sipgate.de'). Combined with transport to produce an outbound URI baresip uses for REGISTER + INVITE. Triggers a respawn if already running."`
}

type unregisterInput struct {
	AOR string `json:"aor" jsonschema:"AOR of the account to deregister"`
}

type recentEventsInput struct {
	Limit int `json:"limit,omitempty" jsonschema:"max number of most recent events to return (default 50)"`
}

type inspectAccountInput struct {
	AOR      string `json:"aor" jsonschema:"AOR of the account to inspect"`
	LogTail  int    `json:"log_tail,omitempty" jsonschema:"number of trailing lines from the baresip child's log to include (default 40, 0 for none)"`
	LogGrep  string `json:"log_grep,omitempty" jsonschema:"optional substring filter applied to the tail before returning; e.g. 'binding' or 'REGISTER'"`
}

type inspectAccountOutput struct {
	AOR             string `json:"aor"`
	AccountLine     string `json:"account_line"`              // possibly augmented; auth_pass redacted
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

func main() {
	accountsPath := flag.String("accounts", envOr("BARESIP_ACCOUNTS", defaultAccountsPath()), "path to a baresip accounts file; one baresip child is spawned per active line")
	bufSize := flag.Int("event-buffer", 256, "size of the recent-events ring buffer")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sweepOrphans()

	fleet, err := NewFleet(ctx, *accountsPath, *bufSize)
	if err != nil {
		log.Fatalf("start fleet: %v", err)
	}
	defer fleet.Close()
	log.Printf("fleet ready with %d account(s): %v", len(fleet.AccountAORs()), fleet.AccountAORs())

	server := mcp.NewServer(&mcp.Implementation{
		Name:    "baresip-mcp",
		Version: "0.1.0",
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
		return broadcastOrTargeted(ctx, fleet, "", in.CallID, "sndcode", in.Digits)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "register",
		Description: "Register an account at its provider so it can receive incoming calls. Pass regint=0 to stop registering. Pass auto_answer_after_seconds>0 to enable delayed auto-answer (gives caller a ringback window).",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in registerInput) (*mcp.CallToolResult, any, error) {
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
// in.From. Each baresip in the fleet has exactly one UA, so the UA index
// is always 0.
func dialHandler(ctx context.Context, f *Fleet, in dialInput) (*mcp.CallToolResult, any, error) {
	if in.From == "" {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: "'from' is required (which account dials)"}},
		}, nil, nil
	}
	c, err := f.ClientFor(ctx, in.From)
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

func defaultAccountsPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".baresip", "accounts")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
