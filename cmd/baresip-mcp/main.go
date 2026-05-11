// Command baresip-mcp exposes a baresip ctrl_tcp connection as an MCP server.
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
	"sort"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/sipgate/baresip-mcp/pkg/baresip"
)

type dialInput struct {
	URI     string            `json:"uri" jsonschema:"SIP URI to dial, e.g. sip:alice@example.com"`
	From    string            `json:"from,omitempty" jsonschema:"optional AOR of the local account to call from, e.g. sip:1126226e1@proxy.dev.sipgate.de. If omitted, baresip picks a matching UA by URI host."`
	Headers map[string]string `json:"headers,omitempty" jsonschema:"optional SIP headers to attach to this specific outgoing INVITE, e.g. {\"X-Client-Correlation-ID\": \"abc-123\"}. Headers are scoped to this call only — they're set on the UA before dial, copied into the call by baresip, then removed from the UA."`
}

type empty struct{}

type rawInput struct {
	Command string `json:"command" jsonschema:"baresip long-form command name, e.g. dial, hangup, reginfo"`
	Params  string `json:"params,omitempty" jsonschema:"optional parameters appended to the command"`
}

type transferInput struct {
	URI string `json:"uri" jsonschema:"target SIP URI to blind-transfer the current call to"`
}

type dtmfInput struct {
	Digits string `json:"digits" jsonschema:"DTMF digits to send to the active call, e.g. 1234#"`
}

type muteInput struct {
	On bool `json:"on" jsonschema:"true to mute, false to un-mute the active call"`
}

type holdInput struct {
	On bool `json:"on" jsonschema:"true to put the active call on hold, false to resume"`
}

type hangupAllInput struct {
	Direction string `json:"direction,omitempty" jsonschema:"optional filter: 'in' or 'out'. Empty hangs up all calls."`
}

type uafindInput struct {
	AOR string `json:"aor" jsonschema:"address-of-record to look up, e.g. sip:alice@example.com"`
}

type registerInput struct {
	AOR    string `json:"aor" jsonschema:"AOR of the account to register, e.g. sip:1126226e1@proxy.dev.sipgate.de"`
	Regint int    `json:"regint,omitempty" jsonschema:"registration interval in seconds (default 600). 0 means do not register."`
}

type unregisterInput struct {
	AOR string `json:"aor" jsonschema:"AOR of the account to deregister"`
}

type recentEventsInput struct {
	Limit int `json:"limit,omitempty" jsonschema:"max number of most recent events to return (default 50)"`
}

type waitForEventInput struct {
	Types           []string `json:"types,omitempty" jsonschema:"event types to match (e.g. CALL_ESTABLISHED, CALL_CLOSED). Empty matches any event."`
	CallID          string   `json:"call_id,omitempty" jsonschema:"optional baresip call id to filter on. If set, only events for that call match."`
	TimeoutSeconds  int      `json:"timeout_seconds,omitempty" jsonschema:"max seconds to wait (default 30)"`
	LookbackSeconds int      `json:"lookback_seconds,omitempty" jsonschema:"check the recent-events buffer for a matching event that already arrived this many seconds ago (default 30). Set to -1 to disable lookback and only wait for new events."`
}

type waitForEventOutput struct {
	Event    *baresip.RecordedEvent `json:"event"`
	TimedOut bool                   `json:"timed_out"`
}

func main() {
	accountsPath := flag.String("accounts", envOr("BARESIP_ACCOUNTS", defaultAccountsPath()), "path to a baresip accounts file (copied into the child baresip's tmpdir)")
	bufSize := flag.Int("event-buffer", 256, "size of the recent-events ring buffer")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Take out any baresips left over from previous MCP instances that
	// died without running their defer (Claude Code SIGKILL on reload).
	sweepOrphans()

	instance, err := spawnBaresip(*accountsPath)
	if err != nil {
		log.Fatalf("spawn baresip: %v", err)
	}
	defer instance.Close()
	log.Printf("spawned baresip ctrl_tcp=%s tmpdir=%s log=%s", instance.addr, instance.tmpDir, instance.logPath)

	client := baresip.New(instance.addr)
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	if err := client.Connect(dialCtx); err != nil {
		cancel()
		log.Fatalf("connect spawned baresip at %s: %v", instance.addr, err)
	}
	cancel()
	defer client.Close()

	events := baresip.NewEventBuffer(*bufSize)
	fanout := baresip.NewEventFanout()

	server := mcp.NewServer(&mcp.Implementation{
		Name:    "baresip-mcp",
		Version: "0.1.0",
	}, nil)

	// Fan events out to the ring buffer (recent_events), stderr, and the
	// in-process fanout (wait_for_event). We don't push events via MCP
	// notifications because Claude Code, the primary client, ignores them.
	go func() {
		for ev := range client.Events() {
			events.Add(ev)
			fanout.Publish(ev)
			log.Printf("baresip event: class=%s type=%s param=%s", ev.Class, ev.Type, ev.Param)
		}
	}()

	mcp.AddTool(server, &mcp.Tool{
		Name:        "dial",
		Description: "Place an outgoing SIP call to the given URI via baresip. Pass 'from' to pick a specific local account as the caller. Pass 'headers' to attach custom SIP headers to this specific INVITE.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in dialInput) (*mcp.CallToolResult, any, error) {
		return dialHandler(ctx, client, in)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "accept",
		Description: "Accept (answer) the currently ringing incoming call.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ empty) (*mcp.CallToolResult, any, error) {
		return runCmd(ctx, client, "accept", "")
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "hangup",
		Description: "Hang up the current call.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ empty) (*mcp.CallToolResult, any, error) {
		return runCmd(ctx, client, "hangup", "")
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "hangup_all",
		Description: "Hang up all active calls, optionally filtered by direction ('in' or 'out').",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in hangupAllInput) (*mcp.CallToolResult, any, error) {
		return runCmd(ctx, client, "hangupall", in.Direction)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_calls",
		Description: "List all active calls. Returns structured per-UA call details.",
	}, listCallsHandler(client))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "call_status",
		Description: "Show status of the currently active call.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ empty) (*mcp.CallToolResult, any, error) {
		return runCmd(ctx, client, "callstat", "")
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "reginfo",
		Description: "Show registration status of all configured SIP accounts. Returns structured per-AOR registration state.",
	}, reginfoHandler(client))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "hold",
		Description: "Put the active call on hold or resume it.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in holdInput) (*mcp.CallToolResult, any, error) {
		// baresip's "hold" command toggles; we map on/off explicitly.
		cmd := "hold"
		if !in.On {
			cmd = "resume"
		}
		return runCmd(ctx, client, cmd, "")
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "mute",
		Description: "Mute or un-mute the active call.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in muteInput) (*mcp.CallToolResult, any, error) {
		val := "false"
		if in.On {
			val = "true"
		}
		return runCmd(ctx, client, "mute", val)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "transfer",
		Description: "Blind-transfer the active call to the given SIP URI.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in transferInput) (*mcp.CallToolResult, any, error) {
		return runCmd(ctx, client, "transfer", in.URI)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "dtmf",
		Description: "Send DTMF digits to the active call.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in dtmfInput) (*mcp.CallToolResult, any, error) {
		return runCmd(ctx, client, "sndcode", in.Digits)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "uafind",
		Description: "Find a configured User-Agent by address-of-record and make it the current one.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in uafindInput) (*mcp.CallToolResult, any, error) {
		return runCmd(ctx, client, "uafind", in.AOR)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "register",
		Description: "Register a configured SIP account at its provider so it can receive incoming calls. Pass regint=0 to stop registering.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in registerInput) (*mcp.CallToolResult, any, error) {
		regint := in.Regint
		if regint == 0 {
			regint = 600
		}
		return uaregByAOR(ctx, client, in.AOR, regint)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "unregister",
		Description: "Deregister a SIP account (regint=0). The account stays loaded but is no longer reachable for incoming calls.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in unregisterInput) (*mcp.CallToolResult, any, error) {
		return uaregByAOR(ctx, client, in.AOR, 0)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "recent_events",
		Description: "Return recent asynchronous baresip events (call state, registration, messages) as JSON.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in recentEventsInput) (*mcp.CallToolResult, any, error) {
		limit := in.Limit
		if limit == 0 {
			limit = 50
		}
		snap := events.Snapshot(limit)
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
	}, waitForEventHandler(events, fanout))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "command",
		Description: "Send an arbitrary baresip long-form command. Escape hatch for commands not exposed as dedicated tools.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in rawInput) (*mcp.CallToolResult, any, error) {
		return runCmd(ctx, client, in.Command, in.Params)
	})

	if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil {
		log.Fatalf("mcp server: %v", err)
	}
}

type listCallsOutput struct {
	UserAgents []baresip.UserAgentCalls `json:"user_agents"`
	Raw        string                   `json:"raw"`
}

func listCallsHandler(c *baresip.Client) func(context.Context, *mcp.CallToolRequest, empty) (*mcp.CallToolResult, listCallsOutput, error) {
	return func(ctx context.Context, _ *mcp.CallToolRequest, _ empty) (*mcp.CallToolResult, listCallsOutput, error) {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		resp, err := c.Do(cctx, "listcalls", "")
		if err != nil {
			return nil, listCallsOutput{}, err
		}
		out := listCallsOutput{
			UserAgents: baresip.ParseListCalls(resp.Data),
			Raw:        resp.Data,
		}
		return &mcp.CallToolResult{
			IsError: !resp.OK,
			Content: []mcp.Content{&mcp.TextContent{Text: resp.Data}},
		}, out, nil
	}
}

type reginfoOutput struct {
	Registrations []baresip.Registration `json:"registrations"`
	Raw           string                 `json:"raw"`
}

func reginfoHandler(c *baresip.Client) func(context.Context, *mcp.CallToolRequest, empty) (*mcp.CallToolResult, reginfoOutput, error) {
	return func(ctx context.Context, _ *mcp.CallToolRequest, _ empty) (*mcp.CallToolResult, reginfoOutput, error) {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		resp, err := c.Do(cctx, "reginfo", "")
		if err != nil {
			return nil, reginfoOutput{}, err
		}
		out := reginfoOutput{
			Registrations: baresip.ParseRegInfo(resp.Data),
			Raw:           resp.Data,
		}
		return &mcp.CallToolResult{
			IsError: !resp.OK,
			Content: []mcp.Content{&mcp.TextContent{Text: resp.Data}},
		}, out, nil
	}
}

// dialHandler implements 'from' selection and per-call custom headers
// without a race. baresip's commands invoked through ctrl_tcp go through
// menu_ua_carg, which requires an explicit UA index — uafind alone is
// not honored. So when 'from' is given we resolve the AOR to its index
// via reginfo and pass it to dial / uaaddheader / uarmheader.
//
// Headers are race-free because baresip copies ua->custom_hdrs into the
// call at call_alloc time (ua.c ua_call_alloc → call_set_custom_hdrs).
// Removing them on the UA right after dial leaves the active call
// intact while keeping the UA clean for subsequent calls.
func dialHandler(ctx context.Context, c *baresip.Client, in dialInput) (*mcp.CallToolResult, any, error) {
	uaIdx := -1
	if in.From != "" {
		idx, err := lookupUAIndex(ctx, c, in.From)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("resolve UA index for %s: %v", in.From, err)}},
			}, nil, nil
		}
		uaIdx = idx
	}

	if len(in.Headers) > 0 && uaIdx < 0 {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: "headers require 'from' so the UA index can be resolved"}},
		}, nil, nil
	}

	keys := make([]string, 0, len(in.Headers))
	for k := range in.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		// baresip's uri_header_unescape runs on the value, so callers can
		// percent-encode anything special. We pass through as-is.
		arg := fmt.Sprintf("%s=%s %d", k, in.Headers[k], uaIdx)
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

	dialArg := in.URI
	if uaIdx >= 0 {
		dialArg = fmt.Sprintf("%s %d", in.URI, uaIdx)
	}
	dialResult, _, dialErr := runCmd(ctx, c, "dial", dialArg)

	// Clean up headers from the UA regardless of dial outcome. The active
	// call (if dial succeeded) keeps them because baresip copied them at
	// call_alloc time.
	for _, k := range keys {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, _ = c.Do(cctx, "uarmheader", fmt.Sprintf("%s %d", k, uaIdx))
		cancel()
	}

	return dialResult, nil, dialErr
}

// uaregByAOR resolves an AOR to a UA index via reginfo, then runs uareg
// with both the regint and explicit UA index. baresip's cmd_uareg uses
// menu_ua_carg which requires two whitespace-separated words (regint and
// index) when carg->data is NULL; without the index it returns NULL and
// the handler silently no-ops.
func uaregByAOR(ctx context.Context, c *baresip.Client, aor string, regint int) (*mcp.CallToolResult, any, error) {
	idx, err := lookupUAIndex(ctx, c, aor)
	if err != nil {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("resolve UA index for %s: %v", aor, err)}},
		}, nil, nil
	}
	return runCmd(ctx, c, "uareg", fmt.Sprintf("%d %d", regint, idx))
}

func lookupUAIndex(ctx context.Context, c *baresip.Client, aor string) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	resp, err := c.Do(cctx, "reginfo", "")
	cancel()
	if err != nil {
		return 0, err
	}
	if !resp.OK {
		return 0, fmt.Errorf("reginfo failed: %s", resp.Data)
	}
	for _, r := range baresip.ParseRegInfo(resp.Data) {
		if r.AOR == aor {
			return r.Index, nil
		}
	}
	return 0, fmt.Errorf("AOR %s not found in reginfo", aor)
}

func waitForEventHandler(buf *baresip.EventBuffer, fan *baresip.EventFanout) func(context.Context, *mcp.CallToolRequest, waitForEventInput) (*mcp.CallToolResult, waitForEventOutput, error) {
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

		// Subscribe first so we don't miss events that arrive while we're
		// scanning the lookback buffer.
		ch, unsub := fan.Subscribe()
		defer unsub()

		// Lookback: check the ring buffer for a matching event newer than
		// (now - lookback).
		if lookback > 0 {
			cutoff := time.Now().Add(-lookback)
			snap := buf.Snapshot(0)
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
