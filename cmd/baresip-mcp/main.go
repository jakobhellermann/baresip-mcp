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

const eventsResourceURI = "baresip://events"

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

func main() {
	accountsPath := flag.String("accounts", envOr("BARESIP_ACCOUNTS", defaultAccountsPath()), "path to a baresip accounts file (copied into the child baresip's tmpdir)")
	bufSize := flag.Int("event-buffer", 256, "size of the recent-events ring buffer")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

	// Non-nil subscribe handlers cause the SDK to advertise the
	// resources.subscribe capability. The SDK itself tracks which
	// sessions are subscribed; ResourceUpdated honors that internally.
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "baresip-mcp",
		Version: "0.1.0",
	}, &mcp.ServerOptions{
		SubscribeHandler:   func(context.Context, *mcp.SubscribeRequest) error { return nil },
		UnsubscribeHandler: func(context.Context, *mcp.UnsubscribeRequest) error { return nil },
	})

	server.AddResource(&mcp.Resource{
		URI:         eventsResourceURI,
		Name:        "baresip events",
		Description: "Ring buffer of recent asynchronous events emitted by baresip. Subscribe for push notifications.",
		MIMEType:    "application/json",
	}, eventsResourceHandler(events))

	// Fan events out to: ring buffer (queryable via recent_events and
	// the baresip://events resource), stderr log, logging/message
	// notifications to every connected session, and a resource-updated
	// notification so resource subscribers are pinged.
	go func() {
		for ev := range client.Events() {
			events.Add(ev)
			log.Printf("baresip event: class=%s type=%s param=%s", ev.Class, ev.Type, ev.Param)
			notifyEvent(server, ev)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = server.ResourceUpdated(ctx, &mcp.ResourceUpdatedNotificationParams{URI: eventsResourceURI})
			cancel()
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
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := client.Do(cctx, "uafind", in.AOR)
		cancel()
		if err != nil {
			return nil, nil, err
		}
		if !resp.OK {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: resp.Data}},
			}, nil, nil
		}
		regint := in.Regint
		if regint == 0 {
			regint = 600
		}
		return runCmd(ctx, client, "uareg", fmt.Sprintf("%d", regint))
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "unregister",
		Description: "Deregister a SIP account (regint=0). The account stays loaded but is no longer reachable for incoming calls.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in unregisterInput) (*mcp.CallToolResult, any, error) {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := client.Do(cctx, "uafind", in.AOR)
		cancel()
		if err != nil {
			return nil, nil, err
		}
		if !resp.OK {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: resp.Data}},
			}, nil, nil
		}
		return runCmd(ctx, client, "uareg", "0")
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

// dialHandler implements per-call custom headers without a race: baresip
// copies ua->custom_hdrs into the call at call-alloc time (see ua.c
// ua_call_alloc → call_set_custom_hdrs), so removing them on the UA right
// after dial leaves the active call's headers intact while keeping the UA
// clean for subsequent calls.
//
// baresip's uaaddheader / uarmheader require a UA index as the second
// argument when invoked via ctrl_tcp (menu_ua_carg insists on two
// whitespace-separated words). We resolve the AOR to an index by parsing
// reginfo. Headers therefore require a 'from' AOR.
func dialHandler(ctx context.Context, c *baresip.Client, in dialInput) (*mcp.CallToolResult, any, error) {
	if in.From != "" {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := c.Do(cctx, "uafind", in.From)
		cancel()
		if err != nil {
			return nil, nil, err
		}
		if !resp.OK {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: resp.Data}},
			}, nil, nil
		}
	}

	uaIdx := -1
	if len(in.Headers) > 0 {
		if in.From == "" {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: "headers require 'from' so the UA index can be resolved"}},
			}, nil, nil
		}
		idx, err := lookupUAIndex(ctx, c, in.From)
		if err != nil {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("resolve UA index for %s: %v", in.From, err)}},
			}, nil, nil
		}
		uaIdx = idx
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

	dialResult, _, dialErr := runCmd(ctx, c, "dial", in.URI)

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

func eventsResourceHandler(buf *baresip.EventBuffer) mcp.ResourceHandler {
	return func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		body, err := json.MarshalIndent(buf.Snapshot(0), "", "  ")
		if err != nil {
			return nil, err
		}
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI:      req.Params.URI,
				MIMEType: "application/json",
				Text:     string(body),
			}},
		}, nil
	}
}

func notifyEvent(server *mcp.Server, ev baresip.Event) {
	params := &mcp.LoggingMessageParams{
		Level:  "info",
		Logger: "baresip",
		Data: map[string]any{
			"class": ev.Class,
			"type":  ev.Type,
			"param": ev.Param,
			"extra": ev.Extra,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for session := range server.Sessions() {
		// Best-effort: a slow or dead client must not block the event pump.
		_ = session.Log(ctx, params)
	}
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
