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
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/sipgate/baresip-mcp/pkg/baresip"
)

const (
	defaultAddr       = "127.0.0.1:4444"
	eventsResourceURI = "baresip://events"
)

type dialInput struct {
	URI string `json:"uri" jsonschema:"SIP URI to dial, e.g. sip:alice@example.com"`
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

type recentEventsInput struct {
	Limit int `json:"limit,omitempty" jsonschema:"max number of most recent events to return (default 50)"`
}

func main() {
	addr := flag.String("addr", envOr("BARESIP_CTRL_ADDR", defaultAddr), "baresip ctrl_tcp address host:port")
	bufSize := flag.Int("event-buffer", 256, "size of the recent-events ring buffer")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client := baresip.New(*addr)
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	if err := client.Connect(dialCtx); err != nil {
		cancel()
		log.Fatalf("connect baresip: %v", err)
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
		Description: "Place an outgoing SIP call to the given URI via baresip.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in dialInput) (*mcp.CallToolResult, any, error) {
		return runCmd(ctx, client, "dial", in.URI)
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
		Description: "Find a configured User-Agent by address-of-record.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in uafindInput) (*mcp.CallToolResult, any, error) {
		return runCmd(ctx, client, "uafind", in.AOR)
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

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
