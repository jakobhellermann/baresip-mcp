// Command baresip-mcp exposes a baresip ctrl_tcp connection as an MCP server.
package main

import (
	"context"
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

const defaultAddr = "127.0.0.1:4444"

type dialInput struct {
	URI string `json:"uri" jsonschema:"SIP URI to dial, e.g. sip:alice@example.com"`
}

type acceptInput struct{}
type hangupInput struct{}
type reginfoInput struct{}

type rawInput struct {
	Command string `json:"command" jsonschema:"baresip long-form command name, e.g. dial, hangup, reginfo"`
	Params  string `json:"params,omitempty" jsonschema:"optional parameters appended to the command"`
}

func main() {
	addr := flag.String("addr", envOr("BARESIP_CTRL_ADDR", defaultAddr), "baresip ctrl_tcp address host:port")
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

	// Drain events so the channel buffer doesn't fill up. For now we log them;
	// later we can expose them as MCP notifications or a resource.
	go func() {
		for ev := range client.Events() {
			log.Printf("baresip event: class=%s type=%s param=%s", ev.Class, ev.Type, ev.Param)
		}
	}()

	server := mcp.NewServer(&mcp.Implementation{
		Name:    "baresip-mcp",
		Version: "0.1.0",
	}, nil)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "dial",
		Description: "Place an outgoing SIP call to the given URI via baresip.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in dialInput) (*mcp.CallToolResult, any, error) {
		return runCmd(ctx, client, "dial", in.URI)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "accept",
		Description: "Accept (answer) the currently ringing incoming call.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ acceptInput) (*mcp.CallToolResult, any, error) {
		return runCmd(ctx, client, "accept", "")
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "hangup",
		Description: "Hang up the current call.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ hangupInput) (*mcp.CallToolResult, any, error) {
		return runCmd(ctx, client, "hangup", "")
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "reginfo",
		Description: "Show registration status of all configured SIP accounts.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ reginfoInput) (*mcp.CallToolResult, any, error) {
		return runCmd(ctx, client, "reginfo", "")
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

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
