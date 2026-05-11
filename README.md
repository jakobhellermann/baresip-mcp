# baresip-mcp

An [MCP](https://modelcontextprotocol.io) server that exposes a running
[baresip](https://github.com/baresip/baresip) instance to LLM clients. It
talks to baresip through the `ctrl_tcp` module (JSON-over-netstring) and
publishes a set of typed tools plus an `recent_events` query for the
asynchronous event stream.

## Architecture

```
LLM client ──stdio MCP──▶ baresip-mcp ──TCP ctrl_tcp──▶ baresip
```

- `pkg/baresip` — TCP client with netstring framing, JSON command/response
  correlation via tokens, async event channel, and reconnect-with-backoff.
- `cmd/baresip-mcp` — MCP stdio server using
  [`github.com/modelcontextprotocol/go-sdk`](https://pkg.go.dev/github.com/modelcontextprotocol/go-sdk).

## Tools

| Tool             | Description                                          |
|------------------|------------------------------------------------------|
| `dial`           | Place an outgoing SIP call to a URI                  |
| `accept`         | Answer the ringing incoming call                     |
| `hangup`         | Hang up the current call                             |
| `hangup_all`     | Hang up all calls, optionally filtered by direction  |
| `list_calls`     | List active calls                                    |
| `call_status`    | Status of the current call                           |
| `reginfo`        | SIP registration status                              |
| `hold`           | Hold / resume the active call                        |
| `mute`           | Mute / unmute the active call                        |
| `transfer`       | Blind-transfer the active call                       |
| `dtmf`           | Send DTMF digits                                     |
| `uafind`         | Look up a configured User-Agent by AOR               |
| `recent_events`  | Return recent async baresip events as JSON           |
| `command`        | Raw escape hatch for any baresip long-form command   |

## Baresip configuration

Enable the `ctrl_tcp` module in your `~/.baresip/config`:

```
module          ctrl_tcp.so
ctrl_tcp_listen 127.0.0.1:4444
```

Start baresip as usual.

## Build & run

```sh
go build ./cmd/baresip-mcp
./baresip-mcp -addr 127.0.0.1:4444
```

The server speaks MCP over stdin/stdout, so it is meant to be spawned by
an MCP client (Claude Code, Claude Desktop, etc.).

### Configure as a Claude Code MCP server

Add the following to your `~/.claude/settings.json` (or project-local
`.mcp.json`):

```json
{
  "mcpServers": {
    "baresip": {
      "command": "/absolute/path/to/baresip-mcp",
      "args": ["-addr", "127.0.0.1:4444"]
    }
  }
}
```

Environment variable `BARESIP_CTRL_ADDR` overrides the default
`127.0.0.1:4444`.

## Tests

```sh
go test ./...
```

The package tests include an in-process fake `ctrl_tcp` server that
exercises the netstring framing, response correlation, and the
reconnect-on-disconnect supervisor.
