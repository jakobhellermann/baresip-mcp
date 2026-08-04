**Status:** AI slop but it works for me

# baresip-mcp

An [MCP](https://modelcontextprotocol.io) server that drives
[baresip](https://github.com/baresip/baresip) on behalf of an LLM client.
It starts with an empty fleet; accounts are introduced at runtime via
the `register` tool, and each gets its own headless baresip child with
its own tmpdir, SIP port, and `ctrl_tcp` listener. It publishes a set
of typed tools plus a `recent_events` query for the asynchronous event
stream.

## Architecture

```
                                            ┌─▶ baresip (account A)
LLM client ──stdio MCP ──▶ baresip-mcp ─────┼─▶ baresip (account B)
                                            └─▶ baresip (account C)
```

- `pkg/baresip` — TCP client with netstring framing, JSON command/response
  correlation via tokens, async event channel, and reconnect-with-backoff.
- `cmd/baresip-mcp` — MCP stdio server using
  [`github.com/modelcontextprotocol/go-sdk`](https://pkg.go.dev/github.com/modelcontextprotocol/go-sdk);
  manages the per-account baresip fleet.

One process per account is intentional: sipgate hairpins INVITEs between
two local accounts, so each leg needs its own SIP socket to avoid
state-machine collisions.

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
| `register`       | Introduce an account (AOR + password) and register it; can also set auto-answer, transport, outbound proxy (respawns the child) |
| `unregister`     | Deregister an account                                |
| `inspect_account`| Show the per-child accounts line, tmpdir, ctrl_tcp address, and tail of the baresip log |
| `recent_events`  | Return recent async baresip events as JSON           |
| `wait_for_event` | Block until a matching event arrives (by type / call_id / AOR) |
| `command`        | Raw escape hatch for any baresip long-form command   |

## Baresip configuration

You do **not** need to run baresip yourself or hand-configure
`ctrl_tcp` — baresip-mcp spawns one baresip child per registered
account and writes the necessary config into a per-child tmpdir.

baresip-mcp reads **no** accounts file. Accounts come in at runtime
through the `register` MCP tool (AOR + `password`, plus optional
`username` / `extra_params`). The credentials live only in memory and
in the spawned child's tmpdir.

The `baresip` binary must be on `PATH`, and the baresip module
directory must be discoverable (set `BARESIP_MODPATH` if your install
keeps modules in a non-standard location).

## Build & run

```sh
go build ./cmd/baresip-mcp
./baresip-mcp
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
      "command": "/absolute/path/to/baresip-mcp"
    }
  }
}
```

### …or without installing anything, via the Docker image

The published image bundles baresip, so there is nothing to build or put
on `PATH`. Point the MCP client at `docker` — the container is the server
and speaks MCP over the stdio it inherits:

```json
{
  "mcpServers": {
    "baresip": {
      "command": "docker",
      "args": ["run", "-i", "--rm", "--network", "host",
               "ghcr.io/jakobhellermann/baresip-mcp:latest"]
    }
  }
}
```

SIP and RTP are UDP, so `--network host` (Linux) is usually needed for
registration and media to traverse NAT. On Docker Desktop (macOS/Windows)
host networking is limited; a locally built binary is the smoother path
there.

## Tests

```sh
go test ./...
```

The package tests include an in-process fake `ctrl_tcp` server that
exercises the netstring framing, response correlation, and the
reconnect-on-disconnect supervisor.
