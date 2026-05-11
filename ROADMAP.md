# TODO

- [x] **1. Real smoke test against a live baresip**
      `scripts/smoke.sh` — starts a real baresip with ctrl_tcp + menu modules
      in a tmpdir, drives baresip-mcp over stdio with an MCP initialize +
      `reginfo` + `list_calls` round-trip, and asserts on the responses.

- [x] **2. Push events as MCP notifications**
      Events are now fanned out to every connected `ServerSession.Log()` in
      addition to the ring buffer and stderr.

- [x] **3. Structured outputs for `list_calls`**
      `pkg/baresip` parses listcalls output into typed `UserAgentCalls`
      with per-call line/id/duration/state/on_hold/peer_uri. The MCP tool
      returns both a text content block and `structuredContent` so clients
      can pick whichever shape they prefer.

## Maybe next

- Structured output for `reginfo`. Format is per-line and a bit lossy
  (`reg_status` in `src/reg.c`); leaving as raw text until needed.
- CI: GitHub Actions running `go test ./...` and `scripts/smoke.sh`
  (smoke needs baresip on the runner — easy on linux via apt).
- Resource-based event stream (`baresip://events`) with subscribe.
