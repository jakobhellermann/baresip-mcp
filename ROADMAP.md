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

- [x] **4. Structured output for `reginfo`**
      Parsed via `pkg/baresip.ParseRegInfo` into typed `Registration`
      entries (aor, status OK/ERR/zzz, fallback flag, server, expires).

- [x] **5. CI**
      `.github/workflows/ci.yml` — go vet + go test -race on every push,
      plus a separate job that installs baresip via apt and runs
      `scripts/smoke.sh` against the freshly-built MCP binary.

- [x] **6. Resource-based event stream**
      `baresip://events` MCP resource. ResourceHandler returns the
      ring buffer as JSON. Each incoming baresip event fires
      `ResourceUpdated` so subscribed clients get push notifications
      via the standard resources mechanism. Smoke test asserts
      resources/list and resources/read both work.

- [x] **7. Dockerfile**
      Two-stage build: golang:1.26 → debian-slim with baresip from apt
      and the MCP binary at /usr/local/bin/baresip-mcp.
- [x] **8. Smoke covers full tool surface**
      tools/list is asserted against every declared tool name.

- [x] **9. End-to-end test with two real baresips**
      `test/e2e/e2e_test.go` (build tag `e2e`) spawns two baresip
      processes on loopback (no registrar / no provider), dials A→B
      by URI, accepts on B, asserts the CALL_ESTABLISHED and
      CALL_CLOSED events arrive on A's ctrl_tcp event stream.

## Maybe next

- More structured parsers (`callstat` outputs multi-line debug text
  that's brittle to parse — leave as raw via the `command` tool).
- Nix flake (mirroring the Dockerfile for the nix-using crowd here).
- Run the e2e test in CI (needs `baresip` apt package on the runner;
  the smoke job already installs it, so a single combined job would
  cover both).
