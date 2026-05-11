#!/usr/bin/env bash
# Smoke test: build baresip-mcp and drive it over stdio. baresip-mcp itself
# spawns a child baresip in a tmpdir; this script no longer needs to start
# baresip separately.
#
# Requires: baresip (>= 4.x) on PATH and Go toolchain.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"; kill $(jobs -p) 2>/dev/null || true' EXIT

echo "==> using temp dir $WORK"

echo "==> building baresip-mcp"
( cd "$ROOT" && go build -o "$WORK/baresip-mcp" ./cmd/baresip-mcp )

echo "==> driving baresip-mcp over stdio (spawns its own baresip)"
INIT='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}'
INITED='{"jsonrpc":"2.0","method":"notifications/initialized"}'
CALL1='{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"reginfo","arguments":{}}}'
CALL2='{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_calls","arguments":{}}}'
LIST_TOOLS='{"jsonrpc":"2.0","id":6,"method":"tools/list"}'

# Two regint=0 fake accounts so the fleet has something to spawn but
# nothing reaches the network. Hermetic.
cat >"$WORK/accounts" <<EOF
<sip:smoke1@localhost>;regint=0
<sip:smoke2@localhost>;regint=0
EOF

OUT="$( { printf '%s\n%s\n%s\n%s\n%s\n' "$INIT" "$INITED" "$CALL1" "$CALL2" "$LIST_TOOLS"; sleep 3; } | \
  "$WORK/baresip-mcp" -accounts "$WORK/accounts" 2>"$WORK/mcp.log" || true )"

echo "--- mcp stdout ---"
echo "$OUT"
echo "--- mcp log ---"
cat "$WORK/mcp.log" || true

if ! grep -q '"id":2' <<<"$OUT"; then
  echo "FAIL: no response with id=2"
  exit 1
fi
if ! grep -qE 'User Agents|dormant' <<<"$OUT"; then
  echo "FAIL: reginfo content did not include 'User Agents' or 'dormant'"
  exit 1
fi
if ! grep -q '"id":3' <<<"$OUT"; then
  echo "FAIL: no response with id=3 (list_calls)"
  exit 1
fi
if ! grep -q '"user_agents"' <<<"$OUT"; then
  echo "FAIL: list_calls did not return structured output"
  exit 1
fi
for tool in dial accept hangup hangup_all list_calls call_status reginfo hold mute transfer dtmf register unregister recent_events wait_for_event command; do
  if ! grep -q "\"name\":\"$tool\"" <<<"$OUT"; then
    echo "FAIL: tool '$tool' not advertised in tools/list"
    exit 1
  fi
done

echo "==> SMOKE OK"
