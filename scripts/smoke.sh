#!/usr/bin/env bash
# Smoke test: start a real baresip with ctrl_tcp, connect baresip-mcp to it,
# send a single MCP tools/call for reginfo, and check the response.
#
# Requires: baresip (>= 4.x) on PATH and Go toolchain.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${BARESIP_CTRL_PORT:-44144}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"; kill $(jobs -p) 2>/dev/null || true' EXIT

echo "==> using temp dir $WORK"
mkdir -p "$WORK/.baresip"

# Minimal config: load only ctrl_tcp and a no-op audio stack so baresip
# starts headless. No accounts means reginfo returns "0 User Agents".
# Locate the modules directory (brew installs to /opt/homebrew/...).
MODPATH="${BARESIP_MODPATH:-}"
if [[ -z "$MODPATH" ]]; then
  for cand in /opt/homebrew/lib/baresip/modules /usr/local/lib/baresip/modules /usr/lib/x86_64-linux-gnu/baresip/modules /usr/lib/aarch64-linux-gnu/baresip/modules /usr/lib/baresip/modules; do
    if [[ -f "$cand/ctrl_tcp.so" ]]; then MODPATH="$cand"; break; fi
  done
fi
if [[ -z "$MODPATH" ]]; then
  echo "could not locate baresip modules dir; set BARESIP_MODPATH" >&2
  exit 1
fi
echo "==> module_path=$MODPATH"

cat >"$WORK/.baresip/config" <<EOF
sip_listen              127.0.0.1:0
module_path             $MODPATH
module                  menu.so
module                  ctrl_tcp.so
ctrl_tcp_listen         127.0.0.1:$PORT
EOF
: >"$WORK/.baresip/accounts"

echo "==> building baresip-mcp"
( cd "$ROOT" && go build -o "$WORK/baresip-mcp" ./cmd/baresip-mcp )

echo "==> starting baresip headless"
HOME="$WORK" baresip -f "$WORK/.baresip" >"$WORK/baresip.log" 2>&1 &
BARESIP_PID=$!

# Wait for ctrl_tcp port to accept connections.
for i in $(seq 1 50); do
  if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then break; fi
  sleep 0.1
done
if ! nc -z 127.0.0.1 "$PORT" 2>/dev/null; then
  echo "baresip did not open ctrl_tcp on 127.0.0.1:$PORT"
  echo "--- baresip log ---"; cat "$WORK/baresip.log"
  exit 1
fi
echo "==> baresip ctrl_tcp ready on 127.0.0.1:$PORT (pid $BARESIP_PID)"

echo "==> driving baresip-mcp over stdio"
# Two JSON-RPC requests + the initialized notification. The MCP server speaks
# newline-delimited JSON-RPC 2.0 over stdio per the spec.
INIT='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}'
INITED='{"jsonrpc":"2.0","method":"notifications/initialized"}'
CALL1='{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"reginfo","arguments":{}}}'
CALL2='{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_calls","arguments":{}}}'

OUT="$( { printf '%s\n%s\n%s\n%s\n' "$INIT" "$INITED" "$CALL1" "$CALL2"; sleep 1; } | \
  "$WORK/baresip-mcp" -addr "127.0.0.1:$PORT" 2>"$WORK/mcp.log" || true )"

echo "--- mcp stdout ---"
echo "$OUT"
echo "--- mcp log ---"
cat "$WORK/mcp.log" || true

# The reginfo response should be id=2 with a content array.
if ! grep -q '"id":2' <<<"$OUT"; then
  echo "FAIL: no response with id=2"
  exit 1
fi
if ! grep -q 'User Agents' <<<"$OUT"; then
  echo "FAIL: reginfo content did not include 'User Agents'"
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

echo "==> SMOKE OK"
