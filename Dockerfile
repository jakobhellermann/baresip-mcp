# Multi-stage build for baresip-mcp.
#
# The runtime image bundles baresip itself (with ctrl_tcp + menu) and the
# MCP binary. Treat it as a one-shot: stdin is the MCP transport, so run
# it with `-i` and have your MCP client spawn the container.

FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/baresip-mcp ./cmd/baresip-mcp

FROM debian:stable-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends baresip ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/baresip-mcp /usr/local/bin/baresip-mcp

ENV BARESIP_CTRL_ADDR=127.0.0.1:4444

# Note: this image does not start baresip for you — the MCP server connects
# to an existing ctrl_tcp endpoint. To run baresip alongside, override the
# entrypoint or use a sidecar container.
ENTRYPOINT ["/usr/local/bin/baresip-mcp"]
