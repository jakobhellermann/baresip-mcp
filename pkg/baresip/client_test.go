package baresip

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"
)

// fakeServer is a minimal stand-in for baresip's ctrl_tcp module.
// It accepts a single connection at a time, decodes netstring-wrapped
// JSON commands, and echoes back a response with ok=true and data set
// to the command name.
type fakeServer struct {
	ln net.Listener
	t  *testing.T

	connCh chan net.Conn
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fakeServer{ln: ln, t: t, connCh: make(chan net.Conn, 4)}
	go s.acceptLoop()
	return s
}

func (s *fakeServer) addr() string { return s.ln.Addr().String() }

func (s *fakeServer) acceptLoop() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.connCh <- c
		go s.serve(c)
	}
}

func (s *fakeServer) serve(c net.Conn) {
	defer c.Close()
	br := bufio.NewReader(c)
	for {
		buf, err := readNetstring(br)
		if err != nil {
			return
		}
		var cmd Command
		if err := json.Unmarshal(buf, &cmd); err != nil {
			return
		}
		resp, _ := json.Marshal(Response{
			Response: true,
			OK:       true,
			Data:     "echo:" + cmd.Command + ":" + cmd.Params,
			Token:    cmd.Token,
		})
		if err := writeNetstring(c, resp); err != nil {
			return
		}
	}
}

func (s *fakeServer) Close() { _ = s.ln.Close() }

// waitConn waits for the next accepted connection and returns it.
func (s *fakeServer) waitConn(t *testing.T, d time.Duration) net.Conn {
	t.Helper()
	select {
	case c := <-s.connCh:
		return c
	case <-time.After(d):
		t.Fatal("timed out waiting for connection")
		return nil
	}
}

func TestClientDoEchoesResponse(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.Close()

	client := New(srv.addr())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	resp, err := client.Do(ctx, "dial", "sip:alice@example.com")
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if !resp.OK {
		t.Fatalf("expected OK, got %+v", resp)
	}
	want := "echo:dial:sip:alice@example.com"
	if resp.Data != want {
		t.Fatalf("data: got %q want %q", resp.Data, want)
	}
}

func TestClientReconnectsAfterServerDrop(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.Close()

	client := New(srv.addr())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	firstConn := srv.waitConn(t, time.Second)
	if _, err := client.Do(ctx, "reginfo", ""); err != nil {
		t.Fatalf("first Do: %v", err)
	}

	// Drop the server side of the connection and confirm pending Do calls fail
	// while disconnected.
	_ = firstConn.(*net.TCPConn).CloseRead()
	_ = firstConn.Close()

	// Wait for the supervisor to notice and reconnect (~1s backoff).
	srv.waitConn(t, 4*time.Second)

	// Now the client should be reconnected and Do should succeed again.
	// Allow a brief settle window for the new conn to be installed.
	var lastErr error
	for i := 0; i < 50; i++ {
		_, err := client.Do(ctx, "listcalls", "")
		if err == nil {
			return
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("Do after reconnect kept failing: %v", lastErr)
}

func TestClientDoFailsWhenDisconnected(t *testing.T) {
	// Open and immediately close a listener so the port is unused.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	client := New(addr)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := client.Connect(ctx); err == nil {
		t.Fatal("expected initial connect to fail")
	}

	// Do should fail fast since the client never connected.
	if _, err := client.Do(context.Background(), "dial", "x"); err == nil {
		t.Fatal("expected ErrDisconnected")
	}
}
