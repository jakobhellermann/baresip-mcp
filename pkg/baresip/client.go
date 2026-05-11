// Package baresip provides a TCP client for baresip's ctrl_tcp module.
//
// Protocol: JSON messages wrapped in netstrings. See baresip/modules/ctrl_tcp.
package baresip

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

type Command struct {
	Command string `json:"command"`
	Params  string `json:"params,omitempty"`
	Token   string `json:"token,omitempty"`
}

type Response struct {
	Response bool   `json:"response"`
	OK       bool   `json:"ok"`
	Data     string `json:"data"`
	Token    string `json:"token,omitempty"`
}

// Event is a baresip-emitted async message. Unknown keys are kept in Extra.
type Event struct {
	Class string `json:"class,omitempty"`
	Type  string `json:"type,omitempty"`
	Param string `json:"param,omitempty"`
	Extra map[string]any
	Raw   json.RawMessage
}

type Client struct {
	addr   string
	dialer net.Dialer

	mu      sync.Mutex
	conn    net.Conn
	br      *bufio.Reader
	pending map[string]chan Response
	events  chan Event
	closed  bool
	done    chan struct{}
}

func New(addr string) *Client {
	return &Client{
		addr:    addr,
		dialer:  net.Dialer{Timeout: 5 * time.Second},
		pending: make(map[string]chan Response),
		events:  make(chan Event, 64),
		done:    make(chan struct{}),
	}
}

// Connect dials baresip. Must be called before Do.
func (c *Client) Connect(ctx context.Context) error {
	conn, err := c.dialer.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return fmt.Errorf("dial baresip ctrl_tcp at %s: %w", c.addr, err)
	}
	c.mu.Lock()
	c.conn = conn
	c.br = bufio.NewReader(conn)
	c.mu.Unlock()
	go c.readLoop()
	return nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	<-c.done
	return nil
}

// Events returns a channel of asynchronous events from baresip.
// The channel is closed when the client is closed.
func (c *Client) Events() <-chan Event {
	return c.events
}

// Do sends a command and waits for the matching response.
func (c *Client) Do(ctx context.Context, command, params string) (Response, error) {
	tok, err := randomToken()
	if err != nil {
		return Response{}, err
	}
	ch := make(chan Response, 1)

	c.mu.Lock()
	if c.closed || c.conn == nil {
		c.mu.Unlock()
		return Response{}, errors.New("baresip: client not connected")
	}
	c.pending[tok] = ch
	conn := c.conn
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, tok)
		c.mu.Unlock()
	}()

	payload, err := json.Marshal(Command{Command: command, Params: params, Token: tok})
	if err != nil {
		return Response{}, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(deadline)
		defer conn.SetWriteDeadline(time.Time{})
	}
	if err := writeNetstring(conn, payload); err != nil {
		return Response{}, fmt.Errorf("write command: %w", err)
	}

	select {
	case resp := <-ch:
		return resp, nil
	case <-ctx.Done():
		return Response{}, ctx.Err()
	case <-c.done:
		return Response{}, errors.New("baresip: connection closed")
	}
}

func (c *Client) readLoop() {
	defer close(c.done)
	defer close(c.events)
	for {
		c.mu.Lock()
		br := c.br
		c.mu.Unlock()
		if br == nil {
			return
		}
		buf, err := readNetstring(br)
		if err != nil {
			c.mu.Lock()
			for _, ch := range c.pending {
				close(ch)
			}
			c.pending = map[string]chan Response{}
			c.mu.Unlock()
			return
		}
		c.dispatch(buf)
	}
}

func (c *Client) dispatch(buf []byte) {
	// Peek at the message type. Responses have "response": true,
	// events have "event": true, SIP messages have "message": true.
	var probe struct {
		Response bool `json:"response"`
		Event    bool `json:"event"`
	}
	if err := json.Unmarshal(buf, &probe); err != nil {
		return
	}
	switch {
	case probe.Response:
		var r Response
		if err := json.Unmarshal(buf, &r); err != nil {
			return
		}
		c.mu.Lock()
		ch, ok := c.pending[r.Token]
		if ok {
			delete(c.pending, r.Token)
		}
		c.mu.Unlock()
		if ok {
			ch <- r
		}
	case probe.Event:
		var extra map[string]any
		_ = json.Unmarshal(buf, &extra)
		ev := Event{
			Class: stringFrom(extra, "class"),
			Type:  stringFrom(extra, "type"),
			Param: stringFrom(extra, "param"),
			Extra: extra,
			Raw:   append(json.RawMessage(nil), buf...),
		}
		select {
		case c.events <- ev:
		default:
			// Drop if consumer is slow; events are best-effort.
		}
	}
}

func stringFrom(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func randomToken() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
