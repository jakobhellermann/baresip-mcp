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
	"log"
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

var ErrDisconnected = errors.New("baresip: not connected")

type Client struct {
	addr   string
	dialer net.Dialer
	logger *log.Logger

	mu      sync.Mutex
	conn    net.Conn
	pending map[string]chan Response

	events chan Event

	closeOnce sync.Once
	closeCh   chan struct{}
	doneCh    chan struct{}
}

type Option func(*Client)

func WithLogger(l *log.Logger) Option {
	return func(c *Client) { c.logger = l }
}

func New(addr string, opts ...Option) *Client {
	c := &Client{
		addr:    addr,
		dialer:  net.Dialer{Timeout: 5 * time.Second},
		logger:  log.New(log.Writer(), "baresip: ", log.LstdFlags),
		pending: make(map[string]chan Response),
		events:  make(chan Event, 64),
		closeCh: make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Connect dials baresip and starts the supervisor goroutine that
// reconnects with backoff on disconnect.
// The first dial is synchronous; if it fails, Connect returns the error
// and the supervisor does not start.
func (c *Client) Connect(ctx context.Context) error {
	conn, err := c.dialer.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return fmt.Errorf("dial baresip ctrl_tcp at %s: %w", c.addr, err)
	}
	c.setConn(conn)
	go c.supervise()
	return nil
}

// Close shuts the client down and waits for the supervisor to exit.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		close(c.closeCh)
		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
	})
	<-c.doneCh
	return nil
}

// Events returns a channel of asynchronous events. Closed when the client is closed.
func (c *Client) Events() <-chan Event { return c.events }

// Do sends a command and waits for the matching response.
// If currently disconnected, returns ErrDisconnected.
func (c *Client) Do(ctx context.Context, command, params string) (Response, error) {
	tok, err := randomToken()
	if err != nil {
		return Response{}, err
	}
	ch := make(chan Response, 1)

	c.mu.Lock()
	if c.conn == nil {
		c.mu.Unlock()
		return Response{}, ErrDisconnected
	}
	conn := c.conn
	c.pending[tok] = ch
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
	case resp, ok := <-ch:
		if !ok {
			return Response{}, ErrDisconnected
		}
		return resp, nil
	case <-ctx.Done():
		return Response{}, ctx.Err()
	case <-c.closeCh:
		return Response{}, errors.New("baresip: client closed")
	}
}

func (c *Client) setConn(conn net.Conn) {
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
}

// dropConn clears the current connection and fails pending requests.
func (c *Client) dropConn() {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	pending := c.pending
	c.pending = make(map[string]chan Response)
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	for _, ch := range pending {
		close(ch)
	}
}

func (c *Client) supervise() {
	defer close(c.doneCh)
	defer close(c.events)

	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		c.readLoop() // returns when connection drops
		c.dropConn()

		select {
		case <-c.closeCh:
			return
		default:
		}

		c.logger.Printf("disconnected; reconnecting in %s", backoff)
		select {
		case <-time.After(backoff):
		case <-c.closeCh:
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := c.dialer.DialContext(ctx, "tcp", c.addr)
		cancel()
		if err != nil {
			c.logger.Printf("reconnect failed: %v", err)
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			continue
		}
		c.logger.Printf("reconnected to %s", c.addr)
		c.setConn(conn)
		backoff = time.Second
	}
}

func (c *Client) readLoop() {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return
	}
	br := bufio.NewReader(conn)
	for {
		buf, err := readNetstring(br)
		if err != nil {
			return
		}
		c.dispatch(buf)
	}
}

func (c *Client) dispatch(buf []byte) {
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
			// Best-effort: drop if the consumer is slow.
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
