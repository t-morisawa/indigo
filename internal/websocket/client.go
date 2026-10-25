package websocket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/internal/ticker"
	"github.com/gorilla/websocket"
)

// errConnClosed is reported when the connection is closed outside of the
// iteration, for example by a concurrent call to Close.
var errConnClosed = errors.New("websocket client: connection closed")

const (
	// TextMessage represents a text data message. The text message payload is
	// interpreted as UTF-8 encoded text data.
	TextMessage = websocket.TextMessage

	// BinaryMessage represents a binary data message.
	BinaryMessage = websocket.BinaryMessage
)

// Message is a single message received from the server. The Body reader is
// only valid for the current iteration step: consumers must read it before
// the loop advances to the next message.
type Message struct {
	Type int       // TextMessage or BinaryMessage
	Body io.Reader // valid only for the current iteration
}

type Client struct {
	ctx       context.Context
	logger    *slog.Logger
	dialURL   func() string
	userAgent string

	err error

	state func(*Client)

	dialer          websocket.Dialer
	connMu          sync.RWMutex
	conn            *websocket.Conn
	pingCancel      context.CancelFunc
	backoffStrategy backoffStrategy

	pending struct {
		typ int
		r   io.Reader
	}
}

// NewClient returns a client that connects to the URL returned by dialURL.
// dialURL is evaluated before every dial, so a reconnect can pick up a
// changed URL, for example one carrying an updated cursor.
func NewClient(ctx context.Context, logger *slog.Logger, dialURL func() string, userAgent string) *Client {
	return &Client{
		ctx:       ctx,
		logger:    logger.With("component", "websocket_client"),
		dialURL:   dialURL,
		userAgent: userAgent,
		dialer: websocket.Dialer{
			Proxy:            websocket.DefaultDialer.Proxy,
			HandshakeTimeout: 10 * time.Second,
		},
		backoffStrategy: backoffStrategy{
			initial:    1 * time.Second,
			multiplier: 1.5,
			max:        30 * time.Second,
		},
		state: (*Client).connectState,
	}
}

// Seq returns an iterator over the messages received from the server.
//
// Transient disconnections (close errors, i/o timeouts, failed dials) are
// handled transparently: the client reconnects with backoff and keeps
// yielding messages; reconnections are visible only as log lines. The
// iteration terminates by yielding one final (Message, err) pair, where err
// is non-nil, when:
//
//   - the context is done: err is the context error, or
//   - a non-reconnectable error occurs: err is that error.
//
// Breaking out of the loop closes the current connection and stops the
// iteration; a subsequent call to Seq starts a fresh iteration.
//
// The ctx argument should be the context handed to NewClient: it drives
// dialing, backoff, and the ping/pong machinery as well as the iteration.
func (c *Client) Seq(ctx context.Context) iter.Seq2[Message, error] {
	return func(yield func(Message, error) bool) {
		defer c.cleanup()

		for {
			if err := ctx.Err(); err != nil {
				// terminal: context done
				yield(Message{}, err)
				return
			}
			if !c.next() {
				// terminal: fatal error, or the context is done (see
				// connectState); prefer the context error over any
				// derivative network error
				err := c.err
				if ctxErr := ctx.Err(); ctxErr != nil {
					err = ctxErr
				}
				yield(Message{}, err)
				return
			}
			if !yield(c.message(), nil) {
				// consumer break: close the connection, do not reconnect
				return
			}
		}
	}
}

// Next returns true if there is more data to read.
// If Next returns false, check the Err method for any error encountered.
func (c *Client) Next() bool { return c.next() }

func (c *Client) next() bool {
	for c.err == nil && c.pending.typ == 0 {
		c.state(c)
	}
	return c.err == nil
}

// Err returns any error encountered during reading.
func (c *Client) Err() error {
	return c.err
}

// Message returns the current message value, it is analogous to gorilla/websocket.Conn.NextReader.
// Calling Message consumes the current message, so subsequent calls to Message will return nil
func (c *Client) Message() (int, io.Reader) {
	return c.message().Type, c.message().Body
}

// message returns the current message and resets the message buffer so that
// next() knows to read another one.
func (c *Client) message() Message {
	msg := Message{Type: c.pending.typ, Body: c.pending.r}
	c.pending.typ = 0
	c.pending.r = nil
	return msg
}

// Close closes any resources held by the client. It is safe to call Close
// multiple times, and safe to call after an iteration has finished; a running
// iteration sees the closed connection as a fatal error.
func (c *Client) Close() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.pingCancel != nil {
		c.pingCancel()
		c.pingCancel = nil
	}
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

// cleanup releases the connection held by a finished iteration and resets the
// state machine so the client can be iterated again.
func (c *Client) cleanup() {
	if err := c.Close(); err != nil {
		c.logger.Warn("error closing connection", "error", err)
	}
	c.err = nil
	c.state = (*Client).connectState
	c.backoffStrategy.Reset()
}

func (c *Client) setConn(conn *websocket.Conn, pingCancel context.CancelFunc) {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	c.conn = conn
	c.pingCancel = pingCancel
}

// currentConn returns the current connection, or nil if there is none.
func (c *Client) currentConn() *websocket.Conn {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn
}

func (c *Client) connectState() {
	attempts := c.backoffStrategy.attempts
	d := c.backoffStrategy.Next()
	if attempts > 0 {
		c.logger.Info("reconnecting", "delay", d, "attempt", attempts)
	}
	if err := sleep(c.ctx, d); err != nil {
		// the context is done while backing off: terminal
		c.err = err
		return
	}
	url := c.dialURL()
	c.logger.Info("connecting", "attempt", attempts, "url", url)
	conn, _, err := c.dialer.DialContext(c.ctx, url, http.Header{
		"User-Agent": []string{c.userAgent},
	})
	if err != nil {
		c.logger.Warn("connection error", "error", err)
		if ctxErr := c.ctx.Err(); ctxErr != nil {
			// the dial failed because the client context is done; keep the
			// error so the iteration terminates instead of retrying forever
			c.err = ctxErr
		}
		// otherwise the failure is transient: retry with backoff
		return
	}
	c.logger.Info("connected", "attempt", attempts)

	// set a +1 minute initial read deadline, it will be extended by pings/pongs
	if err := conn.SetReadDeadline(time.Now().Add(time.Minute)); err != nil {
		c.err = fmt.Errorf("failed to set read deadline: %w", err)
		conn.Close()
		return
	}

	// The pinger runs on a per-connection context so it stops when the
	// connection is lost; deriving it from the client context instead would
	// accumulate one pinger goroutine per (re)connection, all writing to the
	// same conn.
	//
	// TODO(dfc) sending pings periodically seems to disarm the read deadline.
	// Not sure why. The broader ping/pong design is an open question: see the
	// original websocket-iterator branch notes.
	pctx, pingCancel := context.WithCancel(c.ctx)
	go ticker.Periodically(pctx, 30*time.Second, func(ctx context.Context) error {
		// TODO(dfc) this is a data race, the whole send a ping without respect to the conn being used elsewhere
		// needs to be revisited
		conn := c.currentConn()
		if conn == nil {
			return nil
		}
		c.logger.Info("sending ping")
		return conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second))
	})
	// pong handler moves the read deadline forward
	conn.SetPongHandler(func(appData string) error {
		c.logger.Info("received pong")
		return conn.SetReadDeadline(time.Now().Add(time.Minute))
	})

	// setup ping handler for server -> client pings
	conn.SetPingHandler(func(appData string) error {
		c.logger.Info("received ping, sending pong")
		c.err = conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(10*time.Second))
		return c.err
	})

	c.setConn(conn, pingCancel)

	c.backoffStrategy.Reset()
	c.state = (*Client).readState
}

func (c *Client) readState() {
	conn := c.currentConn()
	if conn == nil {
		// the connection was closed out from under us (see Close); treat it
		// as fatal rather than something to reconnect from
		c.err = errConnClosed
		return
	}
	c.pending.typ, c.pending.r, c.err = conn.NextReader()
	if c.err != nil {
		c.logger.Error("websocket read error", "error", c.err, "debug", fmt.Sprintf("%T %+v", c.err, c.err))
		if shouldReconnect(c.err) {
			c.logger.Error("websocket connection closed, will reconnect", "error", c.err)
			// closes the connection and stops its pinger
			c.Close()
			c.err = nil
			c.state = (*Client).connectState
		}
		return
	}
}

func shouldReconnect(err error) bool {
	if cerr := new(websocket.CloseError); errors.As(err, &cerr) {
		// other end hung up, try to reconnect
		return true
	}
	// TODO(dfc) gross hack because gorilla/websocket doesn't export its netError type
	if strings.HasSuffix(err.Error(), "i/o timeout") {
		return true
	}
	return false
}

type backoffStrategy struct {
	initial    time.Duration
	multiplier float64
	max        time.Duration
	attempts   int
}

func (b *backoffStrategy) Next() time.Duration {
	// first time, no wait
	if b.attempts == 0 {
		b.attempts++
		return 0
	}

	d := time.Duration(float64(b.initial) * pow(b.multiplier, float64(b.attempts)))
	if d > b.max {
		d = b.max
	}

	b.attempts++
	return d
}

func (b *backoffStrategy) Reset() {
	b.attempts = 0
}

func pow(a, b float64) float64 {
	result := 1.0
	for i := 0; i < int(b); i++ {
		result *= a
	}
	return result
}

// sleep sleeps for the given duration or until the context is done.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
