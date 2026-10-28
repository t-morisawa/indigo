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

	dialer          websocket.Dialer
	connMu          sync.RWMutex
	conn            *websocket.Conn
	pingCancel      context.CancelFunc
	backoffStrategy backoffStrategy
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
// iteration; a subsequent call to Seq starts a fresh iteration. Only one
// iteration may use a Client at a time.
//
// The ctx argument should be the context handed to NewClient: it drives
// dialing, backoff, and the ping/pong machinery as well as the iteration.
func (c *Client) Seq(ctx context.Context) iter.Seq2[Message, error] {
	return func(yield func(Message, error) bool) {
		defer c.cleanup()
		stopWatchdog := c.startWatchdog(ctx)
		defer stopWatchdog()

		for {
			if err := ctx.Err(); err != nil {
				// terminal: context done
				yield(Message{}, err)
				return
			}

			conn, err := c.connectWithRetry()
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					err = ctxErr
				}
				yield(Message{}, err)
				return
			}

			for {
				if err := ctx.Err(); err != nil {
					yield(Message{}, err)
					return
				}

				typ, body, err := conn.NextReader()
				if err == nil {
					// yield runs the consumer synchronously. Only after it
					// returns true do we advance the websocket reader.
					if !yield(Message{Type: typ, Body: body}, nil) {
						// consumer break: cleanup closes the connection
						return
					}
					continue
				}

				// Cancellation takes precedence over the timeout generated
				// by the watchdog (or another network error racing with it).
				if ctxErr := ctx.Err(); ctxErr != nil {
					yield(Message{}, ctxErr)
					return
				}

				c.logger.Error("websocket read error", "error", err, "debug", fmt.Sprintf("%T %+v", err, err))
				if shouldReconnect(err) {
					c.logger.Error("websocket connection closed, will reconnect", "error", err)
					if closeErr := c.Close(); closeErr != nil {
						c.logger.Warn("error closing connection", "error", closeErr)
					}
					break // outer loop dials a new connection
				}

				yield(Message{}, err)
				return
			}
		}
	}
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
// backoff for a subsequent iteration.
func (c *Client) cleanup() {
	if err := c.Close(); err != nil {
		c.logger.Warn("error closing connection", "error", err)
	}
	c.backoffStrategy.Reset()
}

func (c *Client) setConn(conn *websocket.Conn, pingCancel context.CancelFunc) {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	c.conn = conn
	c.pingCancel = pingCancel
}

// startWatchdog starts a single goroutine that waits for ctx cancellation
// and, when it fires, unblocks a pending read on the current connection by
// setting its read deadline in the past. The returned stop function must be
// called when the iteration ends; it stops the goroutine so it does not leak
// for the lifetime of ctx.
//
// The deadline-past error surfaced by the interrupted read is classified as
// an i/o timeout. Seq checks ctx immediately after the read returns, before
// considering that reconnect path, and terminates with the context error
// instead of mistaking cancellation for a network failure.
//
// NOTE: net.Conn documents SetReadDeadline as safe to call concurrently with
// Read, but gorilla's websocket.Conn is a wrapper around a net.Conn with its
// own state on top; this concurrency pattern needs review before it is used
// anywhere more serious than an experiment.
func (c *Client) startWatchdog(ctx context.Context) (stop func()) {
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
		case <-stopped:
			return
		}
		if conn := c.currentConn(); conn != nil {
			if err := conn.SetReadDeadline(time.Now()); err != nil {
				c.logger.Warn("watchdog: failed to set read deadline", "error", err)
			}
		}
	}()
	return func() { close(stopped) }
}

// currentConn returns the current connection, or nil if there is none.
func (c *Client) currentConn() *websocket.Conn {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn
}

// connectWithRetry returns a configured connection. Dial failures are
// transparent while the client context is alive; cancellation and local
// setup errors terminate the iterator.
func (c *Client) connectWithRetry() (*websocket.Conn, error) {
	for {
		attempts := c.backoffStrategy.attempts
		d := c.backoffStrategy.Next()
		if attempts > 0 {
			c.logger.Info("reconnecting", "delay", d, "attempt", attempts)
		}
		if err := sleep(c.ctx, d); err != nil {
			return nil, err
		}

		url := c.dialURL()
		c.logger.Info("connecting", "attempt", attempts, "url", url)
		conn, _, err := c.dialer.DialContext(c.ctx, url, http.Header{
			"User-Agent": []string{c.userAgent},
		})
		if err != nil {
			c.logger.Warn("connection error", "error", err)
			if ctxErr := c.ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			continue
		}
		c.logger.Info("connected", "attempt", attempts)

		if err := c.configureConn(conn); err != nil {
			conn.Close()
			return nil, err
		}
		c.backoffStrategy.Reset()
		return conn, nil
	}
}

func (c *Client) configureConn(conn *websocket.Conn) error {
	// set a +1 minute initial read deadline, it will be extended by pings/pongs
	if err := conn.SetReadDeadline(time.Now().Add(time.Minute)); err != nil {
		return fmt.Errorf("failed to set read deadline: %w", err)
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
	conn.SetPongHandler(func(appData string) error {
		c.logger.Info("received pong")
		return conn.SetReadDeadline(time.Now().Add(time.Minute))
	})
	conn.SetPingHandler(func(appData string) error {
		c.logger.Info("received ping, sending pong")
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(10*time.Second))
	})

	c.setConn(conn, pingCancel)
	go ticker.Periodically(pctx, 30*time.Second, func(ctx context.Context) error {
		c.logger.Info("sending ping")
		return conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second))
	})
	return nil
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
