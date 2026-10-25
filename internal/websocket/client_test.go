package websocket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// firehoseServer is a minimal websocket server for exercising the client: it
// upgrades every request, records the URL of each dial in order, and hands
// each connection to the test's handler.
type firehoseServer struct {
	srv      *httptest.Server
	upgrader websocket.Upgrader

	mu    sync.Mutex
	dials []string
}

func newFirehoseServer(t *testing.T, handleConn func(connNum int, conn *websocket.Conn)) *firehoseServer {
	fs := &firehoseServer{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}
	fs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := fs.upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		fs.mu.Lock()
		fs.dials = append(fs.dials, r.URL.RequestURI())
		connNum := len(fs.dials) - 1
		fs.mu.Unlock()
		handleConn(connNum, conn)
	}))
	t.Cleanup(fs.srv.Close)
	return fs
}

func (fs *firehoseServer) url() string {
	return "ws" + strings.TrimPrefix(fs.srv.URL, "http")
}

func (fs *firehoseServer) recordedDials() []string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return append([]string(nil), fs.dials...)
}

func newTestClient(ctx context.Context, t *testing.T, dialURL func() string) *Client {
	t.Helper()
	c := NewClient(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), dialURL, "test")
	// shrink the backoff so reconnects in tests are fast
	c.backoffStrategy = backoffStrategy{
		initial:    10 * time.Millisecond,
		multiplier: 1.5,
		max:        50 * time.Millisecond,
	}
	return c
}

// TestSeqTransparentReconnect checks that a consumer sees messages across
// several server-side disconnects without ever observing an error: the first
// dial and every reconnect are invisible to the range loop.
func TestSeqTransparentReconnect(t *testing.T) {
	fs := newFirehoseServer(t, func(connNum int, conn *websocket.Conn) {
		for i := 0; i < 3; i++ {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("conn-%d-msg-%d", connNum, i))); err != nil {
				return
			}
		}
		// hang up without a close handshake; the client sees an abnormal
		// closure, which is classified as reconnectable
		conn.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := newTestClient(ctx, t, func() string { return fs.url() })

	seen := make(map[string]bool)
	for msg, err := range c.Seq(ctx) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		body, err := io.ReadAll(msg.Body)
		if err != nil {
			t.Fatalf("reading message body: %v", err)
		}
		if msg.Type != TextMessage {
			t.Fatalf("unexpected message type %d", msg.Type)
		}
		seen[string(body)] = true
		if seen["conn-2-msg-0"] {
			// messages from three connections: two transparent reconnects
			break
		}
	}
	for _, marker := range []string{
		"conn-0-msg-2", "conn-1-msg-2", "conn-2-msg-0",
	} {
		if !seen[marker] {
			t.Fatalf("missing message %q; saw %v", marker, seen)
		}
	}
}

// TestSeqBreakClosesConnection checks that breaking out of the loop closes
// the underlying connection, and that a break is not reported as an error.
func TestSeqBreakClosesConnection(t *testing.T) {
	closedByServer := make(chan struct{})
	fs := newFirehoseServer(t, func(connNum int, conn *websocket.Conn) {
		defer close(closedByServer)
		for i := 0; ; i++ {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("msg-%d", i))); err != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := newTestClient(ctx, t, func() string { return fs.url() })

	n := 0
	for msg, err := range c.Seq(ctx) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, err := io.ReadAll(msg.Body); err != nil {
			t.Fatalf("reading message body: %v", err)
		}
		n++
		if n == 5 {
			break
		}
	}
	if n != 5 {
		t.Fatalf("expected 5 messages, got %d", n)
	}

	select {
	case <-closedByServer:
	case <-time.After(2 * time.Second):
		t.Fatal("server connection was not closed after break")
	}
}

// TestSeqCancelUnblocksRead checks the cancellation watchdog: with the client
// blocked in a read on a silent server, cancelling the context must end the
// iteration promptly (well before the one minute read deadline) with the
// context error.
func TestSeqCancelUnblocksRead(t *testing.T) {
	fs := newFirehoseServer(t, func(connNum int, conn *websocket.Conn) {
		// say nothing; block until the client hangs up
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := newTestClient(ctx, t, func() string { return fs.url() })

	done := make(chan error, 1)
	go func() {
		for msg, err := range c.Seq(ctx) {
			if err != nil {
				done <- err
				return
			}
			done <- fmt.Errorf("unexpected message: type %d", msg.Type)
			return
		}
		done <- nil
	}()

	time.Sleep(200 * time.Millisecond) // let the client connect and block in read
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Seq did not return within 1s of cancellation")
	}
}

// TestSeqDialFailureTerminatesOnCancel checks that failed dials are retried
// transparently while the context is alive, but that cancelling the context
// stops the retries instead of spinning forever.
func TestSeqDialFailureTerminatesOnCancel(t *testing.T) {
	// reserve a port and stop listening on it, so dials fail fast
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadURL := "ws://" + l.Addr().String()
	l.Close()

	var dials atomic.Int64
	dialURL := func() string {
		dials.Add(1)
		return deadURL
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := newTestClient(ctx, t, dialURL)

	done := make(chan error, 1)
	go func() {
		for msg, err := range c.Seq(ctx) {
			if err != nil {
				done <- err
				return
			}
			done <- fmt.Errorf("unexpected message: type %d", msg.Type)
			return
		}
		done <- nil
	}()

	time.Sleep(300 * time.Millisecond) // give the client time to retry a few dials
	if got := dials.Load(); got < 2 {
		t.Fatalf("expected transparent dial retries, saw only %d dials", got)
	}
	before := dials.Load()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Seq did not terminate after cancellation")
	}

	// no infinite retry: the dial count must settle once the iteration ended
	time.Sleep(200 * time.Millisecond)
	if got := dials.Load(); got > before+2 {
		t.Fatalf("client kept dialing after cancellation: %d dials since cancel", got-before)
	}
}

// TestSeqDialURLReevaluated checks that dialURL is called before every dial,
// so a reconnect picks up a changed URL (for example one carrying a fresh
// cursor) instead of replaying from the URL captured at startup.
func TestSeqDialURLReevaluated(t *testing.T) {
	fs := newFirehoseServer(t, func(connNum int, conn *websocket.Conn) {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("conn-%d", connNum))); err != nil {
			return
		}
		conn.Close() // hang up to force a reconnect
	})
	base := fs.url()
	var gen atomic.Int64
	dialURL := func() string {
		return fmt.Sprintf("%s/cursor/%d", base, gen.Add(1)-1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := newTestClient(ctx, t, dialURL)

	n := 0
	for msg, err := range c.Seq(ctx) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		body, err := io.ReadAll(msg.Body)
		if err != nil {
			t.Fatalf("reading message body: %v", err)
		}
		n++
		if string(body) != fmt.Sprintf("conn-%d", n-1) {
			t.Fatalf("unexpected message %q", body)
		}
		if string(body) == "conn-2" {
			break
		}
	}
	if n != 3 {
		t.Fatalf("expected 3 messages, got %d", n)
	}

	got := fs.recordedDials()
	want := []string{"/cursor/0", "/cursor/1", "/cursor/2"}
	if len(got) < len(want) {
		t.Fatalf("expected at least %d dials, got %v", len(want), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("dial %d used %q, want %q; dials: %v", i, got[i], w, got)
		}
	}
}
