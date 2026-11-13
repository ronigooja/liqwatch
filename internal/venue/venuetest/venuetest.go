// Package venuetest provides fixtures and a scriptable WebSocket server for venue tests.
//
// Example: s := venuetest.NewServer(t, func(c *venuetest.Conn) { c.Send(websocket.MessageText, []byte(`{"ok":true}`)) }); defer s.Close().
package venuetest

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Frame is a client frame received by a connection.
type Frame struct {
	Type    websocket.MessageType
	Payload []byte
	At      time.Time
}

// Conn is one scripted server-side connection.
type Conn struct {
	ws     *websocket.Conn
	ctx    context.Context
	Frames chan Frame
}

// Send sends a data frame.
func (c *Conn) Send(typ websocket.MessageType, payload []byte) error {
	return c.ws.Write(c.ctx, typ, payload)
}

// Ping sends a protocol ping and waits for its pong.
func (c *Conn) Ping(ctx context.Context) error { return c.ws.Ping(ctx) }

// Close closes the connection with a status.
func (c *Conn) Close(code websocket.StatusCode, reason string) error { return c.ws.Close(code, reason) }

// Server runs a script separately for each accepted connection.
type Server struct {
	HTTP  *httptest.Server
	URL   string
	mu    sync.Mutex
	conns []*Conn
}

// NewServer starts a test server. Each script can receive client frames from Conn.Frames.
func NewServer(t testing.TB, script func(*Conn)) *Server {
	t.Helper()
	s := &Server{}
	s.HTTP = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		c := &Conn{ws: ws, ctx: r.Context(), Frames: make(chan Frame, 1024)}
		s.mu.Lock()
		s.conns = append(s.conns, c)
		s.mu.Unlock()
		go func() {
			defer close(c.Frames)
			for {
				typ, p, err := ws.Read(r.Context())
				if err != nil {
					return
				}
				select {
				case c.Frames <- Frame{typ, p, time.Now()}:
				case <-r.Context().Done():
					return
				}
			}
		}()
		script(c)
	}))
	s.URL = "ws" + strings.TrimPrefix(s.HTTP.URL, "http")
	return s
}

// Connections returns the accepted connections so far.
func (s *Server) Connections() []*Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*Conn(nil), s.conns...)
}

// Close stops the server.
func (s *Server) Close() { s.HTTP.Close() }

// Fixture reads one frame per line; blank lines are retained.
func Fixture(t testing.TB, path string) [][]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 16<<20)
	var frames [][]byte
	for scanner.Scan() {
		frames = append(frames, append([]byte(nil), scanner.Bytes()...))
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return frames
}
