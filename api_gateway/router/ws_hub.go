package router

import (
	"context"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var perConnMax = 200 * time.Millisecond
var perConnMin = 20 * time.Millisecond

type wsConn struct {
	Conn    *websocket.Conn
	writeMu sync.Mutex
}

type wsHub struct {
	mu      sync.Mutex
	conns   map[*wsConn]struct{}
	closing bool
}

var hubOnce sync.Once
var defaultHub *wsHub

func DefaultWSHub() *wsHub {
	hubOnce.Do(func() {
		defaultHub = NewWSHub()
	})
	return defaultHub
}

func NewWSHub() *wsHub {
	return &wsHub{
		conns: make(map[*wsConn]struct{}),
	}
}

func NewWsConn(conn *websocket.Conn) *wsConn {
	return &wsConn{Conn: conn}
}

func (h *wsHub) Add(c *wsConn) {
	if c == nil || c.Conn == nil {
		return
	}
	toClose := false
	h.mu.Lock()
	if h.closing {
		toClose = true
	} else {
		h.conns[c] = struct{}{}
	}
	h.mu.Unlock()
	if toClose {
		c.writeMu.Lock()
		_ = c.Conn.Close()
		c.writeMu.Unlock()
	}
}

func (h *wsHub) Remove(c *wsConn) {
	if c == nil {
		return
	}
	h.mu.Lock()
	delete(h.conns, c)
	h.mu.Unlock()
}

func (h *wsHub) CloseAll(ctx context.Context) (closed int) {
	h.mu.Lock()
	h.closing = true
	snapshot := make([]*wsConn, 0, len(h.conns))
	for c := range h.conns {
		snapshot = append(snapshot, c)
	}
	h.conns = make(map[*wsConn]struct{})
	h.mu.Unlock()

	dl := time.Now().Add(2 * time.Second)
	if ctxDl, ok := ctx.Deadline(); ok {
		dl = ctxDl
	}

	i := 0
gracefulClose:
	for ; i < len(snapshot); i++ {
		select {
		case <-ctx.Done():
			break gracefulClose
		default:
		}

		remaining := time.Until(dl)
		if remaining <= 0 {
			break gracefulClose
		}

		c := snapshot[i]
		if c == nil || c.Conn == nil {
			continue
		}

		connsLeft := len(snapshot) - i
		perConn := remaining / time.Duration(connsLeft)
		if perConn > perConnMax {
			perConn = perConnMax
		}
		if perConn < perConnMin {
			perConn = perConnMin
		}

		deadline := time.Now().Add(perConn)
		if deadline.After(dl) {
			deadline = dl
		}
		c.writeMu.Lock()
		_ = c.Conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(1001, "server shutdown"),
			deadline,
		)
		_ = c.Conn.Close()
		c.writeMu.Unlock()
		closed++
	}

	for ; i < len(snapshot); i++ {
		c := snapshot[i]
		if c == nil || c.Conn == nil {
			continue
		}
		c.writeMu.Lock()
		_ = c.Conn.Close()
		c.writeMu.Unlock()
		closed++
	}
	return closed
}

func CloseAllWebSockets(ctx context.Context) int {
	return DefaultWSHub().CloseAll(ctx)
}
