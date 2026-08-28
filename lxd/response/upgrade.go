package response

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/canonical/lxd/lxd/metrics"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
	"github.com/canonical/lxd/shared/tcp"
)

// Upgrade takes a hijacked HTTP connection and sends the HTTP 101 Switching Protocols headers for protocolName.
func Upgrade(hijackedConn net.Conn, protocolName string) error {
	// Write the status line and upgrade header by hand since w.WriteHeader() would fail after Hijack().
	sb := strings.Builder{}
	sb.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
	fmt.Fprintf(&sb, "Upgrade: %s\r\n", protocolName)
	sb.WriteString("Connection: Upgrade\r\n\r\n")

	_ = hijackedConn.SetWriteDeadline(time.Now().Add(time.Second * 5))
	n, err := hijackedConn.Write([]byte(sb.String()))
	_ = hijackedConn.SetWriteDeadline(time.Time{}) // Cancel deadline.

	if err != nil {
		return fmt.Errorf("Failed writing upgrade headers: %w", err)
	}

	if n != sb.Len() {
		return errors.New("Failed writing upgrade headers")
	}

	return nil
}

// upgradeResponse switches the client connection to another protocol and relays it to a local connection.
type upgradeResponse struct {
	conn     net.Conn
	protocol string
	cleanup  func()
}

// UpgradeResponse returns a response that upgrades the client connection to protocol and copies data between
// the client and conn until either side closes. cleanup, when not nil, runs once the relay has ended.
func UpgradeResponse(conn net.Conn, protocol string, cleanup func()) Response {
	return &upgradeResponse{conn: conn, protocol: protocol, cleanup: cleanup}
}

// String returns the response type name.
func (r *upgradeResponse) String() string {
	return r.protocol + " upgrade"
}

// Render hijacks the client connection, sends the upgrade headers and relays both directions.
func (r *upgradeResponse) Render(w http.ResponseWriter, req *http.Request) error {
	if r.cleanup != nil {
		defer r.cleanup()
	}

	defer func() { _ = r.conn.Close() }()

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return api.StatusErrorf(http.StatusInternalServerError, "Webserver does not support hijacking")
	}

	remoteConn, _, err := hijacker.Hijack()
	if err != nil {
		return api.StatusErrorf(http.StatusInternalServerError, "Failed hijacking connection: %w", err)
	}

	defer func() { _ = remoteConn.Close() }()

	// The hijacked connection can no longer carry an HTTP error response, so failures from here on are logged.
	l := logger.AddContext(logger.Ctx{
		"protocol": r.protocol,
		"local":    remoteConn.LocalAddr(),
		"remote":   remoteConn.RemoteAddr(),
	})

	remoteTCP, err := tcp.ExtractConn(remoteConn)
	if err == nil && remoteTCP != nil {
		// Apply TCP timeouts if remote connection is TCP (rather than Unix).
		err = tcp.SetTimeouts(remoteTCP, 0)
		if err != nil {
			l.Warn("Failed setting TCP timeouts on remote connection", logger.Ctx{"err": err})
			return nil
		}
	}

	err = Upgrade(remoteConn, r.protocol)
	if err != nil {
		l.Warn("Failed upgrading connection", logger.Ctx{"err": err})
		return nil
	}

	if r.protocol == "nbd" {
		// NBD is a server-speaks-first protocol, so the server greeting leaves as soon as the relay starts.
		// A client that has not yet finished reading the HTTP 101 response discards it, which breaks the
		// handshake, so give the client a moment to complete the upgrade first.
		time.Sleep(250 * time.Millisecond)
	}

	ctx, cancel := context.WithCancel(req.Context())

	// Each direction closes both connections once its copy ends, so that the other direction never stays
	// blocked in a write to a peer that has gone away.
	wg := sync.WaitGroup{}
	wg.Go(func() {
		_, err := io.Copy(remoteConn, r.conn)
		if err != nil {
			if ctx.Err() == nil {
				l.Warn("Failed copying local connection to remote connection", logger.Ctx{"err": err})
			}
		}

		cancel() // Cancel context first so the closes below don't cause a warning.
		_ = remoteConn.Close()
		_ = r.conn.Close()
	})

	_, err = io.Copy(r.conn, remoteConn)
	if err != nil {
		if ctx.Err() == nil {
			l.Warn("Failed copying remote connection to local connection", logger.Ctx{"err": err})
		}
	}

	cancel() // Cancel context first so the closes below don't cause a warning.
	_ = r.conn.Close()
	_ = remoteConn.Close()

	wg.Wait() // Wait for copy go routine to finish.

	metrics.UseMetricsCallback(req, metrics.Success)

	return nil
}
