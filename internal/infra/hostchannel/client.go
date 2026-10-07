// Package hostchannel is the daemon's gRPC client for agent.v1.HostChannel:
// TLS to the public ingress, the owner's bearer token on every stream, and
// keepalives so a dead NAT mapping is noticed. It also holds the token source
// that keeps that bearer fresh and persists every rotated refresh token.
package hostchannel

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/host"
	agentv1 "github.com/sparkflow-space/sparkflow-agentd/internal/generated/agent/v1"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/zitadel"
)

// Target turns a server URL into a gRPC target and whether it needs TLS.
// https://host → host:443 over TLS; http:// is accepted ONLY for loopback
// (a developer's local stack) — a bearer token never crosses a network in the
// clear.
func Target(serverURL string) (string, bool, error) {
	u, err := url.Parse(serverURL)
	if err != nil || u.Host == "" {
		return "", false, fmt.Errorf("server %q is not a URL", serverURL)
	}
	hostname, port := u.Hostname(), u.Port()
	switch u.Scheme {
	case "https":
		if port == "" {
			port = "443"
		}
		return net.JoinHostPort(hostname, port), true, nil
	case "http":
		ip := net.ParseIP(hostname)
		if hostname != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return "", false, fmt.Errorf("refusing plaintext to %q — only https, or http on loopback", serverURL)
		}
		if port == "" {
			port = "80"
		}
		return net.JoinHostPort(hostname, port), false, nil
	}
	return "", false, fmt.Errorf("server %q: unsupported scheme", serverURL)
}

// Tokens hands out a valid access token, refreshing when needed.
type Tokens struct {
	mu    sync.Mutex
	creds host.Credentials
	oidc  *zitadel.Client
	save  func(host.Credentials) error
	now   func() time.Time
}

// NewTokens wraps stored credentials. save persists rotations; it runs BEFORE
// a new access token is used, because a rotated refresh token that was not
// written down is a device that cannot reconnect after its next restart.
func NewTokens(c host.Credentials, oidc *zitadel.Client, save func(host.Credentials) error) *Tokens {
	return &Tokens{creds: c, oidc: oidc, save: save, now: time.Now}
}

// Access returns a token with at least a minute left.
func (t *Tokens) Access(ctx context.Context) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.creds.AccessToken != "" && t.now().Add(time.Minute).Before(t.creds.ExpiresAt) {
		return t.creds.AccessToken, nil
	}
	tok, err := t.oidc.Refresh(ctx, t.creds.TokenEndpoint, t.creds.Deployment.ClientID, t.creds.RefreshToken)
	if err != nil {
		return "", err
	}
	next := t.creds
	next.AccessToken, next.RefreshToken, next.ExpiresAt = tok.AccessToken, tok.RefreshToken, tok.ExpiresAt
	if err := t.save(next); err != nil {
		return "", fmt.Errorf("persisting the rotated sign-in: %w", err)
	}
	t.creds = next
	return next.AccessToken, nil
}

// bearer is per-RPC credentials carrying one token.
type bearer struct {
	token  string
	secure bool
}

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}
func (b bearer) RequireTransportSecurity() bool { return b.secure }

func dial(target string, secure bool) (*grpc.ClientConn, error) {
	tc := insecure.NewCredentials()
	if secure {
		tc = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	return grpc.NewClient(target,
		grpc.WithTransportCredentials(tc),
		// Pings keep an idle channel alive through NATs and the ingress, and
		// detect a dead one in about 40s. The server permits pings every 10s.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true}),
	)
}

// Dialer opens the channel. The composition root adapts Connect to the
// handler's Dial func (infra does not import the handler layer).
type Dialer struct {
	Target string
	Secure bool
	Tokens *Tokens

	mu   sync.Mutex
	conn *grpc.ClientConn
}

func (d *Dialer) Connect(ctx context.Context) (agentv1.HostChannel_ConnectClient, error) {
	tok, err := d.Tokens.Access(ctx)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	if d.conn == nil {
		if d.conn, err = dial(d.Target, d.Secure); err != nil {
			d.mu.Unlock()
			return nil, err
		}
	}
	conn := d.conn
	d.mu.Unlock()
	return agentv1.NewHostChannelClient(conn).Connect(ctx, grpc.PerRPCCredentials(bearer{token: tok, secure: d.Secure}))
}

// Close releases the connection.
func (d *Dialer) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.conn != nil {
		_ = d.conn.Close()
		d.conn = nil
	}
}

// Register calls RegisterHost with a freshly signed-in token (`init`).
func Register(ctx context.Context, serverURL, accessToken string, req *agentv1.RegisterHostRequest) (*agentv1.RegisterHostResponse, error) {
	target, secure, err := Target(serverURL)
	if err != nil {
		return nil, err
	}
	conn, err := dial(target, secure)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return agentv1.NewHostChannelClient(conn).RegisterHost(ctx, req, grpc.PerRPCCredentials(bearer{token: accessToken, secure: secure}))
}

// Permanent says whether a channel error cannot be fixed by retrying: the
// host was revoked or is unknown to the server, or the sign-in is gone. The
// service manager is told not to restart on those (exit status 3).
func Permanent(err error) bool {
	if errors.Is(err, zitadel.ErrSignInAgain) {
		return true
	}
	var se interface{ GRPCStatus() *status.Status }
	if errors.As(err, &se) {
		switch se.GRPCStatus().Code() {
		case codes.PermissionDenied, codes.NotFound:
			return true
		}
	}
	return false
}
