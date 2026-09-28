// Command sparkflow-agentd owns CLI-agent sessions on a host.
//
//	sparkflow-agentd -config /etc/sparkflow-agentd/config.json
//
// See the README for what it is and the security notes for what it refuses.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/sparkflow-space/sparkflow-agentd/internal/application/sessions"
	"github.com/sparkflow-space/sparkflow-agentd/internal/core"
	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
	agentdv1 "github.com/sparkflow-space/sparkflow-agentd/internal/generated/agentd/v1"
	handler "github.com/sparkflow-space/sparkflow-agentd/internal/handler/grpc"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/hostfs"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/tmux"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/tokenauth"
)

// version is stamped at release time by GoReleaser (-X main.version={{.Tag}}).
//
// Without the stamp the module version recorded in the build info is used:
// `go install …@vX.Y.Z` records the tag, and a local `go build` in a checkout
// records a pseudo-version derived from the commit (measured: a build at
// 6ed5e46 reports v0.0.0-20260927105140-6ed5e469b2fd). So the binary reports
// the same version however it was obtained, and "dev" only when there is no
// build info at all.
var version = "dev"

func versionString() string {
	if version != "dev" {
		return version
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return version
}

func main() {
	cfgPath := flag.String("config", "", "path to config.json")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(versionString())
		return
	}

	if err := run(*cfgPath); err != nil {
		log.Fatalf("sparkflow-agentd: %v", err)
	}
}

func run(cfgPath string) error {
	cfg, err := core.Load(cfgPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Auth is built BEFORE the listener opens: a daemon that cannot verify tokens
	// must not accept a connection at all, and there is no degraded mode — the
	// degraded mode here is "run anybody's code".
	verifier, err := tokenauth.New(ctx, cfg.Auth.JWKSURL, cfg.Auth.Issuer, cfg.Auth.Audience,
		func(url string, err error) {
			log.Printf("warning: refreshing the JWKS at %s failed: %v; token verification may start failing", url, err)
		})
	if err != nil {
		return err
	}

	cli := tmux.NewCLI(cfg.TmuxBin)
	control := tmux.NewControl(cfg.TmuxBin, cfg.StreamBuffer, cfg.StreamBufferBytes, log.Printf)
	defer control.Close()

	svc := sessions.New(sessions.Deps{
		Tmux:          combined{CLI: cli, Control: control},
		Workspaces:    hostfs.Dirs{},
		Audit:         newAuditLog(os.Stdout),
		Clock:         realClock{},
		IDs:           randomIDs{},
		Catalog:       session.Catalog(cfg.Kinds),
		Env:           cfg.EnvPolicy(),
		WorkspaceRoot: cfg.WorkspaceRoot,
		Limits:        sessions.Limits{PerOwner: cfg.MaxSessionsPerOwner, Total: cfg.MaxSessions},
	})

	// tmux outlives this process, so the first thing we do is find out what is
	// actually running rather than assume we start from nothing.
	if err := svc.Reconcile(ctx); err != nil {
		log.Printf("warning: reconciling existing tmux sessions failed: %v", err)
	}
	// And keep doing it: a session that exits on its own — the agent finished, the
	// host ran out of memory, somebody killed it by hand — would otherwise read as
	// alive until the next restart.
	go reconcileLoop(ctx, svc, time.Duration(cfg.ReconcileSeconds)*time.Second)

	unary, stream := handler.Interceptors(verifier, refusalLog{})
	opts := []grpc.ServerOption{grpc.UnaryInterceptor(unary), grpc.StreamInterceptor(stream)}
	creds, err := serverCreds(cfg.TLS)
	if err != nil {
		return err
	}
	if creds != nil {
		opts = append(opts, grpc.Creds(creds))
	}
	srv := grpc.NewServer(opts...)
	agentdv1.RegisterAgentDaemonServer(srv, handler.NewServer(svc))

	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.Listen, err)
	}
	log.Printf("sparkflow-agentd listening on %s (%s); kinds: %v; workspace root: %s; env prefixes: %v",
		cfg.Listen, transportName(creds), session.Catalog(cfg.Kinds).Kinds(),
		cfg.WorkspaceRoot, cfg.EnvAllowPrefixes)

	go func() {
		<-ctx.Done()
		// GracefulStop lets in-flight streams finish — but it BLOCKS until they
		// do, and gRPC does not cancel a handler's context, so a single open
		// Stream would keep the process alive forever and a second SIGTERM would
		// be a no-op (NotifyContext's context is already cancelled). Hence the
		// deadline: ask nicely, then stop.
		//
		// The tmux sessions themselves are deliberately left alone and re-adopted
		// next boot.
		done := make(chan struct{})
		go func() { srv.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			log.Printf("warning: a stream did not finish in 10s; stopping anyway")
			srv.Stop()
		}
	}()
	return srv.Serve(lis)
}

// serverCreds builds mutual TLS, or returns nil when the configuration has
// explicitly allowed plaintext on loopback (core.Config refuses every other
// plaintext case).
func serverCreds(c core.TLSConfig) (credentials.TransportCredentials, error) {
	if !c.Enabled() {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("agentd: loading the server certificate: %w", err)
	}
	pem, err := os.ReadFile(c.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("agentd: reading the client CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("agentd: the client CA file %q contains no certificate", c.ClientCAFile)
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		// RequireAndVerify, not VerifyIfGiven: the caller is one known service
		// holding a certificate from a private CA, and anything else has no
		// business opening this port.
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  pool,
		MinVersion: tls.VersionTLS13,
	}), nil
}

func transportName(c credentials.TransportCredentials) string {
	if c == nil {
		return "PLAINTEXT — loopback only"
	}
	return "mutual TLS"
}

func reconcileLoop(ctx context.Context, svc *sessions.Service, every time.Duration) {
	if every <= 0 {
		every = 30 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := svc.Reconcile(ctx); err != nil {
				log.Printf("warning: reconcile failed: %v", err)
			}
		}
	}
}

// combined joins the two tmux adapters into the single port the use cases want.
type combined struct {
	*tmux.CLI
	*tmux.Control
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type randomIDs struct{}

func (randomIDs) New() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A session id must be unpredictable: it is part of the tmux session
		// name, and a guessable one lets a caller aim at somebody else's session
		// by id. Failing here is correct.
		log.Fatalf("sparkflow-agentd: cannot read random bytes: %v", err)
	}
	return hex.EncodeToString(b[:])
}

// auditLog writes one line per attempted command.
//
// The mutex and the single Write are the point: a `send` detail can be a whole
// kanban-card brief, and two concurrent fmt.Fprintf calls longer than PIPE_BUF
// interleave in journald — producing a log that is worse than a short one
// because it looks complete.
type auditLog struct {
	mu sync.Mutex
	w  interface{ Write([]byte) (int, error) }
}

func newAuditLog(w interface{ Write([]byte) (int, error) }) *auditLog {
	return &auditLog{w: w}
}

func (a *auditLog) Record(_ context.Context, e sessions.AuditEvent) {
	var b strings.Builder
	fmt.Fprintf(&b, "audit ts=%s actor=%q action=%s outcome=%s session=%s detail=%q",
		time.Now().UTC().Format(time.RFC3339), e.Actor, e.Action, e.Outcome, e.SessionID, e.Detail)
	if e.Reason != "" {
		fmt.Fprintf(&b, " reason=%q", e.Reason)
	}
	b.WriteByte('\n')
	a.mu.Lock()
	defer a.mu.Unlock()
	_, _ = a.w.Write([]byte(b.String()))
}

type refusalLog struct{}

func (refusalLog) Refused(reason string, err error) {
	log.Printf("refused: %s (%v)", reason, err)
}
