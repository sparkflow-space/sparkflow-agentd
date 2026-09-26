// Command sparkflow-agentd owns CLI-agent sessions on a host.
//
//	sparkflow-agentd -config /etc/sparkflow-agentd/config.json
//
// See the README for what it is and the security notes for what it refuses.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/sparkflow-space/sparkflow-agentd/internal/application/sessions"
	"github.com/sparkflow-space/sparkflow-agentd/internal/core"
	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
	agentdv1 "github.com/sparkflow-space/sparkflow-agentd/internal/generated/agentd/v1"
	handler "github.com/sparkflow-space/sparkflow-agentd/internal/handler/grpc"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/tmux"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/tokenauth"
)

func main() {
	cfgPath := flag.String("config", "", "path to config.json")
	flag.Parse()

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

	// Auth is built BEFORE the listener opens: a daemon that cannot verify
	// tokens must not accept a connection at all, and there is no degraded mode
	// — the degraded mode here is "run anybody's code".
	verifier, err := tokenauth.New(ctx, cfg.Auth.JWKSURL, cfg.Auth.Issuer, cfg.Auth.Audience,
		func(url string, err error) {
			log.Printf("warning: refreshing the JWKS at %s failed: %v; token verification may start failing", url, err)
		})
	if err != nil {
		return err
	}

	cli := tmux.NewCLI(cfg.TmuxBin)
	control := tmux.NewControl(cfg.TmuxBin, cfg.StreamBuffer)
	defer control.Close()

	svc := sessions.New(
		combined{CLI: cli, Control: control},
		auditLog{}, realClock{}, randomIDs{},
		session.Catalog(cfg.Kinds), cfg.WorkspaceRoot,
	)

	// tmux outlives this process, so the first thing we do is find out what is
	// actually running rather than assume we start from nothing.
	if err := svc.Reconcile(ctx); err != nil {
		log.Printf("warning: reconciling existing tmux sessions failed: %v", err)
	}

	unary, stream := handler.Interceptors(verifier, refusalLog{})
	srv := grpc.NewServer(grpc.UnaryInterceptor(unary), grpc.StreamInterceptor(stream))
	agentdv1.RegisterAgentDaemonServer(srv, handler.NewServer(svc))

	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.Listen, err)
	}
	log.Printf("sparkflow-agentd listening on %s; kinds: %v; workspace root: %s",
		cfg.Listen, session.Catalog(cfg.Kinds).Kinds(), cfg.WorkspaceRoot)

	go func() {
		<-ctx.Done()
		// GracefulStop lets in-flight streams finish; the tmux sessions
		// themselves are deliberately left alone and re-adopted next boot.
		srv.GracefulStop()
	}()
	return srv.Serve(lis)
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
		// name, and a guessable one lets a caller aim at somebody else's
		// session by id. Failing here is correct.
		log.Fatalf("sparkflow-agentd: cannot read random bytes: %v", err)
	}
	return hex.EncodeToString(b[:])
}

type auditLog struct{}

func (auditLog) Record(_ context.Context, actor, action, sessionID, detail string) {
	// One line per command, on stdout, where journald picks it up. A CLI agent
	// can delete a repository; this is how "who told it to" is answerable.
	fmt.Fprintf(os.Stdout, "audit actor=%q action=%s session=%s detail=%q\n",
		actor, action, sessionID, detail)
}

type refusalLog struct{}

func (refusalLog) Refused(reason string, err error) {
	log.Printf("refused: %s (%v)", reason, err)
}
