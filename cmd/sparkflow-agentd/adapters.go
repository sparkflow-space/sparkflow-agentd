package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/user"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sparkflow-space/sparkflow-agentd/internal/application/sessions"
	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/host"
	agentv1 "github.com/sparkflow-space/sparkflow-agentd/internal/generated/agent/v1"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/credfile"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/hostchannel"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/hostinfo"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/loopback"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/tmux"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/userservice"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/zitadel"
)

// The adapters from infra to the domain's ports. They live in the composition
// root because infra may not import application or handler, and the ports'
// types are the domain's.

type idpAdapter struct{ c *zitadel.Client }

func (a idpAdapter) Discover(ctx context.Context, issuer string) (string, string, error) {
	e, err := a.c.Discover(ctx, issuer)
	return e.AuthorizationEndpoint, e.TokenEndpoint, err
}
func (idpAdapter) NewAttempt() (host.Attempt, error) {
	p, err := zitadel.NewPKCE()
	return host.Attempt{Verifier: p.Verifier, Challenge: p.Challenge, State: p.State}, err
}
func (idpAdapter) AuthorizeURL(ep, client, redirect string, a host.Attempt) string {
	return zitadel.AuthorizeURL(ep, client, redirect, zitadel.PKCE{Verifier: a.Verifier, Challenge: a.Challenge, State: a.State})
}
func (a idpAdapter) Exchange(ctx context.Context, tokenEP, client, code, verifier, redirect string) (host.SignIn, error) {
	t, err := a.c.Exchange(ctx, tokenEP, client, code, verifier, redirect)
	return host.SignIn{AccessToken: t.AccessToken, RefreshToken: t.RefreshToken, ExpiresAt: t.ExpiresAt, Sub: t.Sub, Email: t.Email}, err
}
func (idpAdapter) ParsePasted(p, state string) (string, error) { return zitadel.ParsePasted(p, state) }

type redirects struct{}

func (redirects) Start(state string) (host.Redirect, error) {
	l, err := loopback.Start(state)
	if err != nil {
		return nil, err
	}
	return redirect{l}, nil
}

type redirect struct{ l *loopback.Listener }

func (r redirect) URI() string                              { return r.l.RedirectURI() }
func (r redirect) Wait(ctx context.Context) (string, error) { return r.l.Wait(ctx) }
func (r redirect) Close()                                   { r.l.Close() }

// systemBrowser opens a URL, best effort.
type systemBrowser struct{}

func (systemBrowser) Open(url string) error {
	name := "xdg-open"
	if runtime.GOOS == "darwin" {
		name = "open"
	}
	if runtime.GOOS == "linux" && os.Getenv("DISPLAY") == "" && os.Getenv("WAYLAND_DISPLAY") == "" {
		return errors.New("no display")
	}
	cmd := exec.Command(name, url)
	cmd.Stdout, cmd.Stderr = nil, nil
	return cmd.Start()
}

type machine struct {
	info  hostinfo.Info
	kinds map[string][]string
}

func (m machine) OSName() string  { return m.info.OSName() }
func (m machine) Country() string { return m.info.Country() }
func (m machine) Arch() string    { return m.info.Arch() }
func (m machine) Kinds() []string { return m.info.KindsOnPath(m.kinds) }

type registrar struct{}

func (registrar) Register(ctx context.Context, d host.Deployment, token string, e host.Enrolment) (string, string, error) {
	resp, err := hostchannel.Register(ctx, d.ServerURL, token, &agentv1.RegisterHostRequest{
		HostId: e.HostID, Name: e.Name, Os: e.OS, Arch: e.Arch, Version: e.Version, Kinds: e.Kinds,
	})
	if err != nil {
		return "", "", err
	}
	return resp.GetHostId(), resp.GetName(), nil
}

type storeAdapter struct{ s credfile.Store }

func (a storeAdapter) Load() (*host.Credentials, error) {
	c, err := a.s.Load()
	if errors.Is(err, credfile.ErrNone) {
		return nil, nil
	}
	return c, err
}
func (a storeAdapter) Save(c host.Credentials) error { return a.s.Save(c) }
func (a storeAdapter) Where() string                 { return a.s.Path }

func serviceManager() (userservice.Manager, error) {
	bin, err := os.Executable()
	if err != nil {
		return userservice.Manager{}, err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return userservice.Manager{}, err
	}
	u, err := user.Current()
	if err != nil {
		return userservice.Manager{}, err
	}
	uid, _ := strconv.Atoi(u.Uid)
	return userservice.Manager{
		GOOS: runtime.GOOS, Home: home, User: u.Username, UID: uid,
		Binary: bin, PATH: os.Getenv("PATH"), Run: userservice.ExecRunner{},
	}, nil
}

type serviceAdapter struct{ m userservice.Manager }

func (a serviceAdapter) Install(ctx context.Context) (host.ServiceResult, error) {
	r, err := a.m.Install(ctx)
	return host.ServiceResult{Path: r.UnitPath, Started: r.Started, Notes: r.Notes}, err
}
func (a serviceAdapter) Commands() []string {
	var out []string
	if text, err := a.m.Render(); err == nil {
		out = append(out, fmt.Sprintf("write this to the unit file (sparkflow-agentd service install does it for you):\n%s", indent(text)))
	}
	for _, c := range a.m.Commands() {
		out = append(out, strings.Join(c, " "))
	}
	return out
}

func (a serviceAdapter) Active(ctx context.Context) bool   { return a.m.Active(ctx) }
func (a serviceAdapter) Restart(ctx context.Context) error { return a.m.Restart(ctx) }

func indent(s string) string {
	return "    " + strings.ReplaceAll(strings.TrimRight(s, "\n"), "\n", "\n    ")
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
		// name. Failing here is correct.
		log.Fatalf("sparkflow-agentd: cannot read random bytes: %v", err)
	}
	return hex.EncodeToString(b[:])
}

// auditLog writes one line per attempted command. The mutex and the single
// Write are the point: a `send` detail can be a whole kanban-card brief, and
// two concurrent writes longer than PIPE_BUF interleave in journald.
type auditLog struct {
	mu sync.Mutex
	w  interface{ Write([]byte) (int, error) }
}

func newAuditLog(w interface{ Write([]byte) (int, error) }) *auditLog { return &auditLog{w: w} }

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
