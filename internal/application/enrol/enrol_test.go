package enrol_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sparkflow-space/sparkflow-agentd/internal/application/enrol"
	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/host"
)

// ---- fakes ----

type idp struct {
	sub, email string
	exchanged  []string // code|redirect
}

func (i *idp) Discover(context.Context, string) (string, string, error) {
	return "https://idp/authorize", "https://idp/token", nil
}
func (i *idp) NewAttempt() (host.Attempt, error) {
	return host.Attempt{Verifier: "V", Challenge: "C", State: "S"}, nil
}
func (i *idp) AuthorizeURL(ep, client, redirect string, a host.Attempt) string {
	return ep + "?client_id=" + client + "&redirect_uri=" + redirect + "&state=" + a.State
}
func (i *idp) Exchange(_ context.Context, _, _, code, verifier, redirect string) (host.SignIn, error) {
	if verifier != "V" {
		return host.SignIn{}, errors.New("wrong verifier")
	}
	i.exchanged = append(i.exchanged, code+"|"+redirect)
	return host.SignIn{AccessToken: "AT", RefreshToken: "RT", Sub: i.sub, Email: i.email, ExpiresAt: time.Now().Add(time.Hour)}, nil
}
func (i *idp) ParsePasted(p, state string) (string, error) {
	p = strings.TrimSpace(p)
	if strings.HasPrefix(p, "bad") {
		return "", errors.New("not a code")
	}
	return strings.TrimPrefix(p, "http://127.0.0.1:4242/?code="), nil
}

type redirect struct {
	code   chan string
	closed bool
}

func (r *redirect) URI() string { return "http://127.0.0.1:4242/" }
func (r *redirect) Wait(ctx context.Context) (string, error) {
	select {
	case c := <-r.code:
		return c, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}
func (r *redirect) Close() { r.closed = true }

type redirects struct{ r *redirect }

func (r redirects) Start(string) (host.Redirect, error) { return r.r, nil }

// prompter scripts the terminal. A scripted answer is given only once its
// question has been asked — like a person, who types after reading. Pasted
// lines (no question) come from `lines`.
type prompter struct {
	mu      sync.Mutex
	said    []string
	lines   chan string
	answers map[string]string // question prefix → answer
	used    map[string]bool
	eof     bool
}

func (p *prompter) Say(s string) { p.mu.Lock(); p.said = append(p.said, s); p.mu.Unlock() }
func (p *prompter) Line(ctx context.Context) (string, error) {
	if p.eof {
		return "", io.EOF
	}
	for {
		p.mu.Lock()
		if n := len(p.said); n > 0 {
			for q, a := range p.answers {
				if strings.HasPrefix(p.said[n-1], q) && !p.used[q] {
					p.used[q] = true
					p.mu.Unlock()
					return a, nil
				}
			}
		}
		p.mu.Unlock()
		select {
		case l := <-p.lines:
			return l, nil
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
}
func (p *prompter) all() string { p.mu.Lock(); defer p.mu.Unlock(); return strings.Join(p.said, "\n") }

type browser struct{ opened []string }

func (b *browser) Open(u string) error { b.opened = append(b.opened, u); return nil }

type machine struct{}

func (machine) OSName() string  { return "Ubuntu" }
func (machine) Country() string { return "Germany" }
func (machine) Arch() string    { return "amd64" }
func (machine) Kinds() []string { return []string{"claude", "qwen"} }

type registrar struct {
	got []host.Enrolment
	tok string
}

func (r *registrar) Register(_ context.Context, _ host.Deployment, tok string, e host.Enrolment) (string, string, error) {
	r.got = append(r.got, e)
	r.tok = tok
	id := e.HostID
	if id == "" {
		id = "h-new"
	}
	return id, e.Name, nil
}

type store struct{ c *host.Credentials }

func (s *store) Load() (*host.Credentials, error) { return s.c, nil }
func (s *store) Save(c host.Credentials) error    { s.c = &c; return nil }
func (s *store) Where() string                    { return "~/.config/sparkflow-agentd/credentials.json" }

type service struct {
	installed bool
	fail      bool
	active    bool
	restarts  int
}

func (s *service) Active(context.Context) bool   { return s.active }
func (s *service) Restart(context.Context) error { s.restarts++; return nil }

func (s *service) Install(context.Context) (host.ServiceResult, error) {
	if s.fail {
		return host.ServiceResult{}, errors.New("no systemd")
	}
	s.installed = true
	return host.ServiceResult{Path: "/u/.config/systemd/user/sparkflow-agentd.service", Started: true, Notes: []string{"linger hint"}}, nil
}
func (s *service) Commands() []string {
	return []string{"systemctl --user enable --now sparkflow-agentd.service"}
}

type world struct {
	d   enrol.Deps
	idp *idp
	rd  *redirect
	p   *prompter
	br  *browser
	reg *registrar
	st  *store
	svc *service
}

func newWorld() *world {
	w := &world{
		idp: &idp{sub: "sub-A", email: "a@x"}, rd: &redirect{code: make(chan string, 1)},
		p: &prompter{lines: make(chan string, 8), answers: map[string]string{}, used: map[string]bool{}}, br: &browser{}, reg: &registrar{}, st: &store{}, svc: &service{},
	}
	w.d = enrol.Deps{IdP: w.idp, Redirects: redirects{w.rd}, Prompter: w.p, Browser: w.br, Machine: machine{}, Registrar: w.reg, Store: w.st, Service: w.svc}
	return w
}

var dev = host.Deployment{ServerURL: "https://sparkflow.ddns.net", IssuerURL: "https://login.sparkflow.ddns.net", ClientID: "daemon-dev"}

// ---- tests ----

func TestInit_LocalBrowser_DefaultsAccepted(t *testing.T) {
	w := newWorld()
	w.rd.code <- "LOOP-CODE"
	w.p.answers["Name this host"] = ""         // accept the suggestion
	w.p.answers["Run it with the system"] = "" // default yes
	res, err := enrol.Run(context.Background(), w.d, enrol.Options{Deployment: dev, Version: "v0.2.0"})
	if err != nil {
		t.Fatal(err)
	}
	if res.HostName != "Ubuntu · Germany" || res.HostID != "h-new" || res.Service == nil || !w.svc.installed {
		t.Fatalf("result: %+v", res)
	}
	if len(w.idp.exchanged) != 1 || w.idp.exchanged[0] != "LOOP-CODE|http://127.0.0.1:4242/" {
		t.Errorf("exchange: %v", w.idp.exchanged)
	}
	if len(w.br.opened) != 1 || !w.rd.closed {
		t.Errorf("browser opened %v, loopback closed %v", w.br.opened, w.rd.closed)
	}
	e := w.reg.got[0]
	if e.HostID != "" || strings.Join(e.Kinds, ",") != "claude,qwen" || e.OS != "Ubuntu" || e.Version != "v0.2.0" {
		t.Errorf("enrolment: %+v", e)
	}
	c := w.st.c
	if c == nil || c.OwnerSub != "sub-A" || c.RefreshToken != "RT" || c.HostID != "h-new" || c.Deployment != dev || c.TokenEndpoint != "https://idp/token" {
		t.Fatalf("credentials: %+v", c)
	}
	out := w.p.all()
	if strings.Contains(out, "RT") && strings.Contains(out, "refresh") || strings.Contains(out, "\"AT\"") {
		t.Errorf("a token reached the terminal:\n%s", out)
	}
	for _, want := range []string{"Signed in as a@x", "Name this host [Ubuntu · Germany]", "Agents found on PATH: claude, qwen", "Note: linger hint"} {
		if !strings.Contains(out, want) {
			t.Errorf("terminal lacks %q:\n%s", want, out)
		}
	}
}

func TestInit_OverSSH_PastedAddressWins(t *testing.T) {
	w := newWorld()
	w.p.lines <- "bad paste"
	w.p.lines <- "http://127.0.0.1:4242/?code=PASTED"
	w.p.answers["Name this host"] = "Work laptop"
	w.p.answers["Run it with the system"] = "n"
	res, err := enrol.Run(context.Background(), w.d, enrol.Options{Deployment: dev, NoBrowser: true})
	if err != nil {
		t.Fatal(err)
	}
	if w.idp.exchanged[0] != "PASTED|http://127.0.0.1:4242/" {
		t.Errorf("the pasted code must be exchanged with the SAME redirect URI: %v", w.idp.exchanged)
	}
	if len(w.br.opened) != 0 {
		t.Error("--no-browser opened a browser")
	}
	if res.HostName != "Work laptop" || w.svc.installed || !strings.Contains(w.p.all(), "systemctl --user enable") {
		t.Errorf("declined service must print the commands: %+v\n%s", res, w.p.all())
	}
	if !strings.Contains(w.p.all(), "not a code — try again") {
		t.Error("a bad paste must be explained and retried")
	}
}

func TestInit_StdinClosed_StillWaitsForTheBrowser(t *testing.T) {
	w := newWorld()
	w.p.eof = true
	go func() { time.Sleep(20 * time.Millisecond); w.rd.code <- "LATE" }()
	if _, err := enrol.Run(context.Background(), w.d, enrol.Options{Deployment: dev, Yes: true}); err != nil {
		t.Fatal(err)
	}
	if w.idp.exchanged[0] != "LATE|http://127.0.0.1:4242/" {
		t.Errorf("exchange: %v", w.idp.exchanged)
	}
}

func TestInit_Manual_UsesTheServerCodePage(t *testing.T) {
	w := newWorld()
	w.p.lines <- "MANUAL-CODE"
	if _, err := enrol.Run(context.Background(), w.d, enrol.Options{Deployment: dev, Manual: true, Yes: true, NoService: true}); err != nil {
		t.Fatal(err)
	}
	if w.idp.exchanged[0] != "MANUAL-CODE|https://sparkflow.ddns.net/agentd/code" {
		t.Errorf("exchange: %v", w.idp.exchanged)
	}
}

func TestInit_ReRunKeepsTheHost_AnotherPersonIsRefused(t *testing.T) {
	w := newWorld()
	w.st.c = &host.Credentials{HostID: "h-old", HostName: "My box", OwnerSub: "sub-A"}
	w.rd.code <- "C"
	res, err := enrol.Run(context.Background(), w.d, enrol.Options{Deployment: dev, Yes: true, NoService: true})
	if err != nil {
		t.Fatal(err)
	}
	if w.reg.got[0].HostID != "h-old" || res.HostID != "h-old" || !res.ReEnrolled || res.HostName != "My box" {
		t.Fatalf("re-run must re-enrol the same host with its name: %+v %+v", res, w.reg.got[0])
	}

	w2 := newWorld()
	w2.idp.sub = "sub-B"
	w2.st.c = &host.Credentials{HostID: "h-old", OwnerSub: "sub-A"}
	w2.rd.code <- "C"
	if _, err := enrol.Run(context.Background(), w2.d, enrol.Options{Deployment: dev, Yes: true}); !errors.Is(err, host.ErrOtherOwner) {
		t.Fatalf("another person on an enrolled device: %v", err)
	}
	if len(w2.reg.got) != 0 || w2.st.c.OwnerSub != "sub-A" {
		t.Fatal("the refused run registered or overwrote the enrolment")
	}
}

func TestInit_NotProvisioned(t *testing.T) {
	w := newWorld()
	_, err := enrol.Run(context.Background(), w.d, enrol.Options{Deployment: host.Production})
	if !errors.Is(err, host.ErrNotProvisioned) {
		t.Fatalf("an empty client id must stop before any sign-in: %v", err)
	}
}

func TestInit_AFailedServiceInstallIsNotReportedAsInstalled(t *testing.T) {
	w := newWorld()
	w.svc.fail = true
	w.rd.code <- "C"
	res, err := enrol.Run(context.Background(), w.d, enrol.Options{Deployment: dev, Yes: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Service != nil || strings.Contains(w.p.all(), "Installed and started") || !strings.Contains(w.p.all(), "by hand") {
		t.Errorf("%+v\n%s", res, w.p.all())
	}
}

func TestInit_ReEnrolmentRestartsTheRunningService(t *testing.T) {
	w := newWorld()
	w.st.c = &host.Credentials{HostID: "h-old", OwnerSub: "sub-A"}
	w.svc.active = true
	w.rd.code <- "C"
	if _, err := enrol.Run(context.Background(), w.d, enrol.Options{Deployment: dev, Yes: true}); err != nil {
		t.Fatal(err)
	}
	if w.svc.restarts != 1 || w.svc.installed {
		t.Fatalf("a running daemon must be restarted, not reinstalled: restarts=%d installed=%v", w.svc.restarts, w.svc.installed)
	}
}
