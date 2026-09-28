package sessions_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sparkflow-space/sparkflow-agentd/internal/application/sessions"
	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
)

// --- fakes ---------------------------------------------------------------

type fakeTmux struct {
	mu        sync.Mutex
	started   []session.Session
	startEnv  [][]string
	keys      []string
	signalled []session.Signal
	watched   []string
	released  []string
	captured  []string

	live      []session.Live
	startErr  error
	signalErr error
	pane      string
}

func (f *fakeTmux) Start(_ context.Context, s session.Session, _, env []string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.startErr != nil {
		return "", f.startErr
	}
	f.started = append(f.started, s)
	f.startEnv = append(f.startEnv, env)
	if f.pane == "" {
		f.pane = "%0"
	}
	return f.pane, nil
}

func (f *fakeTmux) SendKeys(_ context.Context, paneID, text string, submit bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys = append(f.keys, paneID+"|"+text+"|"+boolStr(submit))
	return nil
}

func (f *fakeTmux) Capture(_ context.Context, paneID string, lines int) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.captured = append(f.captured, paneID)
	return "screen", nil
}

func (f *fakeTmux) Signal(_ context.Context, _ session.Session, sig session.Signal) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.signalErr != nil {
		return f.signalErr
	}
	f.signalled = append(f.signalled, sig)
	return nil
}

func (f *fakeTmux) List(context.Context) ([]session.Live, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.live, nil
}

func (f *fakeTmux) Watch(name, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.watched = append(f.watched, name)
	return nil
}

func (f *fakeTmux) Subscribe(context.Context, string, uint64) (<-chan session.Chunk, error) {
	ch := make(chan session.Chunk)
	close(ch)
	return ch, nil
}

func (f *fakeTmux) Release(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released = append(f.released, name)
}

func boolStr(b bool) string {
	if b {
		return "submit"
	}
	return "stage"
}

type fakeAudit struct {
	mu     sync.Mutex
	events []sessions.AuditEvent
}

func (a *fakeAudit) Record(_ context.Context, e sessions.AuditEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, e)
}

func (a *fakeAudit) last() sessions.AuditEvent {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.events) == 0 {
		return sessions.AuditEvent{}
	}
	return a.events[len(a.events)-1]
}

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.Unix(1700000000, 0).UTC() }

type seqIDs struct {
	mu sync.Mutex
	n  int
}

func (s *seqIDs) New() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	// 16 lowercase hex characters, the shape session.ValidID requires.
	return "aaaaaaaaaaaaaa" + string(rune('0'+s.n/10)) + string(rune('0'+s.n%10))
}

type dirs map[string]bool

func (d dirs) IsDir(p string) bool { return d[p] }

// --- harness -------------------------------------------------------------

type harness struct {
	svc   *sessions.Service
	tmux  *fakeTmux
	audit *fakeAudit
}

func newHarness(t *testing.T, opts ...func(*sessions.Deps)) harness {
	t.Helper()
	tm := &fakeTmux{}
	au := &fakeAudit{}
	d := sessions.Deps{
		Tmux:       tm,
		Workspaces: dirs{"/srv/agents/p1": true, "/srv/agents/p2": true},
		Audit:      au,
		Clock:      fixedClock{},
		IDs:        &seqIDs{},
		Catalog:    session.Catalog{"claude": {"claude"}, "qwen": {"qwen", "--tui"}},
		Env:        session.EnvPolicy{AllowPrefixes: []string{"SPARKFLOW_"}},

		WorkspaceRoot: "/srv/agents",
	}
	for _, o := range opts {
		o(&d)
	}
	return harness{svc: sessions.New(d), tmux: tm, audit: au}
}

var (
	alice = session.Actor{Subject: "sub-alice", Email: "alice@example.com"}
	bob   = session.Actor{Subject: "sub-bob", Email: "bob@example.com"}
)

func okStart(t *testing.T, h harness, actor session.Actor) *session.Session {
	t.Helper()
	s, err := h.svc.Start(context.Background(), actor, sessions.StartParams{
		Kind: "claude", Workspace: "/srv/agents/p1",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return s
}

// --- tests ---------------------------------------------------------------

func TestStartRecordsOwnerAndPaneAndBeginsBuffering(t *testing.T) {
	h := newHarness(t)
	s := okStart(t, h, alice)

	if s.Owner != alice.Subject {
		t.Errorf("owner = %q, want %q", s.Owner, alice.Subject)
	}
	if s.PaneID != "%0" {
		t.Errorf("pane = %q, want %%0", s.PaneID)
	}
	if !session.ValidID(s.ID) {
		t.Errorf("id %q does not have the shape reconciliation will accept", s.ID)
	}
	// Buffering must begin at Start, not at the first Subscribe: otherwise the
	// agent's banner and first question are gone before anyone connects.
	if len(h.tmux.watched) != 1 || h.tmux.watched[0] != s.TmuxName {
		t.Errorf("watched = %v, want [%s]", h.tmux.watched, s.TmuxName)
	}
	if e := h.audit.last(); e.Outcome != sessions.OutcomeOK || e.Action != "start" || e.Actor != alice.Email {
		t.Errorf("audit = %+v, want an ok start by %s", e, alice.Email)
	}
}

// Every refusal must leave a record. Before this, audit ran only after success,
// so a caller enumerating kinds or probing paths produced an empty log.
func TestRefusalsAreAudited(t *testing.T) {
	cases := []struct {
		name     string
		params   sessions.StartParams
		sentinel error
	}{
		{"unknown kind", sessions.StartParams{Kind: "bash", Workspace: "/srv/agents/p1"}, session.ErrUnknownKind},
		{"escaping workspace", sessions.StartParams{Kind: "claude", Workspace: "/srv/agents/../../etc"}, session.ErrWorkspaceEscapes},
		{"sibling of the root", sessions.StartParams{Kind: "claude", Workspace: "/srv/agents-evil"}, session.ErrWorkspaceEscapes},
		{"missing workspace", sessions.StartParams{Kind: "claude", Workspace: "/srv/agents/nope"}, session.ErrWorkspaceMissing},
		{"injected env", sessions.StartParams{Kind: "claude", Workspace: "/srv/agents/p1",
			Env: []string{"LD_PRELOAD=/srv/agents/p1/x.so"}}, session.ErrEnvNotAllowed},
		{"unprefixed env", sessions.StartParams{Kind: "claude", Workspace: "/srv/agents/p1",
			Env: []string{"FOO=bar"}}, session.ErrEnvNotAllowed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			if _, err := h.svc.Start(context.Background(), alice, tc.params); !errors.Is(err, tc.sentinel) {
				t.Fatalf("err = %v, want %v", err, tc.sentinel)
			}
			if len(h.tmux.started) != 0 {
				t.Errorf("tmux was called despite the refusal: %+v", h.tmux.started)
			}
			e := h.audit.last()
			if e.Outcome != sessions.OutcomeRefused || e.Action != "start" || e.Reason == "" {
				t.Fatalf("audit = %+v, want a refused start carrying a reason", e)
			}
			if !strings.Contains(e.Detail, "kind=") {
				t.Errorf("audit detail %q does not say what was asked for", e.Detail)
			}
		})
	}
}

// The audit line carries env NAMES and never values: a value may be a token.
func TestAuditLogsEnvNamesNotValues(t *testing.T) {
	h := newHarness(t)
	_, err := h.svc.Start(context.Background(), alice, sessions.StartParams{
		Kind: "claude", Workspace: "/srv/agents/p1",
		Env: []string{"SPARKFLOW_TOKEN=s3cr3t-value"},
	})
	if err != nil {
		t.Fatal(err)
	}
	e := h.audit.last()
	if !strings.Contains(e.Detail, "SPARKFLOW_TOKEN") {
		t.Errorf("detail %q should name the variable", e.Detail)
	}
	if strings.Contains(e.Detail, "s3cr3t-value") {
		t.Errorf("detail %q leaks the value", e.Detail)
	}
}

func TestAllowedEnvReachesTmux(t *testing.T) {
	h := newHarness(t)
	if _, err := h.svc.Start(context.Background(), alice, sessions.StartParams{
		Kind: "claude", Workspace: "/srv/agents/p1", Env: []string{"SPARKFLOW_PROJECT=p1"},
	}); err != nil {
		t.Fatal(err)
	}
	if len(h.tmux.startEnv) != 1 || len(h.tmux.startEnv[0]) != 1 || h.tmux.startEnv[0][0] != "SPARKFLOW_PROJECT=p1" {
		t.Errorf("env passed to tmux = %v", h.tmux.startEnv)
	}
}

// A session belongs to the person who started it. Anyone else gets NotFound —
// the same answer as for an id that never existed, so the RPC is not an oracle.
func TestSessionsAreScopedToTheirOwner(t *testing.T) {
	h := newHarness(t)
	s := okStart(t, h, alice)
	ctx := context.Background()

	if err := h.svc.Send(ctx, bob, s.ID, "rm -rf ~", true); !errors.Is(err, session.ErrNotFound) {
		t.Errorf("Send by a stranger: err = %v, want ErrNotFound", err)
	}
	if len(h.tmux.keys) != 0 {
		t.Errorf("keys reached tmux for a stranger: %v", h.tmux.keys)
	}
	if _, err := h.svc.Snapshot(ctx, bob, s.ID, 0); !errors.Is(err, session.ErrNotFound) {
		t.Errorf("Snapshot by a stranger: err = %v, want ErrNotFound", err)
	}
	if _, err := h.svc.Subscribe(ctx, bob, s.ID, 0); !errors.Is(err, session.ErrNotFound) {
		t.Errorf("Subscribe by a stranger: err = %v, want ErrNotFound", err)
	}
	if err := h.svc.Signal(ctx, bob, s.ID, session.SignalKill); !errors.Is(err, session.ErrNotFound) {
		t.Errorf("Signal by a stranger: err = %v, want ErrNotFound", err)
	}
	if got := h.svc.List(ctx, bob); len(got) != 0 {
		t.Errorf("List for a stranger returned %d sessions", len(got))
	}
	if got := h.svc.List(ctx, alice); len(got) != 1 || got[0].ID != s.ID {
		t.Errorf("List for the owner returned %v", got)
	}
	// And a refused attempt on somebody else's session is in the log.
	if e := h.audit.last(); e.Outcome != sessions.OutcomeOK && e.Reason == "" {
		t.Errorf("audit = %+v", e)
	}
}

func TestStartRefusesTheZeroActor(t *testing.T) {
	h := newHarness(t)
	_, err := h.svc.Start(context.Background(), session.Actor{}, sessions.StartParams{
		Kind: "claude", Workspace: "/srv/agents/p1",
	})
	if !errors.Is(err, session.ErrNoActor) {
		t.Fatalf("err = %v, want ErrNoActor", err)
	}
}

func TestLimits(t *testing.T) {
	h := newHarness(t, func(d *sessions.Deps) {
		d.Limits = sessions.Limits{PerOwner: 2, Total: 3}
	})
	ctx := context.Background()
	okStart(t, h, alice)
	okStart(t, h, alice)
	if _, err := h.svc.Start(ctx, alice, sessions.StartParams{Kind: "claude", Workspace: "/srv/agents/p1"}); !errors.Is(err, session.ErrTooManySessions) {
		t.Fatalf("third session for one owner: err = %v, want ErrTooManySessions", err)
	}
	// Another person still gets one, up to the host total.
	okStart(t, h, bob)
	if _, err := h.svc.Start(ctx, bob, sessions.StartParams{Kind: "claude", Workspace: "/srv/agents/p1"}); !errors.Is(err, session.ErrTooManySessions) {
		t.Fatalf("fourth session on the host: err = %v, want ErrTooManySessions", err)
	}
	// A killed session frees its slot.
	mine := h.svc.List(ctx, alice)
	if err := h.svc.Signal(ctx, alice, mine[0].ID, session.SignalKill); err != nil {
		t.Fatal(err)
	}
	if _, err := h.svc.Start(ctx, bob, sessions.StartParams{Kind: "claude", Workspace: "/srv/agents/p1"}); err != nil {
		t.Fatalf("after a kill freed a slot: %v", err)
	}
}

func TestKillMarksExitedAndReleasesTheBuffer(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	s := okStart(t, h, alice)

	if err := h.svc.Signal(ctx, alice, s.ID, session.SignalKill); err != nil {
		t.Fatalf("Signal KILL: %v", err)
	}
	if len(h.tmux.signalled) != 1 || h.tmux.signalled[0] != session.SignalKill {
		t.Errorf("signals = %v, want one KILL (the adapter owns idempotence)", h.tmux.signalled)
	}
	if len(h.tmux.released) != 1 || h.tmux.released[0] != s.TmuxName {
		t.Errorf("released = %v, want [%s]", h.tmux.released, s.TmuxName)
	}
	// The row must be Exited, and a second Stop must not report Internal — the
	// old code returned the tmux error and left the session looking alive.
	if err := h.svc.Signal(ctx, alice, s.ID, session.SignalKill); !errors.Is(err, session.ErrNotFound) {
		t.Errorf("second KILL: err = %v, want ErrNotFound (already exited)", err)
	}
}

func TestUnknownSignalIsRefusedAndAudited(t *testing.T) {
	h := newHarness(t)
	s := okStart(t, h, alice)
	err := h.svc.Signal(context.Background(), alice, s.ID, session.Signal(9))
	if !errors.Is(err, session.ErrUnknownSignal) {
		t.Fatalf("err = %v, want ErrUnknownSignal", err)
	}
	if e := h.audit.last(); e.Outcome != sessions.OutcomeRefused {
		t.Errorf("audit = %+v, want refused", e)
	}
	if len(h.tmux.signalled) != 0 {
		t.Errorf("an unknown signal reached tmux: %v", h.tmux.signalled)
	}
}

func TestSendAuditsTheTextAndAddressesThePane(t *testing.T) {
	h := newHarness(t)
	s := okStart(t, h, alice)
	if err := h.svc.Send(context.Background(), alice, s.ID, "review the diff", true); err != nil {
		t.Fatal(err)
	}
	if len(h.tmux.keys) != 1 || !strings.HasPrefix(h.tmux.keys[0], "%0|") {
		t.Errorf("keys = %v, want the pane id as the target", h.tmux.keys)
	}
	if e := h.audit.last(); !strings.Contains(e.Detail, "review the diff") {
		t.Errorf("audit detail %q should carry what the agent was told", e.Detail)
	}
}

func TestReconcileAdoptsOnlyWellFormedNamesAndKeepsOwnership(t *testing.T) {
	h := newHarness(t)
	h.tmux.live = []session.Live{
		{TmuxName: "sfagent-0123456789abcdef", PaneID: "%3", Owner: alice.Subject, Kind: "claude", Workspace: "/srv/agents/p1"},
		{TmuxName: "sfagent-a", PaneID: "%4", Owner: bob.Subject},                 // too short: a prefix target would hit a real session
		{TmuxName: "sfagent-0123456789abcdefX", PaneID: "%5", Owner: bob.Subject}, // too long
		{TmuxName: "someone-elses-work", PaneID: "%6"},                            // not ours
		{TmuxName: "sfagent-fedcba9876543210", PaneID: "%7", Kind: "qwen"},        // ours, but unlabelled owner
	}
	ctx := context.Background()
	if err := h.svc.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}

	mine := h.svc.List(ctx, alice)
	if len(mine) != 1 || mine[0].ID != "0123456789abcdef" || mine[0].PaneID != "%3" || mine[0].Workspace != "/srv/agents/p1" {
		t.Fatalf("alice's adopted sessions = %+v", mine)
	}
	if got := h.svc.List(ctx, bob); len(got) != 0 {
		t.Errorf("bob sees %d sessions; the short/long names must not be adopted", len(got))
	}
	// The unlabelled one belongs to nobody — not to everybody.
	if err := h.svc.Send(ctx, alice, "fedcba9876543210", "hello", true); !errors.Is(err, session.ErrNotFound) {
		t.Errorf("an unattributed session answered to alice: %v", err)
	}
	// ...and the operator is told it is there.
	var told bool
	for _, e := range h.audit.events {
		if e.Action == "reconcile" && strings.Contains(e.Detail, "unattributed=1") {
			told = true
		}
	}
	if !told {
		t.Errorf("no audit line names the unattributed session: %+v", h.audit.events)
	}
}

func TestReconcileRetiresVanishedSessions(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	s := okStart(t, h, alice)

	h.tmux.live = nil // it died while nobody was looking
	if err := h.svc.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if got := h.svc.List(ctx, alice); len(got) != 1 || got[0].State != session.StateExited {
		t.Fatalf("after reconcile: %+v", got)
	}
	if err := h.svc.Send(ctx, alice, s.ID, "x", true); !errors.Is(err, session.ErrNotFound) {
		t.Errorf("Send to a dead session: %v", err)
	}
	if len(h.tmux.released) != 1 {
		t.Errorf("released = %v, want the dead session's buffer dropped", h.tmux.released)
	}
}
