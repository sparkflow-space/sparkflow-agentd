package sessions_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
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
	// slow makes Start take time, outside the lock: the window concurrent
	// Starts race through.
	slow time.Duration
}

func (f *fakeTmux) Start(_ context.Context, s session.Session, _, env []string) (string, error) {
	if f.slow > 0 {
		time.Sleep(f.slow)
	}
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

// fakeDirs is a filesystem: directories, symlinks, repositories.
type fakeDirs struct {
	mu        sync.Mutex
	dirs      map[string]bool
	links     map[string]string // path → where it really points
	git       map[string]bool
	worktrees []string
	removed   []string
	listing   []session.Dir
	raceLink  map[string]string // links that appear only once git runs
}

func (d *fakeDirs) IsDir(p string) bool { return d.dirs[p] }
func (d *fakeDirs) Resolve(p string) (string, error) {
	c := filepath.Clean(p)
	if real, ok := d.links[c]; ok {
		return real, nil
	}
	if !d.dirs[c] {
		return "", errors.New("no such file")
	}
	return c, nil
}
func (d *fakeDirs) IsGitRoot(p string) bool { return d.git[p] }
func (d *fakeDirs) ListDirs(string) ([]session.Dir, error) {
	return d.listing, nil
}
func (d *fakeDirs) AddWorktree(_ context.Context, repo, path, branch string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.worktrees = append(d.worktrees, repo+"|"+path+"|"+branch)
	for k, v := range d.raceLink {
		d.links[k] = v
	}
	if real, ok := d.links[filepath.Dir(filepath.Dir(path))]; ok {
		// git followed a symlinked .claude: the worktree is really there.
		d.links[path] = filepath.Join(real, "worktrees", filepath.Base(path))
		d.dirs[d.links[path]] = true
		return nil
	}
	d.dirs[path] = true
	return nil
}

func (d *fakeDirs) RemoveWorktree(_ context.Context, repo, path, branch string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.removed = append(d.removed, repo+"|"+path+"|"+branch)
	return nil
}

func newDirs() *fakeDirs {
	return &fakeDirs{
		dirs:  map[string]bool{"/home/u-evil": true, "/home/u": true, "/home/u/p1": true, "/home/u/p2": true, "/home/u/repo": true, "/etc": true},
		links: map[string]string{"/home/u/link-out": "/etc"},
		git:   map[string]bool{"/home/u/repo": true},
	}
}

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
		Workspaces: newDirs(),
		Audit:      au,
		Clock:      fixedClock{},
		IDs:        &seqIDs{},
		Catalog:    session.Catalog{"claude": {"claude"}, "qwen": {"qwen", "--tui"}},
		Env:        session.EnvPolicy{AllowPrefixes: []string{"SPARKFLOW_"}},

		Home: "/home/u",
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
		Kind: "claude", Label: "s1", Folder: "/home/u/p1",
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
		{"unknown kind", sessions.StartParams{Kind: "bash", Label: "s1", Folder: "/home/u/p1"}, session.ErrUnknownKind},
		{"escaping workspace", sessions.StartParams{Kind: "claude", Label: "s1", Folder: "/home/u/../../etc"}, session.ErrWorkspaceEscapes},
		{"sibling of the home", sessions.StartParams{Kind: "claude", Label: "s1", Folder: "/home/u-evil"}, session.ErrWorkspaceEscapes},
		{"missing workspace", sessions.StartParams{Kind: "claude", Label: "s1", Folder: "/home/u/nope"}, session.ErrWorkspaceMissing},
		{"injected env", sessions.StartParams{Kind: "claude", Label: "s1", Folder: "/home/u/p1",
			Env: []string{"LD_PRELOAD=/srv/agents/p1/x.so"}}, session.ErrEnvNotAllowed},
		{"unprefixed env", sessions.StartParams{Kind: "claude", Label: "s1", Folder: "/home/u/p1",
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
		Kind: "claude", Label: "s1", Folder: "/home/u/p1",
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
		Kind: "claude", Label: "s1", Folder: "/home/u/p1", Env: []string{"SPARKFLOW_PROJECT=p1"},
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
		Kind: "claude", Label: "s1", Folder: "/home/u/p1",
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
	if _, err := h.svc.Start(ctx, alice, sessions.StartParams{Kind: "claude", Label: "s1", Folder: "/home/u/p1"}); !errors.Is(err, session.ErrTooManySessions) {
		t.Fatalf("third session for one owner: err = %v, want ErrTooManySessions", err)
	}
	// Another person still gets one, up to the host total.
	okStart(t, h, bob)
	if _, err := h.svc.Start(ctx, bob, sessions.StartParams{Kind: "claude", Label: "s1", Folder: "/home/u/p1"}); !errors.Is(err, session.ErrTooManySessions) {
		t.Fatalf("fourth session on the host: err = %v, want ErrTooManySessions", err)
	}
	// A killed session frees its slot.
	mine := h.svc.List(ctx, alice)
	if err := h.svc.Signal(ctx, alice, mine[0].ID, session.SignalKill); err != nil {
		t.Fatal(err)
	}
	if _, err := h.svc.Start(ctx, bob, sessions.StartParams{Kind: "claude", Label: "s1", Folder: "/home/u/p1"}); err != nil {
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
		{TmuxName: "sfagent-0123456789abcdef", PaneID: "%3", Owner: alice.Subject, Kind: "claude", Workspace: "/home/u/p1"},
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
	if len(mine) != 1 || mine[0].ID != "0123456789abcdef" || mine[0].PaneID != "%3" || mine[0].Workspace != "/home/u/p1" {
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

// --- AGENTD-003: the working folder, the brief, the folder picker, events ---

func TestStart_FolderRules(t *testing.T) {
	cases := []struct {
		name   string
		folder string
		label  string
		want   error
	}{
		{"a symlink inside the home pointing out of it", "/home/u/link-out", "s1", session.ErrWorkspaceEscapes},
		{"the home itself", "/home/u", "s1", session.ErrWorkspaceEscapes},
		{"relative", "p1", "s1", session.ErrWorkspaceEscapes},
		{"empty", "", "s1", session.ErrWorkspaceEmpty},
		{"a label that is a path", "/home/u/p1", "../x", session.ErrBadLabel},
		{"a label with a slash", "/home/u/p1", "a/b", session.ErrBadLabel},
		{"a label starting with a dash", "/home/u/p1", "-x", session.ErrBadLabel},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t)
			_, err := h.svc.Start(context.Background(), alice, sessions.StartParams{Kind: "claude", Folder: c.folder, Label: c.label})
			if !errors.Is(err, c.want) {
				t.Fatalf("got %v, want %v", err, c.want)
			}
		})
	}
}

func TestStart_ASymlinkedDotClaudeCannotCarryTheAgentOut(t *testing.T) {
	d := newDirs()
	d.dirs["/home/u/evil"] = true
	d.git["/home/u/evil"] = true
	// Created AFTER the folder check would have passed: only the post-check
	// sees it.
	h := newHarness(t, func(x *sessions.Deps) { x.Workspaces = d })
	d.links["/home/u/evil/.claude"] = "/tmp/x"
	_, err := h.svc.Start(context.Background(), alice, sessions.StartParams{Kind: "claude", Folder: "/home/u/evil", Label: "l1"})
	if !errors.Is(err, session.ErrWorkspaceEscapes) {
		t.Fatalf("a .claude symlink out of the home must be refused: %v", err)
	}
	if len(h.tmux.started) != 0 {
		t.Fatal("nothing may start")
	}
	// The pre-check refused before git ran; now the race: the link appears
	// between the pre-check and git (no link at pre-check time).
	d2 := newDirs()
	d2.dirs["/home/u/evil"], d2.git["/home/u/evil"] = true, true
	h2 := newHarness(t, func(x *sessions.Deps) { x.Workspaces = d2 })
	d2.raceLink = map[string]string{"/home/u/evil/.claude": "/tmp/x"}
	_, err = h2.svc.Start(context.Background(), alice, sessions.StartParams{Kind: "claude", Folder: "/home/u/evil", Label: "l2"})
	if !errors.Is(err, session.ErrWorkspaceEscapes) || len(d2.removed) != 1 {
		t.Fatalf("a worktree that landed outside must be refused AND removed: %v removed=%v", err, d2.removed)
	}
}

func TestStart_AFailedStartRemovesItsWorktree(t *testing.T) {
	d := newDirs()
	h := newHarness(t, func(x *sessions.Deps) { x.Workspaces = d })
	h.tmux.startErr = errors.New("tmux: no server")
	if _, err := h.svc.Start(context.Background(), alice, sessions.StartParams{Kind: "claude", Folder: "/home/u/repo", Label: "l1"}); err == nil {
		t.Fatal("want the tmux error")
	}
	if len(d.removed) != 1 || d.removed[0] != "/home/u/repo|/home/u/repo/.claude/worktrees/l1|agent/l1" {
		t.Fatalf("the worktree and branch must be removed so a retry can use the label: %v", d.removed)
	}
}

func TestAdmit_ConcurrentStartsCannotExceedTheLimit(t *testing.T) {
	h := newHarness(t, func(x *sessions.Deps) { x.Limits = sessions.Limits{PerOwner: 3, Total: 10} })
	h.tmux.slow = 20 * time.Millisecond
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := h.svc.Start(context.Background(), alice, sessions.StartParams{Kind: "claude", Folder: "/home/u/p1", Label: fmt.Sprintf("c%d", i)})
			if err == nil {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if ok != 3 {
		t.Fatalf("12 concurrent Starts with a limit of 3 started %d", ok)
	}
}

func TestStart_ARepositoryGetsAFreshWorktree(t *testing.T) {
	d := newDirs()
	h := newHarness(t, func(x *sessions.Deps) { x.Workspaces = d })
	s, err := h.svc.Start(context.Background(), alice, sessions.StartParams{Kind: "claude", Folder: "/home/u/repo", Label: "abc123"})
	if err != nil {
		t.Fatal(err)
	}
	if s.Workspace != "/home/u/repo/.claude/worktrees/abc123" {
		t.Errorf("workspace = %q", s.Workspace)
	}
	if len(d.worktrees) != 1 || d.worktrees[0] != "/home/u/repo|/home/u/repo/.claude/worktrees/abc123|agent/abc123" {
		t.Errorf("worktrees: %v", d.worktrees)
	}
	// A plain folder is used as it is, no worktree.
	s2, _ := h.svc.Start(context.Background(), alice, sessions.StartParams{Kind: "claude", Folder: "/home/u/p1", Label: "def456"})
	if s2.Workspace != "/home/u/p1" || len(d.worktrees) != 1 {
		t.Errorf("plain folder: %q, worktrees %v", s2.Workspace, d.worktrees)
	}
}

type recEvents struct {
	mu sync.Mutex
	ev []string
}

func (r *recEvents) SessionState(id string, st session.State) {
	r.mu.Lock()
	r.ev = append(r.ev, id+"="+st.String())
	r.mu.Unlock()
}

func TestStart_TypesTheBriefOnceTheAgentIsUp_AndEmitsEvents(t *testing.T) {
	ev := &recEvents{}
	h := newHarness(t, func(x *sessions.Deps) {
		x.Events = ev
		x.BriefWait, x.BriefSettle = 200*time.Millisecond, time.Millisecond
	})
	s, err := h.svc.Start(context.Background(), alice, sessions.StartParams{Kind: "claude", Folder: "/home/u/p1", Label: "s1", Brief: "# TSK-1: do it"})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(strings.Join(h.tmux.sentTexts(), "|"), "# TSK-1: do it") {
		time.Sleep(5 * time.Millisecond)
	}
	if !strings.Contains(strings.Join(h.tmux.sentTexts(), "|"), "# TSK-1: do it") {
		t.Fatalf("the brief was never typed: %v", h.tmux.sentTexts())
	}
	if err := h.svc.Signal(context.Background(), alice, s.ID, session.SignalKill); err != nil {
		t.Fatal(err)
	}
	ev.mu.Lock()
	got := strings.Join(ev.ev, ",")
	ev.mu.Unlock()
	if got != s.ID+"=working,"+s.ID+"=exited" {
		t.Errorf("events: %s", got)
	}
}

func TestListDirs_InsideTheHomeOnly(t *testing.T) {
	d := newDirs()
	d.listing = []session.Dir{{Name: "repo", Git: true}, {Name: "p1"}}
	h := newHarness(t, func(x *sessions.Deps) { x.Workspaces = d })
	path, dirs, err := h.svc.ListDirs(context.Background(), alice, "")
	if err != nil || path != "/home/u" || len(dirs) != 2 || !dirs[0].Git {
		t.Fatalf("home: %q %v %v", path, dirs, err)
	}
	for _, bad := range []string{"/etc", "/home/u/link-out", "/home/u/../../etc", "relative"} {
		if _, _, err := h.svc.ListDirs(context.Background(), alice, bad); !errors.Is(err, session.ErrWorkspaceEscapes) && !errors.Is(err, session.ErrWorkspaceMissing) {
			t.Errorf("ListDirs(%q) must be refused, got %v", bad, err)
		}
	}
	if _, _, err := h.svc.ListDirs(context.Background(), session.Actor{}, ""); !errors.Is(err, session.ErrNoActor) {
		t.Errorf("no actor: %v", err)
	}
	if !h.audit.has("listdirs", sessions.OutcomeRefused) || !h.audit.has("listdirs", sessions.OutcomeOK) {
		t.Error("browsing must be audited, refusals included")
	}
}

func (f *fakeTmux) sentTexts() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.keys...)
}

func (a *fakeAudit) has(action string, o sessions.Outcome) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, e := range a.events {
		if e.Action == action && e.Outcome == o {
			return true
		}
	}
	return false
}
