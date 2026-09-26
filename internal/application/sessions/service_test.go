package sessions_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sparkflow-space/sparkflow-agentd/internal/application/sessions"
	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
)

// ---- fakes: the whole use-case layer is testable without tmux installed ----

type fakeTmux struct {
	started  []string // tmux names
	argv     [][]string
	sizes    [][2]int
	sent     []string
	signals  []session.Signal
	killed   []string
	list     []string
	startErr error
}

func (f *fakeTmux) Start(_ context.Context, name, _ string, argv, _ []string, cols, rows int) error {
	if f.startErr != nil {
		return f.startErr
	}
	f.started = append(f.started, name)
	f.argv = append(f.argv, argv)
	f.sizes = append(f.sizes, [2]int{cols, rows})
	return nil
}
func (f *fakeTmux) SendKeys(_ context.Context, _, text string, _ bool) error {
	f.sent = append(f.sent, text)
	return nil
}
func (f *fakeTmux) Capture(context.Context, string, int) (string, error) { return "screen", nil }
func (f *fakeTmux) Signal(_ context.Context, _ string, s session.Signal) error {
	f.signals = append(f.signals, s)
	return nil
}
func (f *fakeTmux) Kill(_ context.Context, name string) error {
	f.killed = append(f.killed, name)
	return nil
}
func (f *fakeTmux) List(context.Context) ([]string, error) { return f.list, nil }
func (f *fakeTmux) Subscribe(context.Context, string, uint64) (<-chan session.Chunk, error) {
	ch := make(chan session.Chunk)
	close(ch)
	return ch, nil
}

type auditLine struct{ actor, action, id, detail string }
type fakeAudit struct{ lines []auditLine }

func (a *fakeAudit) Record(_ context.Context, actor, action, id, detail string) {
	a.lines = append(a.lines, auditLine{actor, action, id, detail})
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

type seqIDs struct{ n int }

func (s *seqIDs) New() string { s.n++; return string(rune('a' + s.n - 1)) }

func newSvc(t *testing.T) (*sessions.Service, *fakeTmux, *fakeAudit) {
	t.Helper()
	tm, au := &fakeTmux{}, &fakeAudit{}
	cat := session.Catalog{"claude": {"claude"}, "qwen": {"qwen", "--tui"}}
	return sessions.New(tm, au, fixedClock{time.Unix(1700000000, 0)}, &seqIDs{}, cat, "/srv/agents"), tm, au
}

// ---- the tests ----

func TestStart_RefusesAKindThatIsNotAllowListed(t *testing.T) {
	svc, tm, _ := newSvc(t)
	_, err := svc.Start(context.Background(), "boris", sessions.StartParams{Kind: "bash", Workspace: "/srv/agents/p"})
	if !errors.Is(err, session.ErrUnknownKind) {
		t.Fatalf("err = %v, want ErrUnknownKind", err)
	}
	if len(tm.started) != 0 {
		t.Fatal("nothing may be started for a kind outside the allow-list")
	}
}

func TestStart_RefusesAWorkspaceOutsideTheRoot(t *testing.T) {
	svc, tm, _ := newSvc(t)
	for _, ws := range []string{"/etc", "/srv/agents/../etc", "/srv/agents-evil", "relative"} {
		if _, err := svc.Start(context.Background(), "boris", sessions.StartParams{Kind: "claude", Workspace: ws}); err == nil {
			t.Errorf("workspace %q must be refused", ws)
		}
	}
	if len(tm.started) != 0 {
		t.Fatal("a refused workspace must not start a process")
	}
}

func TestStart_NeverPassesTheTmuxDefaultSize(t *testing.T) {
	svc, tm, _ := newSvc(t)
	if _, err := svc.Start(context.Background(), "boris", sessions.StartParams{Kind: "claude", Workspace: "/srv/agents/p"}); err != nil {
		t.Fatal(err)
	}
	if got := tm.sizes[0]; got[0] == 80 && got[1] == 24 {
		t.Fatalf("a detached session must be sized explicitly, got %v — 80x24 is exactly the bug", got)
	}
}

func TestStart_AuditsWithTheActor(t *testing.T) {
	svc, _, au := newSvc(t)
	if _, err := svc.Start(context.Background(), "boris@example.com", sessions.StartParams{Kind: "claude", Workspace: "/srv/agents/p"}); err != nil {
		t.Fatal(err)
	}
	if len(au.lines) != 1 || au.lines[0].actor != "boris@example.com" || au.lines[0].action != "start" {
		t.Fatalf("audit = %+v, want one start line naming the actor", au.lines)
	}
}

func TestSend_AuditsTheTextVerbatim(t *testing.T) {
	svc, _, au := newSvc(t)
	s, _ := svc.Start(context.Background(), "boris", sessions.StartParams{Kind: "claude", Workspace: "/srv/agents/p"})
	if err := svc.Send(context.Background(), "boris", s.ID, "rm -rf /tmp/x", true); err != nil {
		t.Fatal(err)
	}
	last := au.lines[len(au.lines)-1]
	if last.action != "send" || last.detail != "rm -rf /tmp/x" {
		t.Fatalf("what an agent was told must be readable back verbatim; got %+v", last)
	}
}

func TestSignal_KillAlsoKillsTheTmuxSessionAndIsTerminal(t *testing.T) {
	svc, tm, _ := newSvc(t)
	s, _ := svc.Start(context.Background(), "boris", sessions.StartParams{Kind: "claude", Workspace: "/srv/agents/p"})

	if err := svc.Signal(context.Background(), "boris", s.ID, session.SignalKill); err != nil {
		t.Fatal(err)
	}
	if len(tm.killed) != 1 {
		t.Fatal("KILL must leave no detached tmux session behind")
	}
	if err := svc.Send(context.Background(), "boris", s.ID, "hello", true); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("an exited session must not accept input, got %v", err)
	}
}

func TestSignal_RefusesAnUnknownSignal(t *testing.T) {
	svc, tm, _ := newSvc(t)
	s, _ := svc.Start(context.Background(), "boris", sessions.StartParams{Kind: "claude", Workspace: "/srv/agents/p"})
	if err := svc.Signal(context.Background(), "boris", s.ID, session.Signal(99)); err == nil {
		t.Fatal("an unknown signal must be refused, not defaulted — defaulting to KILL is destructive")
	}
	if len(tm.signals) != 0 {
		t.Fatal("nothing may be delivered for an unknown signal")
	}
}

func TestReconcile_AdoptsOnlyOurOwnSessions(t *testing.T) {
	svc, tm, _ := newSvc(t)
	// The dev host really does carry other people's sessions.
	tm.list = []string{"partner-claude", "partner-claude-2", session.TmuxName("zz")}

	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := svc.List()
	if len(got) != 1 || got[0].ID != "zz" {
		t.Fatalf("adopted %+v — a session the daemon did not create must never be offered over gRPC", got)
	}
}

func TestReconcile_MarksAVanishedSessionExited(t *testing.T) {
	svc, tm, _ := newSvc(t)
	s, _ := svc.Start(context.Background(), "boris", sessions.StartParams{Kind: "claude", Workspace: "/srv/agents/p"})
	tm.list = nil // the host rebooted, or somebody killed it by hand

	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, x := range svc.List() {
		if x.ID == s.ID && x.State != session.StateExited {
			t.Fatalf("state = %v, want exited — the daemon must not claim a process that is gone", x.State)
		}
	}
}

func TestStart_FailureLeavesNoPhantomSession(t *testing.T) {
	svc, tm, _ := newSvc(t)
	tm.startErr = errors.New("tmux: no server running")
	if _, err := svc.Start(context.Background(), "boris", sessions.StartParams{Kind: "claude", Workspace: "/srv/agents/p"}); err == nil {
		t.Fatal("want an error")
	} else if !strings.Contains(err.Error(), "no server running") {
		t.Errorf("the cause must survive: %v", err)
	}
	if len(svc.List()) != 0 {
		t.Fatal("a failed start must not leave a session record behind")
	}
}
