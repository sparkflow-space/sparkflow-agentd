package tmux_test

import (
	"context"
	"strings"
	"testing"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/tmux"
)

func sample() session.Session {
	return session.Session{
		ID: "0123456789abcdef", Owner: "sub-alice", Kind: "claude",
		Workspace: "/srv/agents/p1", TmuxName: "sfagent-0123456789abcdef",
		Cols: 160, Rows: 48,
	}
}

func TestCLIStart_LabelsTheSessionAndReturnsThePane(t *testing.T) {
	d := newFakeDir(t)
	cli := tmux.NewCLI(d.bin())

	pane, err := cli.Start(context.Background(), sample(), []string{"claude"}, []string{"SPARKFLOW_X=1"})
	if err != nil {
		t.Fatal(err)
	}
	if pane != "%0" {
		t.Errorf("pane = %q", pane)
	}
	calls := d.read("calls")
	for _, want := range []string{
		"new-session -d -s sfagent-0123456789abcdef -c /srv/agents/p1 -x 160 -y 48 -e SPARKFLOW_X=1 -- claude",
		"set-option -t sfagent-0123456789abcdef @sfagent_owner sub-alice",
		"set-option -t sfagent-0123456789abcdef @sfagent_kind claude",
		"set-option -t sfagent-0123456789abcdef @sfagent_workspace /srv/agents/p1",
		"set-option -t sfagent-0123456789abcdef @sfagent_pane %0",
	} {
		if !strings.Contains(calls, want) {
			t.Errorf("tmux was not told to %q; calls:\n%s", want, calls)
		}
	}
	if strings.Contains(calls, "kill-session") {
		t.Errorf("a successful start must not kill anything:\n%s", calls)
	}
}

// An unlabelled session would come back from reconciliation with no owner, which
// means reachable by nobody — so it is removed rather than left on the host.
func TestCLIStart_KillsTheSessionWhenLabellingFails(t *testing.T) {
	d := newFakeDir(t)
	d.write("fail", "set-option\tno such option")
	cli := tmux.NewCLI(d.bin())

	if _, err := cli.Start(context.Background(), sample(), []string{"claude"}, nil); err == nil {
		t.Fatal("a session that cannot be labelled must not be reported as started")
	}
	if !strings.Contains(d.read("calls"), "kill-session -t =sfagent-0123456789abcdef") {
		t.Errorf("the half-created session was left behind:\n%s", d.read("calls"))
	}
}

// display-message answers an unresolvable target with an empty line and exit 0,
// so the shape of the reply is checked rather than assumed.
func TestCLIStart_RefusesAnEmptyPaneID(t *testing.T) {
	d := newFakeDir(t)
	d.write("pane", "")
	cli := tmux.NewCLI(d.bin())

	if _, err := cli.Start(context.Background(), sample(), []string{"claude"}, nil); err == nil {
		t.Fatal("an empty pane id must be an error, not a session nobody can address")
	}
}

func TestCLISendKeys_TargetsThePaneLiterally(t *testing.T) {
	d := newFakeDir(t)
	cli := tmux.NewCLI(d.bin())
	if err := cli.SendKeys(context.Background(), "%7", "type Enter please", true); err != nil {
		t.Fatal(err)
	}
	calls := d.read("calls")
	if !strings.Contains(calls, "send-keys -t %7 -l -- type Enter please") {
		t.Errorf("literal text must go through -l, or the word Enter is typed as a keystroke:\n%s", calls)
	}
	if !strings.Contains(calls, "send-keys -t %7 Enter") {
		t.Errorf("submit must send the Enter key:\n%s", calls)
	}
}

func TestCLICapture_KeepsEscapesAndAsksForScrollback(t *testing.T) {
	d := newFakeDir(t)
	cli := tmux.NewCLI(d.bin())
	if _, err := cli.Capture(context.Background(), "%7", 200); err != nil {
		t.Fatal(err)
	}
	calls := d.read("calls")
	if !strings.Contains(calls, "capture-pane -p -e -t %7 -S -200") {
		t.Errorf("a snapshot must carry escapes, or it disagrees with the live stream:\n%s", calls)
	}
}

// A Stop on a session that has already gone is SUCCESS: the caller's intent is
// satisfied, and reporting Internal left the UI showing a dead agent as running.
func TestCLISignalKill_IsIdempotent(t *testing.T) {
	d := newFakeDir(t)
	d.write("fail", "display-message\tcan't find pane: %0\nkill-session\tcan't find session: sfagent-x")
	cli := tmux.NewCLI(d.bin())

	if err := cli.Signal(context.Background(), sample(), session.SignalKill); err != nil {
		t.Fatalf("killing a session that is already gone must succeed: %v", err)
	}
}

func TestCLISignal_TermIsARealSignalNotCtrlD(t *testing.T) {
	d := newFakeDir(t)
	d.write("pane", "1") // display-message answers with a pid for this call
	cli := tmux.NewCLI(d.bin())

	// pid 1 is not ours to signal, so this fails — the point is WHICH tmux call
	// was made: a pane_pid lookup, not a send-keys C-d.
	_ = cli.Signal(context.Background(), sample(), session.SignalTerm)
	calls := d.read("calls")
	if !strings.Contains(calls, "#{pane_pid}") {
		t.Errorf("TERM must resolve the pane's pid:\n%s", calls)
	}
	if strings.Contains(calls, "C-d") {
		t.Errorf("C-d is EOF on the pty, which a full-screen agent discards:\n%s", calls)
	}
}

func TestCLISignal_UnknownIsRefused(t *testing.T) {
	d := newFakeDir(t)
	cli := tmux.NewCLI(d.bin())
	if err := cli.Signal(context.Background(), sample(), session.Signal(42)); err == nil {
		t.Fatal("an unknown signal must not default to anything")
	}
	if d.read("calls") != "" {
		t.Errorf("nothing should have been sent to tmux:\n%s", d.read("calls"))
	}
}

func TestCLIList_ReadsLabelsPerSessionAndOnlyForOurs(t *testing.T) {
	d := newFakeDir(t)
	d.write("sessions", "sfagent-0123456789abcdef\nsomeone-elses-work\n")
	d.write("labels", strings.Join([]string{
		"@sfagent_pane\t%0",
		"@sfagent_owner\tsub-alice",
		"@sfagent_kind\tclaude",
		"@sfagent_workspace\t/srv/a b|c\ttab",
	}, "\n"))
	cli := tmux.NewCLI(d.bin())

	live, err := cli.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 2 {
		t.Fatalf("got %d rows, want 2: %+v", len(live), live)
	}
	ours := live[0]
	if ours.Owner != "sub-alice" || ours.Kind != "claude" || ours.PaneID != "%0" {
		t.Errorf("row = %+v", ours)
	}
	// A value containing a space, a pipe AND a tab must survive: this is why the
	// labels are read one per call instead of packed into a format string.
	if ours.Workspace != "/srv/a b|c\ttab" {
		t.Errorf("workspace = %q", ours.Workspace)
	}
	if live[1].Owner != "" {
		t.Errorf("a stranger's session must not be interrogated: %+v", live[1])
	}
	if strings.Count(d.read("calls"), "-q -v @sfagent") != 4 {
		t.Errorf("expected exactly four label reads, calls:\n%s", d.read("calls"))
	}
}

// A host with no tmux server has zero sessions; that is a normal state on a
// freshly booted box, not a failure to report.
func TestCLIList_TreatsNoServerAsZeroSessions(t *testing.T) {
	d := newFakeDir(t)
	d.write("fail", "list-sessions\tno server running on /tmp/tmux-0/default")
	cli := tmux.NewCLI(d.bin())

	live, err := cli.List(context.Background())
	if err != nil || live != nil {
		t.Fatalf("List = (%v, %v), want (nil, nil)", live, err)
	}
}
