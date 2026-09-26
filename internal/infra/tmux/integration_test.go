//go:build tmux

// Run with:  go test -tags tmux ./internal/infra/tmux/ -count=1
//
// This is the only test in the repo that needs a real tmux. It is tagged out of
// the default run so `go test ./...` stays green on a machine without one —
// and tagged IN rather than skipped, so nobody mistakes "0 tests ran" for
// "the adapter works".
package tmux_test

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/tmux"
)

func TestAgainstRealTmux(t *testing.T) {
	cli := tmux.NewCLI("")
	ctrl := tmux.NewControl("", 1024, 1<<20, func(format string, v ...any) {
		// Not t.Logf: a stream goroutine can log after the test function
		// returns, and t.Logf then panics.
		fmt.Printf("control: "+format+"\n", v...)
	})
	defer ctrl.Close()

	// A 16-hex id, the shape the daemon mints and reconciliation accepts.
	sess := session.Session{
		ID:        fmt.Sprintf("%016x", time.Now().UnixNano()),
		Owner:     "sub-integration",
		Kind:      "bash",
		Workspace: "/tmp",
		Cols:      200, Rows: 50,
	}
	sess.TmuxName = session.TmuxName(sess.ID)
	name := sess.TmuxName
	ctx := context.Background()

	// Whatever happens below, do not leave a session on the host.
	defer func() {
		_ = cli.Signal(context.Background(), sess, session.SignalKill)
		if out, _ := exec.Command("tmux", "has-session", "-t", "="+name).CombinedOutput(); len(out) == 0 {
			t.Errorf("leaked tmux session %s", name)
		}
	}()

	pane, err := cli.Start(ctx, sess, []string{"bash", "--norc"}, nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !strings.HasPrefix(pane, "%") {
		t.Fatalf("Start returned pane %q", pane)
	}
	sess.PaneID = pane

	// The labels must be readable back OFF TMUX: that is the mechanism ownership
	// survives a daemon restart by, so it is proved here rather than assumed.
	live, err := cli.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var found *session.Live
	for i := range live {
		if live[i].TmuxName == name {
			found = &live[i]
		}
	}
	if found == nil {
		t.Fatalf("List did not return %s: %+v", name, live)
	}
	if found.Owner != "sub-integration" || found.Kind != "bash" || found.Workspace != "/tmp" || found.PaneID != pane {
		t.Errorf("labels came back as %+v", *found)
	}

	// The size must be what we asked for, not tmux's 80x24 default.
	size, err := exec.Command("tmux", "display-message", "-p", "-t", pane, "#{pane_width}x#{pane_height}").Output()
	if err != nil {
		t.Fatalf("display-message: %v", err)
	}
	if got := strings.TrimSpace(string(size)); got != "200x50" {
		t.Fatalf("pane size = %s, want 200x50", got)
	}

	streamCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ch, err := ctrl.Subscribe(streamCtx, name, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	const marker = "agentd-marker-42"
	if err := cli.SendKeys(ctx, pane, "echo "+marker, true); err != nil {
		t.Fatalf("SendKeys: %v", err)
	}

	var seen strings.Builder
	var lastSeq uint64
	deadline := time.After(8 * time.Second)
	for {
		select {
		case ck, ok := <-ch:
			if !ok {
				t.Fatalf("stream closed before the marker; saw %q", seen.String())
			}
			if ck.Seq < lastSeq {
				t.Fatalf("Seq went backwards: %d after %d", ck.Seq, lastSeq)
			}
			lastSeq = ck.Seq
			seen.Write(ck.Data)
			// The echoed command line contains the marker too, so the assertion
			// is a LINE that is exactly the marker — that is the program's own
			// output. Written as a substring check first, which matched the
			// echo and would have passed with a broken decoder.
			if hasExactLine(seen.String(), marker) {
				goto found
			}
		case <-deadline:
			t.Fatalf("no marker within the deadline; saw %q", seen.String())
		}
	}
found:
	// Decoded, not escaped: if the decoder were skipped this would read \015\012.
	if strings.Contains(seen.String(), `\015`) {
		t.Error("output still carries octal escapes — DecodeOutput is not on the path")
	}

	if err := captureShowsMarker(cli, pane, marker); err != nil {
		t.Error(err)
	}

	// A window the agent opens for itself must NOT reach the client's stream:
	// the transcript is one terminal, and capture-pane and send-keys both address
	// the pane we started, so merging another one corrupts what the panel paints.
	if out, err := exec.Command("tmux", "new-window", "-t", "="+name, "-d",
		"echo SECONDWINDOW; sleep 5").CombinedOutput(); err != nil {
		t.Fatalf("new-window: %v: %s", err, out)
	}
	const marker2 = "agentd-marker-43"
	if err := cli.SendKeys(ctx, pane, "echo "+marker2, true); err != nil {
		t.Fatalf("SendKeys: %v", err)
	}
	deadline2 := time.After(8 * time.Second)
	for !hasExactLine(seen.String(), marker2) {
		select {
		case ck, ok := <-ch:
			if !ok {
				t.Fatalf("stream closed before the second marker; saw %q", seen.String())
			}
			seen.Write(ck.Data)
		case <-deadline2:
			t.Fatalf("no second marker within the deadline; saw %q", seen.String())
		}
	}
	if strings.Contains(seen.String(), "SECONDWINDOW") {
		t.Error("output from another pane of the session leaked into the stream")
	}

	if err := cli.Signal(ctx, sess, session.SignalKill); err != nil {
		t.Fatalf("Signal KILL: %v", err)
	}
	// KILL is idempotent: the caller's intent is satisfied either way.
	if err := cli.Signal(ctx, sess, session.SignalKill); err != nil {
		t.Errorf("a second KILL must succeed, got %v", err)
	}
	after, err := cli.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, l := range after {
		if l.TmuxName == name {
			t.Fatal("KILL must leave no session behind")
		}
	}
}

// hasExactLine reports whether any line — after stripping the CSI sequences a
// shell sprinkles around its prompt — is exactly want.
func hasExactLine(s, want string) bool {
	for _, line := range strings.FieldsFunc(stripANSI(s), func(r rune) bool { return r == '\n' || r == '\r' }) {
		if strings.TrimSpace(line) == want {
			return true
		}
	}
	return false
}

// stripANSI removes CSI sequences (ESC [ … final-byte). Enough for a prompt;
// this is a test helper, not a terminal emulator.
func stripANSI(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] == 0x1b && i+1 < len(s) && s[i+1] == '[' {
			j := i + 2
			for j < len(s) && (s[j] < 0x40 || s[j] > 0x7e) {
				j++
			}
			i = j + 1
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

// captureShowsMarker checks the snapshot path — the one a reconnecting client
// paints before the stream resumes.
func captureShowsMarker(cli *tmux.CLI, pane, marker string) error {
	text, err := cli.Capture(context.Background(), pane, 0)
	if err != nil {
		return fmt.Errorf("Capture: %w", err)
	}
	if !strings.Contains(text, marker) {
		return fmt.Errorf("capture-pane does not show the marker: %q", text)
	}
	return nil
}
