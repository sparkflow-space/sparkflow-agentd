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
	ctrl := tmux.NewControl("", 1024)
	defer ctrl.Close()

	name := session.TmuxName(fmt.Sprintf("test%d", time.Now().UnixNano()))
	ctx := context.Background()

	// Whatever happens below, do not leave a session on the host.
	defer func() {
		_ = cli.Kill(context.Background(), name)
		if out, _ := exec.Command("tmux", "has-session", "-t", name).CombinedOutput(); len(out) == 0 {
			t.Errorf("leaked tmux session %s", name)
		}
	}()

	if err := cli.Start(ctx, name, "/tmp", []string{"bash", "--norc"}, nil, 200, 50); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// The size must be what we asked for, not tmux's 80x24 default.
	size, err := exec.Command("tmux", "display-message", "-p", "-t", name, "#{pane_width}x#{pane_height}").Output()
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
	if err := cli.SendKeys(ctx, name, "echo "+marker, true); err != nil {
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

	if err := captureShowsMarker(cli, name, marker); err != nil {
		t.Error(err)
	}

	if err := cli.Signal(ctx, name, session.SignalKill); err != nil {
		t.Fatalf("Signal KILL: %v", err)
	}
	names, err := cli.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, n := range names {
		if n == name {
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
func captureShowsMarker(cli *tmux.CLI, name, marker string) error {
	text, err := cli.Capture(context.Background(), name, 0)
	if err != nil {
		return fmt.Errorf("Capture: %w", err)
	}
	if !strings.Contains(text, marker) {
		return fmt.Errorf("capture-pane does not show the marker: %q", text)
	}
	return nil
}
