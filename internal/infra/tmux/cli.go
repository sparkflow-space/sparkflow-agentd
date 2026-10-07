// Package tmux fulfils the application's Tmux port.
//
// Two mechanisms, deliberately: ordinary CLI calls for the one-shot operations
// (start, send, capture, signal, list) and CONTROL MODE for the output stream.
// Control mode is what makes the stream push-based — tmux emits %output as the
// bytes appear — so nothing here polls capture-pane in a loop.
//
// # Targeting, and why it is by pane id
//
// MEASURED on tmux 3.4: a `-t <session-name>` target also matches by PREFIX —
// `kill-session -t sfagent-abc` killed `sfagent-abcdef`. The `=` exact-match
// prefix is NOT a general fix: it works for target-SESSION commands
// (has-session, kill-session) and is rejected by the target-PANE ones
// (`send-keys -t =name` → "can't find pane", `capture-pane` likewise, and
// `display-message -t =name` prints nothing and still exits 0).
//
// So every pane-scoped command here addresses the PANE ID (`%0`), which tmux
// never reuses and never prefix-matches, and the two session-scoped ones use
// the `=` form. Session ids are also fixed-length (session.ValidID), so no
// valid name can be a strict prefix of another.
package tmux

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
)

// Session options the daemon writes on every session it creates.
//
// They live on the TMUX SESSION rather than in a file because tmux is what
// outlives the daemon: after a restart the daemon re-adopts what is running, and
// ownership has to come back with it. A file would be a second source of truth
// that can disagree with the first.
const (
	optOwner     = "@sfagent_owner"
	optKind      = "@sfagent_kind"
	optWorkspace = "@sfagent_workspace"
	optPane      = "@sfagent_pane"
)

// CLI implements the one-shot half of the port.
type CLI struct {
	// Bin is the tmux binary; "tmux" unless a host needs otherwise.
	Bin string
}

func NewCLI(bin string) *CLI {
	if bin == "" {
		bin = "tmux"
	}
	return &CLI{Bin: bin}
}

func (c *CLI) run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, c.Bin, args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(errb.String())
		if msg == "" {
			msg = strings.TrimSpace(out.String())
		}
		return "", fmt.Errorf("tmux %s: %w: %s", strings.Join(args, " "), err, msg)
	}
	return strings.TrimRight(out.String(), "\n"), nil
}

// exact is the session target form tmux matches exactly.
func exact(name string) string { return "=" + name }

// Start creates a DETACHED session of an explicit size running argv, labels it,
// and returns the pane id tmux assigned.
//
// The size is always passed: a detached session created without -x/-y is 80x24,
// and a full-screen agent in a window it believes is wider draws garbage.
//
// Labelling is part of Start, not a separate step, and a failure to label is a
// failure to start: an unlabelled session would come back from the next
// reconciliation with no owner, which means reachable by nobody — so it is
// killed here rather than left as a puzzle on the host.
func (c *CLI) Start(ctx context.Context, s session.Session, argv, env []string) (string, error) {
	args := []string{"new-session", "-d", "-s", s.TmuxName, "-c", s.Workspace,
		"-x", strconv.Itoa(s.Cols), "-y", strconv.Itoa(s.Rows)}
	for _, e := range env {
		args = append(args, "-e", e) // tmux >= 3.2: per-session environment
	}
	args = append(args, "--")
	args = append(args, argv...)
	if _, err := c.run(ctx, args...); err != nil {
		return "", err
	}

	paneID, err := c.finishStart(ctx, s)
	if err != nil {
		// Best effort: the session exists and must not be left unattributable.
		_, _ = c.run(ctx, "kill-session", "-t", exact(s.TmuxName))
		return "", err
	}
	return paneID, nil
}

func (c *CLI) finishStart(ctx context.Context, s session.Session) (string, error) {
	paneID, err := c.run(ctx, "display-message", "-p", "-t", s.TmuxName, "#{pane_id}")
	if err != nil {
		return "", err
	}
	// display-message answers an unresolvable target with an EMPTY line and exit
	// 0 (measured), so the shape is checked rather than assumed.
	if !strings.HasPrefix(paneID, "%") {
		return "", fmt.Errorf("tmux: no pane id for session %s (got %q)", s.TmuxName, paneID)
	}
	for _, kv := range [][2]string{
		{optOwner, s.Owner}, {optKind, s.Kind},
		{optWorkspace, s.Workspace}, {optPane, paneID},
	} {
		// set-option rejects the "=" target form (measured), so the plain name
		// is used; the fixed-length id means the exact match wins.
		if _, err := c.run(ctx, "set-option", "-t", s.TmuxName, kv[0], kv[1]); err != nil {
			return "", err
		}
	}
	return paneID, nil
}

// SendKeys types text into a pane. `-l` sends it LITERALLY: without it tmux
// interprets words like "Enter" or "C-c" as key names, so a user whose prompt
// happens to contain one would silently send a keystroke instead of the word.
func (c *CLI) SendKeys(ctx context.Context, paneID, text string, submit bool) error {
	if text != "" {
		if _, err := c.run(ctx, "send-keys", "-t", paneID, "-l", "--", text); err != nil {
			return err
		}
	}
	if submit {
		if _, err := c.run(ctx, "send-keys", "-t", paneID, "Enter"); err != nil {
			return err
		}
	}
	return nil
}

// Capture returns the pane's visible screen, plus `lines` of scrollback above
// it. `-e` keeps the escape sequences, so a reconnecting client paints the same
// colours and cursor positioning the live stream carries; without it a snapshot
// and the stream that follows it disagree about what the screen looks like.
func (c *CLI) Capture(ctx context.Context, paneID string, lines int) (string, error) {
	args := []string{"capture-pane", "-p", "-e", "-t", paneID}
	if lines > 0 {
		args = append(args, "-S", strconv.Itoa(-lines))
	}
	return c.run(ctx, args...)
}

// Signal delivers INT as the keystroke a terminal program expects, TERM as a
// real signal to the pane's process, and KILL as a kill of the pane's process
// followed by the session.
//
// TERM is NOT Ctrl-D. An earlier version sent C-d, which is EOF on the pty: a
// full-screen agent in raw mode discards the byte entirely, so a caller asking
// politely got a successful RPC and a process still running.
func (c *CLI) Signal(ctx context.Context, s session.Session, sig session.Signal) error {
	switch sig {
	case session.SignalInt:
		// Ctrl-C through the pty is a real SIGINT to the foreground process
		// group, which is what a CLI agent handles meaningfully.
		_, err := c.run(ctx, "send-keys", "-t", s.PaneID, "C-c")
		return err
	case session.SignalTerm:
		return c.signalPane(ctx, s.PaneID, syscall.SIGTERM)
	case session.SignalKill:
		// Signal the process first so it dies even if tmux is wedged, then take
		// the session down. A session that has already gone is SUCCESS: the
		// caller's intent is satisfied, and a Stop button that reports failure
		// for an agent that exited on its own is worse than useless.
		if err := c.signalPane(ctx, s.PaneID, syscall.SIGKILL); err != nil && !gone(err) {
			return err
		}
		if _, err := c.run(ctx, "kill-session", "-t", exact(s.TmuxName)); err != nil && !gone(err) {
			return err
		}
		return nil
	default:
		return fmt.Errorf("%w: %v", session.ErrUnknownSignal, sig)
	}
}

// signalPane sends a real signal to the process tmux runs in the pane.
func (c *CLI) signalPane(ctx context.Context, paneID string, sig syscall.Signal) error {
	out, err := c.run(ctx, "display-message", "-p", "-t", paneID, "#{pane_pid}")
	if err != nil {
		return err
	}
	pid, convErr := strconv.Atoi(strings.TrimSpace(out))
	if convErr != nil || pid <= 0 {
		return fmt.Errorf("tmux: no pane pid for %s (got %q)", paneID, out)
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	// Only the pane's own process, not its children: the daemon runs as an
	// unprivileged user and a negative-pid group kill would reach anything the
	// agent re-parented, which is a bigger hammer than the caller asked for.
	if err := p.Signal(sig); err != nil {
		return fmt.Errorf("signal %v to pane %s (pid %d): %w", sig, paneID, pid, err)
	}
	return nil
}

// gone reports whether an error means "it is not there any more", which for a
// kill is success.
func gone(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "can't find session") ||
		strings.Contains(msg, "can't find pane") ||
		strings.Contains(msg, "no such session") ||
		strings.Contains(msg, "no server running") ||
		// The server is up but holds NO sessions at all — kept alive by the
		// daemon's own control-mode attach after the agent's pane died.
		// tmux 3.4 then cannot even resolve a "current" session for
		// `kill-session -t =name` and says this instead of "can't find
		// session". Measured on dev 2026-10-07: Stop's KILL failed with it
		// for a session that was already gone, and the server kept the row
		// "running" until the next reconcile.
		strings.Contains(msg, "no current target") ||
		strings.Contains(msg, os.ErrProcessDone.Error())
}

// List returns every session on the server together with the labels Start
// wrote. An empty server is not an error — "no server running" means zero
// sessions, which is a normal state on a freshly booted host.
//
// The labels are read ONE PER CALL with show-options rather than packed into a
// list-sessions format string, and that is not fussiness. MEASURED on tmux 3.4:
// a format string renders a control byte in its OUTPUT as the four characters
// `\037`, so a 0x1f field separator comes back escaped and every line parses as
// one field — the packed version silently returned zero sessions, and a fake
// tmux writing real separator bytes had said it worked. Every printable
// separator is worse, because a workspace path may legitimately contain it.
// `show-options -q -v` hands back one raw value, tab and pipe and all, and an
// unset option is an empty string rather than an error.
//
// The cost is bounded: only names that already look like ours are asked about,
// and the caller (Reconcile) needs the labels only to ADOPT a session it does
// not know, which in the steady state is none of them.
func (c *CLI) List(ctx context.Context) ([]session.Live, error) {
	out, err := c.run(ctx, "list-sessions", "-F", "#{session_name}")
	if err != nil {
		if gone(err) || strings.Contains(err.Error(), "error connecting") {
			return nil, nil
		}
		return nil, err
	}
	if out == "" {
		return nil, nil
	}
	var live []session.Live
	for _, name := range strings.Split(out, "\n") {
		if name == "" {
			continue
		}
		l := session.Live{TmuxName: name}
		if _, ours := session.IDFromTmuxName(name); ours {
			l.PaneID = c.option(ctx, name, optPane)
			l.Owner = c.option(ctx, name, optOwner)
			l.Kind = c.option(ctx, name, optKind)
			l.Workspace = c.option(ctx, name, optWorkspace)
		}
		live = append(live, l)
	}
	return live, nil
}

// option reads one session option. A missing user option is an empty string, not
// an error: a session adopted from an older daemon carries no labels, and the
// caller's job is to notice that it has no owner — not to fail.
func (c *CLI) option(ctx context.Context, name, opt string) string {
	out, err := c.run(ctx, "show-options", "-t", name, "-q", "-v", opt)
	if err != nil {
		return ""
	}
	return out
}
