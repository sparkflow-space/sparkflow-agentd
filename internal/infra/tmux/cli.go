// Package tmux fulfils the application's Tmux port.
//
// Two mechanisms, deliberately: ordinary CLI calls for the one-shot operations
// (start, send, capture, kill, list) and CONTROL MODE for the output stream.
// Control mode is what makes the stream push-based — tmux emits %output as the
// bytes appear — so nothing here polls capture-pane in a loop.
package tmux

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
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

// Start creates a DETACHED session of an explicit size running argv.
//
// The size is always passed: a detached session created without -x/-y is 80x24,
// and a full-screen agent in a window it believes is wider draws garbage.
func (c *CLI) Start(ctx context.Context, name, workspace string, argv, env []string, cols, rows int) error {
	args := []string{"new-session", "-d", "-s", name, "-c", workspace,
		"-x", strconv.Itoa(cols), "-y", strconv.Itoa(rows)}
	for _, e := range env {
		args = append(args, "-e", e) // tmux ≥ 3.2: per-session environment
	}
	args = append(args, "--")
	args = append(args, argv...)
	_, err := c.run(ctx, args...)
	return err
}

// SendKeys types text. `-l` sends it LITERALLY: without it tmux interprets
// words like "Enter" or "C-c" as key names, so a user whose prompt happens to
// contain one would silently send a keystroke instead of the word.
func (c *CLI) SendKeys(ctx context.Context, name, text string, submit bool) error {
	if text != "" {
		if _, err := c.run(ctx, "send-keys", "-t", name, "-l", "--", text); err != nil {
			return err
		}
	}
	if submit {
		if _, err := c.run(ctx, "send-keys", "-t", name, "Enter"); err != nil {
			return err
		}
	}
	return nil
}

// Capture returns the visible screen, or the last `lines` of scrollback.
func (c *CLI) Capture(ctx context.Context, name string, lines int) (string, error) {
	args := []string{"capture-pane", "-p", "-t", name}
	if lines > 0 {
		args = append(args, "-S", strconv.Itoa(-lines))
	}
	return c.run(ctx, args...)
}

// Signal delivers INT and TERM as the keystrokes a terminal program expects,
// and KILL as an actual kill of the pane's process.
func (c *CLI) Signal(ctx context.Context, name string, sig session.Signal) error {
	switch sig {
	case session.SignalInt:
		_, err := c.run(ctx, "send-keys", "-t", name, "C-c")
		return err
	case session.SignalTerm:
		_, err := c.run(ctx, "send-keys", "-t", name, "C-d")
		return err
	case session.SignalKill:
		// kill-session takes the whole session down, which is what KILL means
		// here; Kill() below is idempotent for the caller that does both.
		_, err := c.run(ctx, "kill-session", "-t", name)
		return err
	default:
		return fmt.Errorf("tmux: unknown signal %v", sig)
	}
}

// Kill removes the session. Killing one that is already gone is NOT an error:
// the caller's KILL path calls Signal then Kill, and a race between them must
// not surface as a failure to the user.
func (c *CLI) Kill(ctx context.Context, name string) error {
	if _, err := c.run(ctx, "kill-session", "-t", name); err != nil {
		if strings.Contains(err.Error(), "can't find session") {
			return nil
		}
		return err
	}
	return nil
}

// List returns every session name the server knows. An empty server is not an
// error — "no server running" means zero sessions, which is a normal state on
// a freshly booted host.
func (c *CLI) List(ctx context.Context) ([]string, error) {
	out, err := c.run(ctx, "list-sessions", "-F", "#{session_name}")
	if err != nil {
		if strings.Contains(err.Error(), "no server running") ||
			strings.Contains(err.Error(), "error connecting") {
			return nil, nil
		}
		return nil, err
	}
	if out == "" {
		return nil, nil
	}
	return strings.Split(out, "\n"), nil
}
