// Package sessions holds the daemon's use cases: start, send, stream, snapshot,
// signal, list and reconcile. It orchestrates; it does not know gRPC, and it
// reaches tmux only through the Tmux port.
package sessions

import (
	"context"
	"time"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
)

// Tmux is everything the daemon needs from tmux. Implemented by
// internal/infra/tmux; faked in tests, which is why the whole use-case layer
// can be tested without tmux installed.
type Tmux interface {
	// Start creates the detached session AND labels it with its owner, kind and
	// workspace, returning the pane id tmux assigned.
	//
	// Both halves or neither: a session that cannot be labelled is killed
	// before Start returns, because an unattributable session is reachable by
	// nobody and would show up at the next reconciliation as a stranger.
	Start(ctx context.Context, s session.Session, argv, env []string) (paneID string, err error)
	// SendKeys types into a PANE, addressed by pane id.
	SendKeys(ctx context.Context, paneID, text string, submit bool) error
	// Capture returns the pane's screen, plus `lines` of scrollback above it.
	Capture(ctx context.Context, paneID string, lines int) (string, error)
	// Signal delivers INT/TERM/KILL. KILL is idempotent: killing a session that
	// has already gone is success, because the caller's intent is satisfied.
	Signal(ctx context.Context, s session.Session, sig session.Signal) error
	// List is the liveness truth, with the labels Start wrote.
	List(ctx context.Context) ([]session.Live, error)

	// Watch starts buffering a session's output.
	//
	// No ctx ON PURPOSE: the attach must outlive the RPC that opened the
	// session. Passing the caller's context here would tear the stream down the
	// moment Start returned, and the first client to connect would find an
	// empty transcript.
	Watch(name, paneID string) error
	// Subscribe delivers output from `from` onward until ctx is done. The
	// implementation owns buffering and replay; the use case only forwards.
	Subscribe(ctx context.Context, name string, from uint64) (<-chan session.Chunk, error)
	// Release drops a dead session's attach and buffer.
	Release(name string)
}

// Workspaces is the host's filesystem as the use cases need it.
//
// IsDir exists because a lexical check is not enough: tmux SILENTLY falls
// back to its own working directory when `-c` names a path that does not
// exist (measured on 3.4 — `new-session -c /srv/agents/nope` exited 0 and the
// pane printed CWD=/root), so a typo would run the agent in the daemon's own
// home while the session record and the audit line both claimed the folder.
type Workspaces interface {
	IsDir(path string) bool
	// Resolve returns the absolute path with every symlink resolved, so the
	// containment check sees where a path really goes.
	Resolve(path string) (string, error)
	IsGitRoot(path string) bool
	ListDirs(path string) ([]session.Dir, error)
	// AddWorktree creates a fresh worktree of repo at path on a new branch.
	AddWorktree(ctx context.Context, repo, path, branch string) error
}

// Events tells the server about a session's state changes (the host channel
// carries them up). Optional: nil drops them.
type Events interface {
	SessionState(id string, st session.State)
}

// Audit records who did what, and — equally — who was refused.
//
// A CLI agent can delete a repository; the log is the only way to answer "who
// told it to" afterwards, so it is a port rather than a log line someone might
// quietly drop.
type Audit interface {
	Record(ctx context.Context, e AuditEvent)
}

// AuditEvent is one attempted command and its outcome.
//
// The outcome is part of the record because a success-only log is blind to
// exactly the traffic worth reading: a caller enumerating kinds, or probing
// `/srv/agents/../../etc`, produces nothing at all in a log written after the
// action succeeds.
type AuditEvent struct {
	Actor     string
	Action    string
	SessionID string
	// Detail describes the request. It carries env NAMES and never values.
	Detail  string
	Outcome Outcome
	// Reason is the refusal or error, empty on success.
	Reason string
}

// Outcome is what happened to the attempt.
type Outcome string

const (
	OutcomeOK      Outcome = "ok"
	OutcomeRefused Outcome = "refused"
	OutcomeError   Outcome = "error"
)

// Clock and IDs exist so tests are deterministic.
type Clock interface{ Now() time.Time }
type IDs interface{ New() string }
