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
	Start(ctx context.Context, name, workspace string, argv, env []string, cols, rows int) error
	SendKeys(ctx context.Context, name, text string, submit bool) error
	Capture(ctx context.Context, name string, lines int) (string, error)
	Signal(ctx context.Context, name string, sig session.Signal) error
	Kill(ctx context.Context, name string) error
	List(ctx context.Context) ([]string, error)
	// Subscribe delivers output from `from` onward until ctx is done. The
	// implementation owns buffering and replay; the use case only forwards.
	Subscribe(ctx context.Context, name string, from uint64) (<-chan session.Chunk, error)
}

// Audit records who did what. A CLI agent can delete a repository; the log is
// the only way to answer "who told it to" afterwards, so it is a port rather
// than a log line someone might quietly drop.
type Audit interface {
	Record(ctx context.Context, actor, action, sessionID, detail string)
}

// Clock and IDs exist so tests are deterministic.
type Clock interface{ Now() time.Time }
type IDs interface{ New() string }
