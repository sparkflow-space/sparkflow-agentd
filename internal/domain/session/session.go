package session

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// State is where a session is. A session is a process, so the only states that
// matter are the ones an operator would act on.
type State int

const (
	StateStarting State = iota + 1
	StateIdle
	StateWorking
	StateWaitingInput
	StateExited
)

var stateNames = map[State]string{
	StateStarting: "starting", StateIdle: "idle", StateWorking: "working",
	StateWaitingInput: "waiting_input", StateExited: "exited",
}

func (s State) String() string {
	if n, ok := stateNames[s]; ok {
		return n
	}
	return "unknown"
}

// CanMoveTo reports whether a transition is allowed. Exited is terminal on
// purpose: a dead process does not come back, and a "restarted" session is a
// new one with a new id — otherwise its transcript would silently splice two
// different processes together.
func (s State) CanMoveTo(next State) bool {
	if s == StateExited {
		return false
	}
	switch next {
	case StateStarting:
		return false // only Start mints this
	case StateIdle, StateWorking, StateWaitingInput, StateExited:
		return true
	default:
		return false
	}
}

// Session is one CLI agent running in one tmux session.
type Session struct {
	ID string
	// Owner is the Actor.Subject of the person the session belongs to.
	//
	// It is not decoration. Without it every RPC is "any authenticated caller
	// may drive any session": List hands out the ids, and Send then types into
	// a stranger's agent — which holds their repository and their logged-in
	// CLI. The recorded auth topology makes that concrete, because the project
	// id is the single audience and every ordinary product token satisfies it,
	// so "authenticated" is a much larger set than "entitled".
	//
	// It is persisted ON THE TMUX SESSION (see the tmux adapter's session
	// options), not in a file: tmux outlives the daemon, so ownership has to
	// survive a restart in the same place liveness does.
	Owner string
	Kind  string
	// Workspace is the agent's working directory, always inside the configured
	// root.
	Workspace string
	TmuxName  string
	// PaneID is tmux's own handle for the agent's pane ("%0").
	//
	// Every pane-scoped command addresses THIS, never the session name, for two
	// reasons measured on tmux 3.4: a `-t <name>` target also matches by
	// PREFIX, and a session name is reusable while a pane id is never reused;
	// and the output stream's %output notifications carry the pane id, so
	// filtering on it is what keeps a window the agent opens for itself out of
	// the transcript the client is painting.
	PaneID       string
	State        State
	Cols, Rows   int
	CreatedAt    time.Time
	LastActivity time.Time
}

// Live is what tmux itself knows about a session, read back at reconciliation.
// The daemon trusts tmux for liveness and for these labels, because a file
// describing sessions could describe a world that no longer exists.
type Live struct {
	TmuxName  string
	PaneID    string
	Owner     string
	Kind      string
	Workspace string
}

// Errors are sentinel values so the transport layer can map them without
// string matching.
var (
	ErrUnknownKind      = errors.New("agentd: kind is not in the allow-list")
	ErrWorkspaceEscapes = errors.New("agentd: workspace is outside the configured root")
	ErrWorkspaceEmpty   = errors.New("agentd: workspace is required")
	ErrBadTransition    = errors.New("agentd: illegal state transition")
	ErrNotFound         = errors.New("agentd: no such session")
	ErrWorkspaceMissing = errors.New("agentd: workspace does not exist")
	ErrEnvNotAllowed    = errors.New("agentd: environment variable is not allowed")
	ErrUnknownSignal    = errors.New("agentd: unknown signal")
	ErrTooManySessions  = errors.New("agentd: too many concurrent sessions")
	ErrNoActor          = errors.New("agentd: unauthenticated")
)

// Catalog is the daemon's allow-list: the agent kinds it will start, mapped to
// the argv it runs.
//
// A caller passes a KIND, never a command line. That is the whole point: the
// design forbids an LLM-chosen command, and this is the last place able to
// enforce it — the service above may be compromised or simply wrong.
type Catalog map[string][]string

// Resolve returns the argv for a kind.
func (c Catalog) Resolve(kind string) ([]string, error) {
	argv, ok := c[kind]
	if !ok || len(argv) == 0 {
		return nil, fmt.Errorf("%w: %q", ErrUnknownKind, kind)
	}
	return append([]string(nil), argv...), nil
}

// Kinds lists the allowed kinds, sorted for a stable API response.
func (c Catalog) Kinds() []string {
	out := make([]string, 0, len(c))
	for k := range c {
		out = append(out, k)
	}
	// insertion sort: the catalog is a handful of entries and this keeps the
	// domain free of imports it does not need.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// ValidateWorkspace resolves a caller-supplied workspace against the configured
// root and refuses anything outside it.
//
// It is lexical on purpose and deliberately strict: `filepath.Clean` first, so
// `/root/../etc` cannot sneak past a prefix test, and then a separator-aware
// prefix check, so `/srv/work-evil` does not pass as a child of `/srv/work`.
// A symlink inside the root can still point out of it — the caller controls
// what it creates there, and the daemon additionally runs as an unprivileged
// user; that residual risk is named in the README rather than pretended away.
func ValidateWorkspace(root, workspace string) (string, error) {
	if strings.TrimSpace(workspace) == "" {
		return "", ErrWorkspaceEmpty
	}
	if !filepath.IsAbs(workspace) {
		return "", fmt.Errorf("%w: %q is not absolute", ErrWorkspaceEscapes, workspace)
	}
	cleanRoot := filepath.Clean(root)
	clean := filepath.Clean(workspace)
	if clean != cleanRoot && !strings.HasPrefix(clean, cleanRoot+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %q", ErrWorkspaceEscapes, workspace)
	}
	return clean, nil
}

// Default terminal size. A detached tmux session created without an explicit
// size is 80x24, and a full-screen TUI in an 80x24 window that the user
// believes is larger draws garbage — the same class of bug as the 0x0 PTY that
// already cost this estate a debugging session (see the TUI harness notes).
const (
	DefaultCols = 160
	DefaultRows = 48
)

// NormaliseSize fills in the daemon's defaults for a caller that sent none.
func NormaliseSize(cols, rows int) (int, int) {
	if cols <= 0 {
		cols = DefaultCols
	}
	if rows <= 0 {
		rows = DefaultRows
	}
	return cols, rows
}

// TmuxName derives the tmux session name from the session id.
//
// Prefixed so a human running `tmux ls` on the host can tell at a glance which
// sessions belong to the daemon, and so reconciliation after a restart can find
// them without keeping a file nobody would trust.
const TmuxPrefix = "sfagent-"

func TmuxName(id string) string { return TmuxPrefix + id }

// IsOurs reports whether a tmux session name belongs to this daemon.
func IsOurs(tmuxName string) bool { return strings.HasPrefix(tmuxName, TmuxPrefix) }

// IDLen is the exact length of a session id: 8 random bytes, hex-encoded.
const IDLen = 16

// ValidID reports whether id has the shape this daemon mints — exactly IDLen
// lowercase hex characters.
//
// This is a SECURITY check, not tidiness. Reconciliation adopts tmux sessions
// by name and derives the id by trimming the prefix, so without it a session
// named `sfagent-a` becomes the id "a" — and because a tmux `-t <name>` target
// matches by prefix (measured on 3.4: `kill-session -t sfagent-abc` killed
// `sfagent-abcdef`), commands aimed at that id would land on a real session
// belonging to somebody else. Fixing the length removes the ambiguity
// entirely: no valid name can be a strict prefix of another valid name.
func ValidID(id string) bool {
	if len(id) != IDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// IDFromTmuxName is the inverse of TmuxName, used when re-adopting sessions.
// A name carrying the prefix but not a well-formed id is NOT ours: it is a
// stranger's session that happens to start with our prefix, or a leftover from
// an older naming scheme, and adopting it would mean offering somebody else's
// terminal over gRPC.
func IDFromTmuxName(tmuxName string) (string, bool) {
	if !IsOurs(tmuxName) {
		return "", false
	}
	id := strings.TrimPrefix(tmuxName, TmuxPrefix)
	if !ValidID(id) {
		return "", false
	}
	return id, true
}
