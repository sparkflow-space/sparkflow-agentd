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
	ID           string
	Kind         string
	Workspace    string
	TmuxName     string
	State        State
	Cols, Rows   int
	CreatedAt    time.Time
	LastActivity time.Time
}

// Errors are sentinel values so the transport layer can map them without
// string matching.
var (
	ErrUnknownKind      = errors.New("agentd: kind is not in the allow-list")
	ErrWorkspaceEscapes = errors.New("agentd: workspace is outside the configured root")
	ErrWorkspaceEmpty   = errors.New("agentd: workspace is required")
	ErrBadTransition    = errors.New("agentd: illegal state transition")
	ErrNotFound         = errors.New("agentd: no such session")
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

// IDFromTmuxName is the inverse of TmuxName, used when re-adopting sessions.
func IDFromTmuxName(tmuxName string) (string, bool) {
	if !IsOurs(tmuxName) {
		return "", false
	}
	return strings.TrimPrefix(tmuxName, TmuxPrefix), true
}
