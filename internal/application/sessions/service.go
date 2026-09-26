package sessions

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
)

// Service is the daemon's use-case layer.
//
// It holds session METADATA in memory and treats tmux as the truth about
// LIVENESS and OWNERSHIP. That split is deliberate: tmux outlives the daemon, so
// a restart must re-adopt what is actually running rather than trust a file that
// may describe a world that no longer exists.
type Service struct {
	tmux    Tmux
	dirs    Workspaces
	audit   Audit
	clock   Clock
	ids     IDs
	catalog session.Catalog
	env     session.EnvPolicy
	root    string
	limits  Limits

	mu       sync.RWMutex
	sessions map[string]*session.Session
}

// Limits bound how much of the host one caller — and everybody together — may
// occupy. A CLI agent is a whole interactive process with a model behind it;
// without a cap, "start" is an unauthenticated-in-effect fork bomb for anyone
// whose token the single-audience topology accepts.
type Limits struct {
	PerOwner int
	Total    int
}

// Default limits, applied when a Limits field is left at zero.
const (
	DefaultPerOwner = 8
	DefaultTotal    = 32
)

// Deps is what the composition root injects. A struct rather than eight
// positional parameters: every one of them is a port, and a mis-ordered pair of
// interfaces compiles.
type Deps struct {
	Tmux          Tmux
	Workspaces    Workspaces
	Audit         Audit
	Clock         Clock
	IDs           IDs
	Catalog       session.Catalog
	Env           session.EnvPolicy
	WorkspaceRoot string
	Limits        Limits
}

func New(d Deps) *Service {
	if d.Limits.PerOwner <= 0 {
		d.Limits.PerOwner = DefaultPerOwner
	}
	if d.Limits.Total <= 0 {
		d.Limits.Total = DefaultTotal
	}
	return &Service{
		tmux: d.Tmux, dirs: d.Workspaces, audit: d.Audit, clock: d.Clock,
		ids: d.IDs, catalog: d.Catalog, env: d.Env, root: d.WorkspaceRoot,
		limits:   d.Limits,
		sessions: map[string]*session.Session{},
	}
}

// StartParams is what a caller may influence. Note what is absent: any command
// line. `Kind` indexes the daemon's allow-list.
type StartParams struct {
	Kind       string
	Workspace  string
	Env        []string
	Cols, Rows int
}

// Start opens a session owned by `actor`.
//
// `actor` is the verified human identity from the caller's token. It is the
// ownership key as well as the audit name: authorisation about PROJECTS happened
// in the service above, which is the only thing that knows about projects, but
// "this session is mine" is enforced here — nothing upstream can enforce it on
// the daemon's behalf.
func (s *Service) Start(ctx context.Context, actor session.Actor, p StartParams) (*session.Session, error) {
	// Built from the RAW request, before validation, so a refusal is logged with
	// what was actually asked for.
	detail := fmt.Sprintf("kind=%q workspace=%q env=[%s] size=%dx%d",
		p.Kind, p.Workspace, strings.Join(session.EnvNames(p.Env), " "), p.Cols, p.Rows)
	fail := func(err error) (*session.Session, error) {
		s.record(ctx, actor, "start", "", detail, err)
		return nil, err
	}

	if !actor.Valid() {
		return fail(session.ErrNoActor)
	}
	argv, err := s.catalog.Resolve(p.Kind)
	if err != nil {
		return fail(err)
	}
	if err := s.env.Validate(p.Env); err != nil {
		return fail(err)
	}
	workspace, err := session.ValidateWorkspace(s.root, p.Workspace)
	if err != nil {
		return fail(err)
	}
	// The lexical check says the path is inside the root; this says it exists.
	// Residual TOCTOU is accepted and named: the directory could be removed
	// between here and tmux, and tmux would then fall back to its own cwd. What
	// this closes is the real case — a typo or a workspace nobody created.
	if !s.dirs.IsDir(workspace) {
		return fail(fmt.Errorf("%w: %s", session.ErrWorkspaceMissing, workspace))
	}
	if err := s.admit(actor.Subject); err != nil {
		return fail(err)
	}

	cols, rows := session.NormaliseSize(p.Cols, p.Rows)
	now := s.clock.Now()
	id := s.ids.New()
	sess := &session.Session{
		ID: id, Owner: actor.Subject, Kind: p.Kind, Workspace: workspace,
		TmuxName: session.TmuxName(id), State: session.StateStarting,
		Cols: cols, Rows: rows, CreatedAt: now, LastActivity: now,
	}

	paneID, err := s.tmux.Start(ctx, *sess, argv, p.Env)
	if err != nil {
		return fail(fmt.Errorf("start %s: %w", p.Kind, err))
	}
	sess.PaneID = paneID

	s.mu.Lock()
	s.sessions[id] = sess
	s.mu.Unlock()

	// Buffering starts NOW, not when a client first connects: the first thing an
	// agent prints is its banner and its first question, and a client that
	// attaches a second later would otherwise never see them.
	if err := s.tmux.Watch(sess.TmuxName, paneID); err != nil {
		s.record(ctx, actor, "watch", id, detail, err)
	}

	s.record(ctx, actor, "start", id, detail, nil)
	cp := *sess
	return &cp, nil
}

// Send types into a session. The text is audited in full: what an agent was
// told is exactly the thing a human will want to read back later.
func (s *Service) Send(ctx context.Context, actor session.Actor, id, text string, submit bool) error {
	detail := fmt.Sprintf("submit=%t text=%q", submit, text)
	sess, err := s.own(id, actor)
	if err != nil {
		s.record(ctx, actor, "send", id, detail, err)
		return err
	}
	if err := s.tmux.SendKeys(ctx, sess.PaneID, text, submit); err != nil {
		s.record(ctx, actor, "send", id, detail, err)
		return err
	}
	s.touch(id, session.StateWorking)
	s.record(ctx, actor, "send", id, detail, nil)
	return nil
}

// Snapshot returns what is on the screen. It is audited too: reading a
// session's terminal is reading whatever the agent has on it — a repository, a
// diff, sometimes a credential the human pasted — so "who looked" is as much
// part of the record as "who typed".
func (s *Service) Snapshot(ctx context.Context, actor session.Actor, id string, lines int) (string, error) {
	detail := fmt.Sprintf("lines=%d", lines)
	sess, err := s.own(id, actor)
	if err != nil {
		s.record(ctx, actor, "snapshot", id, detail, err)
		return "", err
	}
	out, err := s.tmux.Capture(ctx, sess.PaneID, lines)
	s.record(ctx, actor, "snapshot", id, detail, err)
	if err != nil {
		return "", err
	}
	return out, nil
}

func (s *Service) Subscribe(ctx context.Context, actor session.Actor, id string, from uint64) (<-chan session.Chunk, error) {
	detail := fmt.Sprintf("from_seq=%d", from)
	sess, err := s.own(id, actor)
	if err != nil {
		s.record(ctx, actor, "stream", id, detail, err)
		return nil, err
	}
	ch, err := s.tmux.Subscribe(ctx, sess.TmuxName, from)
	s.record(ctx, actor, "stream", id, detail, err)
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// Signal delivers INT, TERM or KILL. KILL takes the tmux session down, because
// the design says Stop leaves nothing detached — but it deliberately does NOT
// touch the workspace: uncommitted work stays on disk exactly as the agent left
// it, which is recoverable and honest.
//
// The grace sequence the design describes (INT first, KILL only for an
// unresponsive agent) belongs to the CALLER: the daemon exposes the three
// signals and does not hold a timer on the caller's behalf, so a Stop button
// sends INT, waits, and sends KILL. Said here because the alternative reading —
// that KILL alone is polite — is the one that loses an agent's flush.
func (s *Service) Signal(ctx context.Context, actor session.Actor, id string, sig session.Signal) error {
	detail := "signal=" + sig.String()
	if !sig.Valid() {
		err := fmt.Errorf("%w: %d", session.ErrUnknownSignal, int(sig))
		s.record(ctx, actor, "signal", id, detail, err)
		return err
	}
	sess, err := s.own(id, actor)
	if err != nil {
		s.record(ctx, actor, "signal", id, detail, err)
		return err
	}
	if err := s.tmux.Signal(ctx, *sess, sig); err != nil {
		s.record(ctx, actor, "signal", id, detail, err)
		return err
	}
	if sig == session.SignalKill {
		// The adapter's KILL is idempotent, so reaching here means the session
		// is gone whether or not it was there a moment ago — which is exactly
		// why the state is set unconditionally. An earlier version returned
		// Internal for a session tmux had already lost and left the row alive,
		// so the UI showed a dead agent as running and Stop kept failing.
		s.touch(id, session.StateExited)
		s.tmux.Release(sess.TmuxName)
	}
	s.record(ctx, actor, "signal", id, detail, nil)
	return nil
}

// List returns only the caller's own sessions.
//
// Returning everything would defeat the unguessable session id: the ids are
// random precisely so a caller cannot aim at somebody else's session, and a
// global List publishes the whole set along with each workspace path.
func (s *Service) List(ctx context.Context, actor session.Actor) []*session.Session {
	s.mu.RLock()
	out := make([]*session.Session, 0, len(s.sessions))
	for _, v := range s.sessions {
		if actor.Owns(v) {
			cp := *v
			out = append(out, &cp)
		}
	}
	s.mu.RUnlock()
	s.record(ctx, actor, "list", "", fmt.Sprintf("returned=%d", len(out)), nil)
	return out
}

// Reconcile re-adopts the tmux sessions this daemon owns and marks as exited any
// it remembers that are gone. Called at boot and periodically, because tmux
// outlives us and a session can die at any moment with nobody watching.
//
// It adopts ONLY names carrying the daemon's prefix AND a well-formed id: the
// dev host has other people's tmux sessions on it right now, and adopting one
// would mean offering a stranger's terminal over gRPC.
func (s *Service) Reconcile(ctx context.Context) error {
	live, err := s.tmux.List(ctx)
	if err != nil {
		return err
	}
	alive := map[string]bool{}
	unattributed := 0
	for _, l := range live {
		id, ok := session.IDFromTmuxName(l.TmuxName)
		if !ok {
			continue
		}
		alive[id] = true
		if l.Owner == "" {
			// Kept, but reachable by nobody: Actor.Owns refuses an empty owner.
			// Not killed — it may be a real agent doing real work, and this
			// daemon did not create it. Named in the log so an operator can
			// decide.
			unattributed++
		}
		s.mu.Lock()
		if _, known := s.sessions[id]; !known {
			now := s.clock.Now()
			s.sessions[id] = &session.Session{
				ID: id, Owner: l.Owner, Kind: l.Kind, Workspace: l.Workspace,
				TmuxName: l.TmuxName, PaneID: l.PaneID, State: session.StateIdle,
				CreatedAt: now, LastActivity: now,
			}
		}
		s.mu.Unlock()
	}

	var gone []string
	s.mu.Lock()
	for id, sess := range s.sessions {
		if !alive[id] && sess.State != session.StateExited {
			sess.State = session.StateExited
			gone = append(gone, sess.TmuxName)
		}
	}
	s.mu.Unlock()
	for _, name := range gone {
		s.tmux.Release(name)
	}

	if unattributed > 0 {
		s.record(ctx, session.System, "reconcile", "",
			fmt.Sprintf("adopted=%d unattributed=%d — a session with no owner label is reachable by nobody; kill it by hand if it is a leftover",
				len(alive), unattributed), nil)
	}
	return nil
}

// admit enforces the concurrency limits. Counts exclude exited sessions, which
// linger in the map as history.
func (s *Service) admit(owner string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var total, mine int
	for _, v := range s.sessions {
		if v.State == session.StateExited {
			continue
		}
		total++
		if v.Owner == owner {
			mine++
		}
	}
	if mine >= s.limits.PerOwner {
		return fmt.Errorf("%w: %d already running for you (limit %d)", session.ErrTooManySessions, mine, s.limits.PerOwner)
	}
	if total >= s.limits.Total {
		return fmt.Errorf("%w: %d already running on this host (limit %d)", session.ErrTooManySessions, total, s.limits.Total)
	}
	return nil
}

// own resolves a session and checks that it belongs to the caller.
//
// A session that exists but belongs to somebody else answers ErrNotFound, the
// same as one that never existed: distinguishing them would turn every RPC into
// an oracle for which ids are real.
func (s *Service) own(id string, actor session.Actor) (*session.Session, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.sessions[id]
	if !ok || !actor.Owns(sess) {
		return nil, fmt.Errorf("%w: %s", session.ErrNotFound, id)
	}
	if sess.State == session.StateExited {
		return nil, fmt.Errorf("%w: %s has exited", session.ErrNotFound, id)
	}
	cp := *sess
	return &cp, nil
}

func (s *Service) touch(id string, next session.State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[id]
	if !ok {
		return
	}
	sess.LastActivity = s.clock.Now()
	if sess.State.CanMoveTo(next) {
		sess.State = next
	}
}

// record writes one audit line for an attempt and its outcome. err == nil is a
// success; a domain sentinel is a REFUSAL and anything else is an error, so a
// reader can tell "we said no" from "something broke".
func (s *Service) record(ctx context.Context, actor session.Actor, action, id, detail string, err error) {
	e := AuditEvent{
		Actor: actor.Name(), Action: action, SessionID: id,
		Detail: detail, Outcome: OutcomeOK,
	}
	if err != nil {
		e.Outcome, e.Reason = classify(err), err.Error()
	}
	s.audit.Record(ctx, e)
}

func classify(err error) Outcome {
	for _, sentinel := range []error{
		session.ErrUnknownKind, session.ErrWorkspaceEscapes, session.ErrWorkspaceEmpty,
		session.ErrWorkspaceMissing, session.ErrEnvNotAllowed, session.ErrUnknownSignal,
		session.ErrTooManySessions, session.ErrNotFound, session.ErrNoActor,
	} {
		if errors.Is(err, sentinel) {
			return OutcomeRefused
		}
	}
	return OutcomeError
}
