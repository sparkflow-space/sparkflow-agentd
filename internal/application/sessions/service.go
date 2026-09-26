package sessions

import (
	"context"
	"fmt"
	"sync"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
)

// Service is the daemon's use-case layer.
//
// It holds session METADATA in memory and treats tmux as the truth about
// LIVENESS. That split is deliberate: tmux outlives the daemon, so a restart
// must re-adopt what is actually running rather than trust a file that may
// describe a world that no longer exists.
type Service struct {
	tmux    Tmux
	audit   Audit
	clock   Clock
	ids     IDs
	catalog session.Catalog
	root    string

	mu       sync.RWMutex
	sessions map[string]*session.Session
}

func New(tmux Tmux, audit Audit, clock Clock, ids IDs, catalog session.Catalog, root string) *Service {
	return &Service{
		tmux: tmux, audit: audit, clock: clock, ids: ids,
		catalog: catalog, root: root,
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

// Start opens a session. `actor` is the verified human identity from the
// caller's token — it is used for the audit trail only; authorisation happened
// in the service above, which is the only place that knows about projects.
func (s *Service) Start(ctx context.Context, actor string, p StartParams) (*session.Session, error) {
	argv, err := s.catalog.Resolve(p.Kind)
	if err != nil {
		return nil, err
	}
	workspace, err := session.ValidateWorkspace(s.root, p.Workspace)
	if err != nil {
		return nil, err
	}
	cols, rows := session.NormaliseSize(p.Cols, p.Rows)

	id := s.ids.New()
	name := session.TmuxName(id)
	now := s.clock.Now()

	if err := s.tmux.Start(ctx, name, workspace, argv, p.Env, cols, rows); err != nil {
		return nil, fmt.Errorf("start %s: %w", p.Kind, err)
	}

	sess := &session.Session{
		ID: id, Kind: p.Kind, Workspace: workspace, TmuxName: name,
		State: session.StateStarting, Cols: cols, Rows: rows,
		CreatedAt: now, LastActivity: now,
	}
	s.mu.Lock()
	s.sessions[id] = sess
	s.mu.Unlock()

	s.audit.Record(ctx, actor, "start", id, fmt.Sprintf("kind=%s workspace=%s", p.Kind, workspace))
	return sess, nil
}

// Send types into a session. The text is audited in full: what an agent was
// told is exactly the thing a human will want to read back later.
func (s *Service) Send(ctx context.Context, actor, id, text string, submit bool) error {
	sess, err := s.get(id)
	if err != nil {
		return err
	}
	if err := s.tmux.SendKeys(ctx, sess.TmuxName, text, submit); err != nil {
		return err
	}
	s.touch(id, session.StateWorking)
	s.audit.Record(ctx, actor, "send", id, text)
	return nil
}

func (s *Service) Snapshot(ctx context.Context, id string, lines int) (string, error) {
	sess, err := s.get(id)
	if err != nil {
		return "", err
	}
	return s.tmux.Capture(ctx, sess.TmuxName, lines)
}

func (s *Service) Subscribe(ctx context.Context, id string, from uint64) (<-chan session.Chunk, error) {
	sess, err := s.get(id)
	if err != nil {
		return nil, err
	}
	return s.tmux.Subscribe(ctx, sess.TmuxName, from)
}

// Signal delivers INT, TERM or KILL. KILL also kills the tmux session, because
// the design says Stop leaves nothing detached — but it deliberately does NOT
// touch the workspace: uncommitted work stays on disk exactly as the agent left
// it, which is recoverable and honest.
func (s *Service) Signal(ctx context.Context, actor, id string, sig session.Signal) error {
	if !sig.Valid() {
		return fmt.Errorf("agentd: unknown signal %d", sig)
	}
	sess, err := s.get(id)
	if err != nil {
		return err
	}
	if err := s.tmux.Signal(ctx, sess.TmuxName, sig); err != nil {
		return err
	}
	if sig == session.SignalKill {
		if err := s.tmux.Kill(ctx, sess.TmuxName); err != nil {
			return err
		}
		s.touch(id, session.StateExited)
	}
	s.audit.Record(ctx, actor, "signal", id, sig.String())
	return nil
}

func (s *Service) List() []*session.Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*session.Session, 0, len(s.sessions))
	for _, v := range s.sessions {
		cp := *v
		out = append(out, &cp)
	}
	return out
}

// Reconcile re-adopts the tmux sessions this daemon owns and marks as exited
// any it remembers that are gone. Called at boot, because tmux outlives us.
//
// It adopts ONLY names carrying the daemon's prefix: the dev host has other
// people's tmux sessions on it right now, and adopting one would mean offering
// a stranger's terminal over gRPC.
func (s *Service) Reconcile(ctx context.Context) error {
	live, err := s.tmux.List(ctx)
	if err != nil {
		return err
	}
	alive := map[string]bool{}
	for _, name := range live {
		id, ok := session.IDFromTmuxName(name)
		if !ok {
			continue
		}
		alive[id] = true
		s.mu.Lock()
		if _, known := s.sessions[id]; !known {
			now := s.clock.Now()
			s.sessions[id] = &session.Session{
				ID: id, TmuxName: name, State: session.StateIdle,
				CreatedAt: now, LastActivity: now,
			}
		}
		s.mu.Unlock()
	}
	s.mu.Lock()
	for id, sess := range s.sessions {
		if !alive[id] && sess.State != session.StateExited {
			sess.State = session.StateExited
		}
	}
	s.mu.Unlock()
	return nil
}

func (s *Service) get(id string) (*session.Session, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.sessions[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", session.ErrNotFound, id)
	}
	if sess.State == session.StateExited {
		return nil, fmt.Errorf("%w: %s has exited", session.ErrNotFound, id)
	}
	return sess, nil
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
