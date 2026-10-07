// Package channel is the daemon's one transport since AGENTD-003: the
// outbound host channel to agent-sessions. It dials, says Hello, executes the
// operations that come down, sends results and output up, and reconnects with
// backoff. It replaces the inbound gRPC server and its interceptors.
//
// The rule it exists to enforce: a host is SINGLE-OWNER. Every operation names
// the person it is for, and anything naming someone other than the person who
// enrolled this device is refused here — even though agent-sessions already
// routes only that person's work to this host. A misroute upstream must not
// become code running as the wrong person.
package channel

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/sparkflow-space/sparkflow-agentd/internal/application/sessions"
	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
	agentv1 "github.com/sparkflow-space/sparkflow-agentd/internal/generated/agent/v1"
)

// Stream is one open channel (agentv1.HostChannel_ConnectClient).
type Stream interface {
	Send(*agentv1.HostMessage) error
	Recv() (*agentv1.ServerMessage, error)
}

// Dial opens a channel with a fresh access token. Implemented in
// infra/hostchannel; the composition root injects it.
type Dial func(ctx context.Context) (Stream, error)

// ErrPermanent wraps a failure that retrying cannot fix: the host was revoked,
// or the sign-in is no longer valid. Run returns it instead of looping.
var ErrPermanent = errors.New("permanent")

// Runner is the channel loop.
type Runner struct {
	Dial     Dial
	Sessions *sessions.Service
	Audit    sessions.Audit
	// Owner is the enrolled person's subject; HostID this device's host id.
	Owner   string
	HostID  string
	Version string
	Kinds   []string
	// Backoff bounds; zero means 1s..60s.
	MinBackoff, MaxBackoff time.Duration
	// Classify says whether a connect/stream error is permanent (revoked,
	// signed out). nil means "never".
	Classify func(error) bool

	mu  sync.Mutex
	out chan *agentv1.HostMessage // the current connection's send queue
}

// SessionState implements sessions.Events: state changes go up the channel
// while one is open. One lost while offline is repaired by the Hello the next
// connection sends, which lists what is actually running.
func (r *Runner) SessionState(id string, st session.State) {
	r.enqueue(&agentv1.HostMessage{Msg: &agentv1.HostMessage_Event{Event: &agentv1.SessionEvent{
		SessionId: id, State: st.String(), TsUnixMs: time.Now().UnixMilli(),
	}}})
}

func (r *Runner) enqueue(m *agentv1.HostMessage) bool {
	r.mu.Lock()
	out := r.out
	r.mu.Unlock()
	if out == nil {
		return false
	}
	select {
	case out <- m:
		return true
	default:
		return false
	}
}

// Run holds the channel until ctx ends or a permanent error.
func (r *Runner) Run(ctx context.Context) error {
	minB, maxB := r.MinBackoff, r.MaxBackoff
	if minB <= 0 {
		minB = time.Second
	}
	if maxB <= 0 {
		maxB = time.Minute
	}
	backoff := minB
	for {
		started := time.Now()
		err := r.once(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if r.Classify != nil && r.Classify(err) {
			return fmt.Errorf("%w: %v", ErrPermanent, err)
		}
		// A connection that held for a while was healthy; start the backoff
		// over rather than punishing the next blip for an old outage.
		if time.Since(started) > 2*time.Minute {
			backoff = minB
		}
		wait := backoff/2 + rand.N(backoff/2+1)
		log.Printf("host channel: %v; reconnecting in %s", err, wait.Round(100*time.Millisecond))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		if backoff *= 2; backoff > maxB {
			backoff = maxB
		}
	}
}

// once is one connection's life.
func (r *Runner) once(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	st, err := r.Dial(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	owner := session.Actor{Subject: r.Owner}
	hello := &agentv1.Hello{HostId: r.HostID, Version: r.Version, Kinds: r.Kinds}
	for _, s := range r.Sessions.List(ctx, owner) {
		if s.State == session.StateExited {
			continue
		}
		hello.Sessions = append(hello.Sessions, &agentv1.HostSession{
			Id: s.ID, OwnerSub: s.Owner, Kind: s.Kind, Workspace: s.Workspace, State: s.State.String(),
			CreatedAtUnixMs: s.CreatedAt.UnixMilli(), LastActivityUnixMs: s.LastActivity.UnixMilli(),
		})
	}
	if err := st.Send(&agentv1.HostMessage{Msg: &agentv1.HostMessage_Hello{Hello: hello}}); err != nil {
		return fmt.Errorf("hello: %w", err)
	}
	first, err := st.Recv()
	if err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	w := first.GetWelcome()
	if w == nil {
		return errors.New("handshake: the server did not answer with Welcome")
	}
	log.Printf("host channel: connected as %q", w.GetName())

	out := make(chan *agentv1.HostMessage, 256)
	r.mu.Lock()
	r.out = out
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		if r.out == out {
			r.out = nil
		}
		r.mu.Unlock()
	}()

	every := time.Duration(w.GetHeartbeatSeconds()) * time.Second
	if every <= 0 {
		every = 20 * time.Second
	}
	sendErr := make(chan error, 1)
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case m := <-out:
				if err := st.Send(m); err != nil {
					sendErr <- err
					return
				}
			case <-t.C:
				if err := st.Send(&agentv1.HostMessage{Msg: &agentv1.HostMessage_Heartbeat{Heartbeat: &agentv1.Heartbeat{TsUnixMs: time.Now().UnixMilli()}}}); err != nil {
					sendErr <- err
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	subs := &subscriptions{m: map[string]*subEntry{}}
	defer subs.cancelAll()
	queues := &queues{m: map[string]chan func(){}, ctx: ctx}
	recvErr := make(chan error, 1)
	go func() {
		for {
			m, err := st.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			op := m.GetOperation()
			if op == nil {
				continue
			}
			// Operations on ONE session run in the order they arrived: a Send
			// then an Enter, or a Send then an INT, must reach tmux in that
			// order. Different sessions — and Start and ListDirs, which name
			// none — run concurrently.
			if key := sessionKey(op); key != "" {
				queues.push(key, func() { r.handle(ctx, op, out, subs) })
			} else {
				go r.handle(ctx, op, out, subs)
			}
		}
	}()
	select {
	case err := <-sendErr:
		return err
	case err := <-recvErr:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// queues runs each session's operations one at a time, in arrival order.
type queues struct {
	mu  sync.Mutex
	m   map[string]chan func()
	ctx context.Context
}

func (q *queues) push(key string, f func()) {
	q.mu.Lock()
	ch, ok := q.m[key]
	if !ok {
		ch = make(chan func(), 64)
		q.m[key] = ch
		go func() {
			for {
				select {
				case f := <-ch:
					f()
				case <-q.ctx.Done():
					return
				}
			}
		}()
	}
	q.mu.Unlock()
	select {
	case ch <- f:
	case <-q.ctx.Done():
	}
}

func sessionKey(op *agentv1.Operation) string {
	switch x := op.GetOp().(type) {
	case *agentv1.Operation_Send:
		return x.Send.GetSessionId()
	case *agentv1.Operation_Signal:
		return x.Signal.GetSessionId()
	case *agentv1.Operation_Snapshot:
		return x.Snapshot.GetSessionId()
	case *agentv1.Operation_Subscribe:
		return x.Subscribe.GetSessionId()
	case *agentv1.Operation_Unsubscribe:
		return x.Unsubscribe.GetSessionId()
	}
	return ""
}

type subEntry struct{ cancel context.CancelFunc }

type subscriptions struct {
	mu sync.Mutex
	m  map[string]*subEntry
}

// add registers a subscription; a duplicate id is refused rather than
// silently replacing — and leaking — the live one.
func (s *subscriptions) add(id string, c context.CancelFunc) (*subEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.m[id]; dup {
		return nil, false
	}
	e := &subEntry{cancel: c}
	s.m[id] = e
	return e, true
}

func (s *subscriptions) cancel(id string) {
	s.mu.Lock()
	e := s.m[id]
	delete(s.m, id)
	s.mu.Unlock()
	if e != nil {
		e.cancel()
	}
}

// drop removes the entry only if it is still THIS one.
func (s *subscriptions) drop(id string, e *subEntry) {
	s.mu.Lock()
	if s.m[id] == e {
		delete(s.m, id)
	}
	s.mu.Unlock()
	e.cancel()
}

func (s *subscriptions) cancelAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, e := range s.m {
		e.cancel()
		delete(s.m, id)
	}
}

// handle executes one operation and answers it.
func (r *Runner) handle(ctx context.Context, op *agentv1.Operation, out chan<- *agentv1.HostMessage, subs *subscriptions) {
	reply := func(res *agentv1.Result) {
		res.RequestId = op.GetRequestId()
		select {
		case out <- &agentv1.HostMessage{Msg: &agentv1.HostMessage_Result{Result: res}}:
		case <-ctx.Done():
		}
	}
	// The single-owner rule. Audited as a refusal with the name the operation
	// carried, so a misroute upstream is visible on the device it hit.
	if op.GetOwnerSub() == "" || op.GetOwnerSub() != r.Owner {
		r.Audit.Record(ctx, sessions.AuditEvent{
			Actor: op.GetOwnerSub(), Action: opName(op), Outcome: sessions.OutcomeRefused,
			Detail: "request=" + op.GetRequestId(), Reason: "this host serves only the person who enrolled it",
		})
		reply(&agentv1.Result{Error: &agentv1.Error{Code: agentv1.Error_PERMISSION_DENIED, Message: "this host serves only the person who enrolled it"}})
		return
	}
	actor := session.Actor{Subject: r.Owner}

	switch x := op.GetOp().(type) {
	case *agentv1.Operation_Start:
		s := x.Start
		sess, err := r.Sessions.Start(ctx, actor, sessions.StartParams{
			Kind: s.GetKind(), Folder: s.GetFolder(), Label: s.GetLabel(), Env: s.GetEnv(),
			Cols: int(s.GetCols()), Rows: int(s.GetRows()), Brief: s.GetBrief(),
		})
		if err != nil {
			reply(errResult(err))
			return
		}
		reply(&agentv1.Result{Payload: &agentv1.Result_Started{Started: &agentv1.HostSession{
			Id: sess.ID, OwnerSub: sess.Owner, Kind: sess.Kind, Workspace: sess.Workspace, State: sess.State.String(),
			CreatedAtUnixMs: sess.CreatedAt.UnixMilli(), LastActivityUnixMs: sess.LastActivity.UnixMilli(),
		}}})
	case *agentv1.Operation_Send:
		reply(errResult(r.Sessions.Send(ctx, actor, x.Send.GetSessionId(), x.Send.GetText(), x.Send.GetSubmit())))
	case *agentv1.Operation_Signal:
		reply(errResult(r.Sessions.Signal(ctx, actor, x.Signal.GetSessionId(), sigFrom(x.Signal.GetSig()))))
	case *agentv1.Operation_Snapshot:
		text, err := r.Sessions.Snapshot(ctx, actor, x.Snapshot.GetSessionId(), int(x.Snapshot.GetLines()))
		if err != nil {
			reply(errResult(err))
			return
		}
		reply(&agentv1.Result{Payload: &agentv1.Result_Snapshot{Snapshot: text}})
	case *agentv1.Operation_ListDirs:
		path, dirs, err := r.Sessions.ListDirs(ctx, actor, x.ListDirs.GetPath())
		if err != nil {
			reply(errResult(err))
			return
		}
		l := &agentv1.DirListing{Path: path}
		for _, d := range dirs {
			l.Dirs = append(l.Dirs, &agentv1.Dir{Name: d.Name, Git: d.Git})
		}
		reply(&agentv1.Result{Payload: &agentv1.Result_Dirs{Dirs: l}})
	case *agentv1.Operation_Subscribe:
		r.subscribe(ctx, actor, op, x.Subscribe, out, subs, reply)
	case *agentv1.Operation_Unsubscribe:
		subs.cancel(x.Unsubscribe.GetSubscriptionId())
		reply(&agentv1.Result{})
	default:
		reply(&agentv1.Result{Error: &agentv1.Error{Code: agentv1.Error_INVALID_ARGUMENT, Message: "unknown operation"}})
	}
}

func (r *Runner) subscribe(ctx context.Context, actor session.Actor, op *agentv1.Operation, s *agentv1.SubscribeOp,
	out chan<- *agentv1.HostMessage, subs *subscriptions, reply func(*agentv1.Result)) {
	subCtx, cancel := context.WithCancel(ctx)
	ch, err := r.Sessions.Subscribe(subCtx, actor, s.GetSessionId(), s.GetFromSeq())
	if err != nil {
		cancel()
		reply(errResult(err))
		return
	}
	id := op.GetRequestId()
	entry, ok := subs.add(id, cancel)
	if !ok {
		cancel()
		reply(&agentv1.Result{Error: &agentv1.Error{Code: agentv1.Error_INVALID_ARGUMENT, Message: "a subscription with this id is already open"}})
		return
	}
	reply(&agentv1.Result{})
	// Registered and answered inside the session's queue; forwarding runs on
	// its own so the queue — and an Unsubscribe behind it — is not blocked.
	go r.forward(ctx, subCtx, s, id, ch, out, func() { subs.drop(id, entry) })
}

func (r *Runner) forward(ctx, subCtx context.Context, s *agentv1.SubscribeOp, id string, ch <-chan session.Chunk,
	out chan<- *agentv1.HostMessage, done func()) {
	defer done()
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				select {
				case out <- &agentv1.HostMessage{Msg: &agentv1.HostMessage_Output{Output: &agentv1.Output{SessionId: s.GetSessionId(), SubscriptionId: id, End: true}}}:
				case <-ctx.Done():
				}
				return
			}
			m := &agentv1.HostMessage{Msg: &agentv1.HostMessage_Output{Output: &agentv1.Output{
				SessionId: s.GetSessionId(), SubscriptionId: id, Seq: c.Seq, Data: c.Data, TsUnixMs: c.At.UnixMilli(),
			}}}
			select {
			case out <- m:
			case <-subCtx.Done():
				return
			}
		case <-subCtx.Done():
			return
		}
	}
}

func sigFrom(s agentv1.SignalOp_Sig) session.Signal {
	switch s {
	case agentv1.SignalOp_INT:
		return session.SignalInt
	case agentv1.SignalOp_TERM:
		return session.SignalTerm
	case agentv1.SignalOp_KILL:
		return session.SignalKill
	}
	return 0
}

// errResult maps a use-case error onto the wire. A domain refusal keeps its
// sentence (the person sees it); anything else is INTERNAL.
func errResult(err error) *agentv1.Result {
	if err == nil {
		return &agentv1.Result{}
	}
	code := agentv1.Error_INTERNAL
	switch {
	case errors.Is(err, session.ErrNotFound):
		code = agentv1.Error_NOT_FOUND
	case errors.Is(err, session.ErrUnknownKind), errors.Is(err, session.ErrEnvNotAllowed), errors.Is(err, session.ErrUnknownSignal),
		errors.Is(err, session.ErrWorkspaceEscapes), errors.Is(err, session.ErrWorkspaceEmpty), errors.Is(err, session.ErrBadLabel):
		code = agentv1.Error_INVALID_ARGUMENT
	case errors.Is(err, session.ErrWorkspaceMissing):
		code = agentv1.Error_FAILED_PRECONDITION
	case errors.Is(err, session.ErrTooManySessions):
		code = agentv1.Error_RESOURCE_EXHAUSTED
	case errors.Is(err, session.ErrNoActor):
		code = agentv1.Error_PERMISSION_DENIED
	}
	return &agentv1.Result{Error: &agentv1.Error{Code: code, Message: err.Error()}}
}

func opName(op *agentv1.Operation) string {
	switch op.GetOp().(type) {
	case *agentv1.Operation_Start:
		return "start"
	case *agentv1.Operation_Send:
		return "send"
	case *agentv1.Operation_Signal:
		return "signal"
	case *agentv1.Operation_Snapshot:
		return "snapshot"
	case *agentv1.Operation_Subscribe:
		return "stream"
	case *agentv1.Operation_Unsubscribe:
		return "unsubscribe"
	case *agentv1.Operation_ListDirs:
		return "listdirs"
	}
	return "unknown"
}
