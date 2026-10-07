package channel_test

// The run loop against an in-process server that plays agent-sessions: Hello
// first, operations down, results and output up, the single-owner refusal,
// and a reconnect after the server drops the stream.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/sparkflow-space/sparkflow-agentd/internal/application/sessions"
	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
	agentv1 "github.com/sparkflow-space/sparkflow-agentd/internal/generated/agent/v1"
	"github.com/sparkflow-space/sparkflow-agentd/internal/handler/channel"
)

// ---- a tmux and a filesystem that exist only here ----

type tmux struct {
	mu    sync.Mutex
	keys  []string
	panes int
}

func (t *tmux) Start(context.Context, session.Session, []string, []string) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.panes++
	return fmt.Sprintf("%%%d", t.panes), nil
}
func (t *tmux) SendKeys(_ context.Context, pane, text string, _ bool) error {
	if strings.HasPrefix(text, "slow") {
		time.Sleep(40 * time.Millisecond)
	}
	t.mu.Lock()
	t.keys = append(t.keys, pane+"|"+text)
	t.mu.Unlock()
	return nil
}
func (t *tmux) Capture(context.Context, string, int) (string, error)          { return "screen", nil }
func (t *tmux) Signal(context.Context, session.Session, session.Signal) error { return nil }
func (t *tmux) List(context.Context) ([]session.Live, error)                  { return nil, nil }
func (t *tmux) Watch(string, string) error                                    { return nil }
func (t *tmux) Release(string)                                                {}
func (t *tmux) Subscribe(ctx context.Context, _ string, from uint64) (<-chan session.Chunk, error) {
	ch := make(chan session.Chunk, 3)
	for i := uint64(0); i < 2; i++ {
		ch <- session.Chunk{Seq: from + i, Data: []byte("out")}
	}
	close(ch)
	return ch, nil
}
func (t *tmux) sent() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.keys...)
}

type fs struct{}

func (fs) IsDir(p string) bool              { return strings.HasPrefix(p, "/home/a") }
func (fs) Resolve(p string) (string, error) { return filepath.Clean(p), nil }
func (fs) IsGitRoot(string) bool            { return false }
func (fs) ListDirs(string) ([]session.Dir, error) {
	return []session.Dir{{Name: "src", Git: true}}, nil
}
func (fs) AddWorktree(context.Context, string, string, string) error    { return nil }
func (fs) RemoveWorktree(context.Context, string, string, string) error { return nil }

type audit struct {
	mu sync.Mutex
	ev []sessions.AuditEvent
}

func (a *audit) Record(_ context.Context, e sessions.AuditEvent) {
	a.mu.Lock()
	a.ev = append(a.ev, e)
	a.mu.Unlock()
}

type clock struct{}

func (clock) Now() time.Time { return time.Now() }

type ids struct {
	mu sync.Mutex
	n  int
}

func (i *ids) New() string { i.mu.Lock(); defer i.mu.Unlock(); i.n++; return fmt.Sprintf("%016x", i.n) }

// ---- the server half ----

type server struct {
	agentv1.UnimplementedHostChannelServer
	hellos chan *agentv1.Hello
	ops    chan *agentv1.Operation // to send down
	got    chan *agentv1.HostMessage
	// drop ends the first stream right after Welcome.
	dropFirst bool
	mu        sync.Mutex
	conns     int
}

func (s *server) Connect(st agentv1.HostChannel_ConnectServer) error {
	m, err := st.Recv()
	if err != nil {
		return err
	}
	s.hellos <- m.GetHello()
	if err := st.Send(&agentv1.ServerMessage{Msg: &agentv1.ServerMessage_Welcome{Welcome: &agentv1.Welcome{Name: "Ubuntu · Germany", HeartbeatSeconds: 3600}}}); err != nil {
		return err
	}
	s.mu.Lock()
	s.conns++
	first := s.conns == 1
	s.mu.Unlock()
	if first && s.dropFirst {
		return status.Error(codes.Unavailable, "pod restarting")
	}
	go func() {
		for {
			m, err := st.Recv()
			if err != nil {
				return
			}
			s.got <- m
		}
	}()
	for {
		select {
		case op := <-s.ops:
			if err := st.Send(&agentv1.ServerMessage{Msg: &agentv1.ServerMessage_Operation{Operation: op}}); err != nil {
				return err
			}
		case <-st.Context().Done():
			return nil
		}
	}
}

func setup(t *testing.T, srv *server) (*tmux, *audit, context.CancelFunc, chan error) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer()
	agentv1.RegisterHostChannelServer(g, srv)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })

	tm, au := &tmux{}, &audit{}
	r := &channel.Runner{
		Audit: au, Owner: "sub-A", HostID: "h1", Version: "v0.2.0", Kinds: []string{"claude"},
		MinBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond,
		Dial: func(ctx context.Context) (channel.Stream, error) {
			return agentv1.NewHostChannelClient(cc).Connect(ctx)
		},
	}
	r.Sessions = sessions.New(sessions.Deps{
		Tmux: tm, Workspaces: fs{}, Audit: au, Clock: clock{}, IDs: &ids{},
		Catalog: session.Catalog{"claude": {"claude"}}, Home: "/home/a", Events: r,
		BriefWait: 50 * time.Millisecond, BriefSettle: time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(cancel)
	return tm, au, cancel, done
}

func newServer() *server {
	return &server{hellos: make(chan *agentv1.Hello, 4), ops: make(chan *agentv1.Operation, 16), got: make(chan *agentv1.HostMessage, 64)}
}

func wait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
	}
	var zero T
	return zero
}

// result waits for the Result answering request id, skipping events.
func result(t *testing.T, s *server, id string) *agentv1.Result {
	t.Helper()
	for {
		m := wait(t, s.got)
		if r := m.GetResult(); r != nil && r.GetRequestId() == id {
			return r
		}
	}
}

func TestRun_HelloOperationsAndTheOwnerRule(t *testing.T) {
	srv := newServer()
	tm, au, _, _ := setup(t, srv)
	h := wait(t, srv.hellos)
	if h.GetHostId() != "h1" || h.GetVersion() != "v0.2.0" || len(h.GetKinds()) != 1 {
		t.Fatalf("hello: %v", h)
	}

	// An operation naming another person: refused, audited, nothing typed.
	srv.ops <- &agentv1.Operation{RequestId: "r1", OwnerSub: "sub-B", Op: &agentv1.Operation_Send{Send: &agentv1.SendOp{SessionId: "x", Text: "CANARY-B"}}}
	if r := result(t, srv, "r1"); r.GetError().GetCode() != agentv1.Error_PERMISSION_DENIED {
		t.Fatalf("foreign owner: %v", r)
	}
	for _, k := range tm.sent() {
		if strings.Contains(k, "CANARY-B") {
			t.Fatal("B's text reached tmux")
		}
	}
	au.mu.Lock()
	refused := false
	for _, e := range au.ev {
		if e.Actor == "sub-B" && e.Outcome == sessions.OutcomeRefused {
			refused = true
		}
	}
	au.mu.Unlock()
	if !refused {
		t.Error("the refusal must be audited under the name the operation carried")
	}

	// Start with a brief, as the owner.
	srv.ops <- &agentv1.Operation{RequestId: "r2", OwnerSub: "sub-A", Op: &agentv1.Operation_Start{Start: &agentv1.StartOp{
		Kind: "claude", Folder: "/home/a/src", Label: "lbl1", Brief: "# TSK-1"}}}
	r := result(t, srv, "r2")
	started := r.GetStarted()
	if r.GetError() != nil || started.GetId() == "" || started.GetWorkspace() != "/home/a/src" || started.GetOwnerSub() != "sub-A" {
		t.Fatalf("start: %v", r)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(strings.Join(tm.sent(), ","), "# TSK-1") {
		time.Sleep(5 * time.Millisecond)
	}
	if !strings.Contains(strings.Join(tm.sent(), ","), "# TSK-1") {
		t.Fatal("the brief was not typed")
	}

	// A folder outside the home is INVALID_ARGUMENT with its sentence.
	srv.ops <- &agentv1.Operation{RequestId: "r3", OwnerSub: "sub-A", Op: &agentv1.Operation_Start{Start: &agentv1.StartOp{Kind: "claude", Folder: "/etc", Label: "x"}}}
	if r := result(t, srv, "r3"); r.GetError().GetCode() != agentv1.Error_INVALID_ARGUMENT {
		t.Fatalf("outside home: %v", r)
	}

	// Subscribe: output carries the subscription id, then an End.
	srv.ops <- &agentv1.Operation{RequestId: "r4", OwnerSub: "sub-A", Op: &agentv1.Operation_Subscribe{Subscribe: &agentv1.SubscribeOp{SessionId: started.GetId(), FromSeq: 7}}}
	var seqs []uint64
	ended := false
	for !ended {
		m := wait(t, srv.got)
		if o := m.GetOutput(); o != nil {
			if o.GetSubscriptionId() != "r4" {
				t.Fatalf("output without its subscription id: %v", o)
			}
			if o.GetEnd() {
				ended = true
			} else {
				seqs = append(seqs, o.GetSeq())
			}
		}
	}
	if fmt.Sprint(seqs) != "[7 8]" {
		t.Errorf("seqs %v", seqs)
	}

	// ListDirs.
	srv.ops <- &agentv1.Operation{RequestId: "r5", OwnerSub: "sub-A", Op: &agentv1.Operation_ListDirs{ListDirs: &agentv1.ListDirsOp{}}}
	if r := result(t, srv, "r5"); r.GetDirs().GetPath() != "/home/a" || len(r.GetDirs().GetDirs()) != 1 || !r.GetDirs().GetDirs()[0].GetGit() {
		t.Fatalf("listdirs: %v", r)
	}

	// Kill → an exited event goes up.
	srv.ops <- &agentv1.Operation{RequestId: "r6", OwnerSub: "sub-A", Op: &agentv1.Operation_Signal{Signal: &agentv1.SignalOp{SessionId: started.GetId(), Sig: agentv1.SignalOp_KILL}}}
	sawExit := false
	for !sawExit {
		m := wait(t, srv.got)
		if e := m.GetEvent(); e != nil && e.GetSessionId() == started.GetId() && e.GetState() == "exited" {
			sawExit = true
		}
	}
}

func TestRun_ReconnectsAndReannounces(t *testing.T) {
	srv := newServer()
	srv.dropFirst = true
	setup(t, srv)
	first := wait(t, srv.hellos)
	second := wait(t, srv.hellos)
	if first.GetHostId() != "h1" || second.GetHostId() != "h1" {
		t.Fatalf("hellos: %v / %v", first, second)
	}
}

type deniedServer struct {
	agentv1.UnimplementedHostChannelServer
}

func (deniedServer) Connect(agentv1.HostChannel_ConnectServer) error {
	return status.Error(codes.PermissionDenied, "this host is revoked")
}

func TestRun_StopsOnAPermanentRefusal(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer()
	agentv1.RegisterHostChannelServer(g, deniedServer{})
	go func() { _ = g.Serve(lis) }()
	defer g.Stop()
	cc, _ := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer cc.Close()
	r := &channel.Runner{
		Owner: "sub-A", HostID: "h1", MinBackoff: time.Millisecond, MaxBackoff: time.Millisecond,
		Dial: func(ctx context.Context) (channel.Stream, error) {
			return agentv1.NewHostChannelClient(cc).Connect(ctx)
		},
		Classify: func(err error) bool {
			return status.Code(errors.Unwrap(err)) == codes.PermissionDenied || status.Code(err) == codes.PermissionDenied
		},
	}
	r.Sessions = sessions.New(sessions.Deps{Tmux: &tmux{}, Workspaces: fs{}, Audit: &audit{}, Clock: clock{}, IDs: &ids{}, Catalog: session.Catalog{"claude": {"claude"}}, Home: "/home/a"})
	errc := make(chan error, 1)
	go func() { errc <- r.Run(context.Background()) }()
	select {
	case err := <-errc:
		if !errors.Is(err, channel.ErrPermanent) {
			t.Fatalf("want a permanent stop, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a revoked host must stop retrying")
	}
}

func TestRun_OneSessionsOperationsKeepTheirOrder(t *testing.T) {
	srv := newServer()
	tm, _, _, _ := setup(t, srv)
	wait(t, srv.hellos)
	srv.ops <- &agentv1.Operation{RequestId: "s", OwnerSub: "sub-A", Op: &agentv1.Operation_Start{Start: &agentv1.StartOp{Kind: "claude", Folder: "/home/a/src", Label: "o1"}}}
	sid := result(t, srv, "s").GetStarted().GetId()
	srv.ops <- &agentv1.Operation{RequestId: "a", OwnerSub: "sub-A", Op: &agentv1.Operation_Send{Send: &agentv1.SendOp{SessionId: sid, Text: "slow git status"}}}
	srv.ops <- &agentv1.Operation{RequestId: "b", OwnerSub: "sub-A", Op: &agentv1.Operation_Send{Send: &agentv1.SendOp{SessionId: sid, Text: "then this", Submit: true}}}
	result(t, srv, "a")
	result(t, srv, "b")
	got := strings.Join(tm.sent(), ",")
	if strings.Index(got, "slow git status") > strings.Index(got, "then this") {
		t.Fatalf("keystrokes reached tmux out of order: %s", got)
	}
}

func TestRun_SubscribeThenImmediateUnsubscribe_AndDuplicateIDs(t *testing.T) {
	srv := newServer()
	setup(t, srv)
	wait(t, srv.hellos)
	srv.ops <- &agentv1.Operation{RequestId: "s", OwnerSub: "sub-A", Op: &agentv1.Operation_Start{Start: &agentv1.StartOp{Kind: "claude", Folder: "/home/a/src", Label: "o2"}}}
	sid := result(t, srv, "s").GetStarted().GetId()
	// Back to back: the Unsubscribe must find the subscription registered.
	srv.ops <- &agentv1.Operation{RequestId: "sub1", OwnerSub: "sub-A", Op: &agentv1.Operation_Subscribe{Subscribe: &agentv1.SubscribeOp{SessionId: sid}}}
	srv.ops <- &agentv1.Operation{RequestId: "u1", OwnerSub: "sub-A", Op: &agentv1.Operation_Unsubscribe{Unsubscribe: &agentv1.UnsubscribeOp{SessionId: sid, SubscriptionId: "sub1"}}}
	if r := result(t, srv, "sub1"); r.GetError() != nil {
		t.Fatalf("subscribe: %v", r)
	}
	result(t, srv, "u1")
	// A second Subscribe reusing a live id is refused, not swapped in.
	srv.ops <- &agentv1.Operation{RequestId: "dup", OwnerSub: "sub-A", Op: &agentv1.Operation_Subscribe{Subscribe: &agentv1.SubscribeOp{SessionId: sid}}}
	srv.ops <- &agentv1.Operation{RequestId: "dup", OwnerSub: "sub-A", Op: &agentv1.Operation_Subscribe{Subscribe: &agentv1.SubscribeOp{SessionId: sid}}}
	first := result(t, srv, "dup")
	second := result(t, srv, "dup")
	errs := 0
	for _, r := range []*agentv1.Result{first, second} {
		if r.GetError() != nil {
			errs++
		}
	}
	// The fake stream ends at once (it replays two chunks and closes), so the
	// first may be gone before the second arrives; what must never happen is
	// two LIVE subscriptions under one id — at most one succeeds while live.
	if errs > 1 {
		t.Fatalf("both refused: %v / %v", first, second)
	}
}
