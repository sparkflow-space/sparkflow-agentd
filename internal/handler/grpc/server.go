// Package grpc is the daemon's delivery layer: it translates AgentDaemon RPCs
// into use-case calls and nothing more. No tmux, no policy.
package grpc

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/sparkflow-space/sparkflow-agentd/internal/application/sessions"
	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
	agentdv1 "github.com/sparkflow-space/sparkflow-agentd/internal/generated/agentd/v1"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/tokenauth"
)

// Verifier is the port the interceptors need. Defined here, at the consumer,
// so the handler can be tested with a stub.
type Verifier interface {
	Verify(raw string) (tokenauth.Actor, error)
}

// Logger keeps the handler free of a logging library.
type Logger interface {
	Refused(reason string, err error)
}

type Server struct {
	agentdv1.UnimplementedAgentDaemonServer
	svc *sessions.Service
}

func NewServer(svc *sessions.Service) *Server { return &Server{svc: svc} }

type actorKey struct{}

// Interceptors authenticate every call. They are the ONLY place a token is
// read; below this line an actor is a verified fact.
func Interceptors(v Verifier, log Logger) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	auth := func(ctx context.Context) (context.Context, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		var raw string
		for _, h := range md.Get("authorization") {
			if len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
				raw = h[7:]
				break
			}
		}
		if raw == "" {
			log.Refused("no bearer token", nil)
			return nil, status.Error(codes.Unauthenticated, "unauthenticated")
		}
		actor, err := v.Verify(raw)
		if err != nil {
			// The reason is logged, never returned: a precise refusal is an
			// oracle for whoever is probing.
			log.Refused("token rejected", err)
			return nil, status.Error(codes.Unauthenticated, "unauthenticated")
		}
		return context.WithValue(ctx, actorKey{}, actor), nil
	}

	unary := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := auth(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := auth(ss.Context())
		if err != nil {
			return err
		}
		return handler(srv, wrapped{ServerStream: ss, ctx: ctx})
	}
	return unary, stream
}

type wrapped struct {
	grpc.ServerStream
	ctx context.Context
}

func (w wrapped) Context() context.Context { return w.ctx }

func actorOf(ctx context.Context) string {
	if a, ok := ctx.Value(actorKey{}).(tokenauth.Actor); ok {
		return a.Name()
	}
	return "" // unreachable behind the interceptor; empty is never authorised upstream
}

func (s *Server) Start(ctx context.Context, req *agentdv1.StartRequest) (*agentdv1.StartResponse, error) {
	sess, err := s.svc.Start(ctx, actorOf(ctx), sessions.StartParams{
		Kind:      req.GetKind(),
		Workspace: req.GetWorkspace(),
		Env:       req.GetEnv(),
		Cols:      int(req.GetCols()),
		Rows:      int(req.GetRows()),
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return &agentdv1.StartResponse{Session: toProto(sess)}, nil
}

func (s *Server) Send(ctx context.Context, req *agentdv1.SendRequest) (*agentdv1.SendResponse, error) {
	if err := s.svc.Send(ctx, actorOf(ctx), req.GetSessionId(), req.GetText(), req.GetSubmit()); err != nil {
		return nil, mapErr(err)
	}
	return &agentdv1.SendResponse{}, nil
}

func (s *Server) Snapshot(ctx context.Context, req *agentdv1.SnapshotRequest) (*agentdv1.SnapshotResponse, error) {
	text, err := s.svc.Snapshot(ctx, req.GetSessionId(), int(req.GetLines()))
	if err != nil {
		return nil, mapErr(err)
	}
	return &agentdv1.SnapshotResponse{Text: text}, nil
}

func (s *Server) Signal(ctx context.Context, req *agentdv1.SignalRequest) (*agentdv1.SignalResponse, error) {
	if err := s.svc.Signal(ctx, actorOf(ctx), req.GetSessionId(), fromProtoSignal(req.GetSignal())); err != nil {
		return nil, mapErr(err)
	}
	return &agentdv1.SignalResponse{}, nil
}

func (s *Server) List(context.Context, *agentdv1.ListRequest) (*agentdv1.ListResponse, error) {
	all := s.svc.List()
	out := make([]*agentdv1.Session, 0, len(all))
	for _, x := range all {
		out = append(out, toProto(x))
	}
	return &agentdv1.ListResponse{Sessions: out}, nil
}

func (s *Server) Stream(req *agentdv1.StreamRequest, srv grpc.ServerStreamingServer[agentdv1.OutputChunk]) error {
	ctx := srv.Context()
	ch, err := s.svc.Subscribe(ctx, req.GetSessionId(), req.GetFromSeq())
	if err != nil {
		return mapErr(err)
	}
	for {
		select {
		case ck, ok := <-ch:
			if !ok {
				return nil
			}
			if err := srv.Send(&agentdv1.OutputChunk{
				Seq: ck.Seq, Data: ck.Data, AtUnixNano: ck.At.UnixNano(),
			}); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// mapErr keeps domain sentinels out of the wire and gives the caller a code it
// can act on. An unexpected error stays Internal and says nothing.
func mapErr(err error) error {
	switch {
	case errors.Is(err, session.ErrNotFound):
		return status.Error(codes.NotFound, "no such session")
	case errors.Is(err, session.ErrUnknownKind):
		return status.Error(codes.InvalidArgument, "kind is not allowed")
	case errors.Is(err, session.ErrWorkspaceEscapes), errors.Is(err, session.ErrWorkspaceEmpty):
		return status.Error(codes.InvalidArgument, "workspace is not permitted")
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

func toProto(s *session.Session) *agentdv1.Session {
	return &agentdv1.Session{
		Id: s.ID, Kind: s.Kind, Workspace: s.Workspace, TmuxName: s.TmuxName,
		State:            agentdv1.State(s.State),
		CreatedAtUnix:    s.CreatedAt.Unix(),
		LastActivityUnix: s.LastActivity.Unix(),
		Cols:             int32(s.Cols), Rows: int32(s.Rows),
	}
}

func fromProtoSignal(s agentdv1.Signal_) session.Signal {
	switch s {
	case agentdv1.Signal__SIGNAL_INT:
		return session.SignalInt
	case agentdv1.Signal__SIGNAL_TERM:
		return session.SignalTerm
	case agentdv1.Signal__SIGNAL_KILL:
		return session.SignalKill
	default:
		return session.Signal(0) // refused by the use case, not silently defaulted
	}
}
