package grpc_test

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
	handler "github.com/sparkflow-space/sparkflow-agentd/internal/handler/grpc"
)

type stubVerifier struct {
	actor session.Actor
	err   error
	saw   string
}

func (s *stubVerifier) Verify(raw string) (session.Actor, error) {
	s.saw = raw
	return s.actor, s.err
}

type capturedLog struct{ reasons []string }

func (c *capturedLog) Refused(reason string, _ error) { c.reasons = append(c.reasons, reason) }

func unaryWith(t *testing.T, v handler.Verifier) (grpc.UnaryServerInterceptor, *capturedLog) {
	t.Helper()
	log := &capturedLog{}
	u, _ := handler.Interceptors(v, log)
	return u, log
}

func ctxWith(headers ...string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(headers...))
}

func TestInterceptor_RefusesACallWithNoToken(t *testing.T) {
	u, log := unaryWith(t, &stubVerifier{})
	called := false
	_, err := u(context.Background(), nil, nil, func(context.Context, any) (any, error) {
		called = true
		return nil, nil
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %v, want Unauthenticated", status.Code(err))
	}
	if called {
		t.Fatal("the handler must not run for an unauthenticated call")
	}
	if len(log.reasons) != 1 {
		t.Errorf("the refusal must be logged; got %v", log.reasons)
	}
}

func TestInterceptor_RefusesABadTokenWithoutSayingWhy(t *testing.T) {
	v := &stubVerifier{err: errors.New("token is expired by 3h and the audience is wrong")}
	u, log := unaryWith(t, v)

	_, err := u(ctxWith("authorization", "Bearer abc"), nil, nil,
		func(context.Context, any) (any, error) { return nil, nil })

	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated {
		t.Fatalf("code = %v, want Unauthenticated", st.Code())
	}
	// A precise refusal is an oracle for whoever is probing: the caller learns
	// "no", the operator learns why from the log.
	if st.Message() != "unauthenticated" {
		t.Errorf("message = %q; it must not leak the reason", st.Message())
	}
	if len(log.reasons) == 0 {
		t.Error("the reason must reach the log")
	}
}

func TestInterceptor_AcceptsBearerCaseInsensitivelyAndPassesTheRawToken(t *testing.T) {
	v := &stubVerifier{actor: session.Actor{Subject: "u1", Email: "boris@example.test"}}
	u, _ := unaryWith(t, v)

	for _, header := range []string{"Bearer tok-123", "bearer tok-123", "BEARER tok-123"} {
		v.saw = ""
		if _, err := u(ctxWith("authorization", header), nil, nil,
			func(context.Context, any) (any, error) { return "ok", nil }); err != nil {
			t.Fatalf("%q: %v", header, err)
		}
		if v.saw != "tok-123" {
			t.Errorf("%q: verifier saw %q, want the raw token", header, v.saw)
		}
	}
}

func TestInterceptor_RefusesANonBearerScheme(t *testing.T) {
	u, _ := unaryWith(t, &stubVerifier{})
	for _, header := range []string{"Basic abc", "tok-123", "Bearer", ""} {
		_, err := u(ctxWith("authorization", header), nil, nil,
			func(context.Context, any) (any, error) { return nil, nil })
		if status.Code(err) != codes.Unauthenticated {
			t.Errorf("header %q: code = %v, want Unauthenticated", header, status.Code(err))
		}
	}
}
