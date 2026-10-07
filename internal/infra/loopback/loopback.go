// Package loopback is the sign-in redirect target on 127.0.0.1: one request,
// state checked, a "you can close this tab" page, then it stops listening.
// The shape is sparkflow-sync's infra/auth/loopback.go.
package loopback

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// Listener is one sign-in attempt's redirect target.
type Listener struct {
	srv    *http.Server
	port   int
	result chan result
	once   sync.Once
	state  string
}

type result struct {
	code string
	err  error
}

// Start listens on 127.0.0.1:0 (never a wildcard) and accepts exactly one
// redirect carrying `state`.
func Start(state string) (*Listener, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("loopback listen: %w", err)
	}
	l := &Listener{port: ln.Addr().(*net.TCPAddr).Port, result: make(chan result, 1), state: state}
	mux := http.NewServeMux()
	mux.HandleFunc("/", l.serve)
	l.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second}
	go func() { _ = l.srv.Serve(ln) }()
	return l, nil
}

// RedirectURI is what the authorize request and the exchange both carry.
func (l *Listener) RedirectURI() string { return fmt.Sprintf("http://127.0.0.1:%d/", l.port) }

const page = `<!doctype html><meta charset="utf-8"><title>sparkflow-agentd</title>` +
	`<style>body{font:16px/1.5 system-ui;padding:3rem;color:#333}</style><h1>%s</h1><p>You can close this tab and return to the terminal.</p>`

func (l *Listener) serve(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if e := q.Get("error"); e != "" {
		_, _ = fmt.Fprintf(w, page, "Sign-in failed.")
		l.deliver(result{err: fmt.Errorf("sign-in failed: %s %s", e, q.Get("error_description"))})
		return
	}
	if q.Get("code") == "" {
		// A favicon request, a scanner: not ours, not an answer.
		http.Error(w, "no code", http.StatusBadRequest)
		return
	}
	if q.Get("state") != l.state {
		// Refused without ending the wait: a stray tab from an older attempt
		// must not abort this one.
		http.Error(w, "this sign-in belongs to another attempt", http.StatusBadRequest)
		return
	}
	_, _ = fmt.Fprintf(w, page, "You're signed in.")
	l.deliver(result{code: q.Get("code")})
}

func (l *Listener) deliver(r result) {
	l.once.Do(func() { l.result <- r })
}

// Wait returns the code, or the failure the IdP redirected with.
func (l *Listener) Wait(ctx context.Context) (string, error) {
	select {
	case r := <-l.result:
		return r.code, r.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// Close stops listening.
func (l *Listener) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := l.srv.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		_ = l.srv.Close()
	}
}
