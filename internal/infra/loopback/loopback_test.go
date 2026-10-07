package loopback_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/loopback"
)

func TestLoopback_OneCodeWithOurState(t *testing.T) {
	l, err := loopback.Start("S1")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if !strings.HasPrefix(l.RedirectURI(), "http://127.0.0.1:") {
		t.Fatalf("must bind loopback only: %s", l.RedirectURI())
	}
	get := func(q string) int {
		resp, err := http.Get(l.RedirectURI() + q)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := get("favicon.ico"); code != 400 {
		t.Errorf("no code: %d", code)
	}
	if code := get("?code=STALE&state=OLD"); code != 400 {
		t.Errorf("another attempt's state: %d", code)
	}
	if code := get("?code=GOOD&state=S1"); code != 200 {
		t.Errorf("ours: %d", code)
	}
	_ = get("?code=SECOND&state=S1")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	code, err := l.Wait(ctx)
	if err != nil || code != "GOOD" {
		t.Fatalf("the first valid redirect wins: %q %v", code, err)
	}
}

func TestLoopback_IdPErrorEndsTheWait(t *testing.T) {
	l, _ := loopback.Start("S1")
	defer l.Close()
	// A forged error without our state is ignored…
	if resp, err := http.Get(l.RedirectURI() + "?error=forged"); err == nil {
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Fatalf("a stateless error must be refused: %d", resp.StatusCode)
		}
	}
	resp, err := http.Get(l.RedirectURI() + "?error=access_denied&error_description=user+cancelled&state=S1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := l.Wait(ctx); err == nil || !strings.Contains(err.Error(), "access_denied") {
		t.Fatalf("want the IdP's error, got %v", err)
	}
}
