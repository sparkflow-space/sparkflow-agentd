package hostchannel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/host"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/zitadel"
)

func TestTarget(t *testing.T) {
	ok := map[string][2]any{
		"https://sparkflow.ddns.net":       {"sparkflow.ddns.net:443", true},
		"https://sparkflow.ddns.net:8443/": {"sparkflow.ddns.net:8443", true},
		"http://127.0.0.1:50052":           {"127.0.0.1:50052", false},
		"http://localhost:50052":           {"localhost:50052", false},
	}
	for in, want := range ok {
		got, secure, err := Target(in)
		if err != nil || got != want[0] || secure != want[1] {
			t.Errorf("Target(%q) = %q %v %v", in, got, secure, err)
		}
	}
	for _, bad := range []string{"http://sparkflow.ddns.net", "http://10.0.0.5:80", "ftp://x", "nonsense"} {
		if _, _, err := Target(bad); err == nil {
			t.Errorf("Target(%q) must be refused — a bearer token never crosses a network in the clear", bad)
		}
	}
}

func TestTokens_RefreshPersistsBeforeUse(t *testing.T) {
	calls := 0
	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_ = r.ParseForm()
		if r.PostForm.Get("refresh_token") == "dead" {
			w.WriteHeader(400)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_grant"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": fmt.Sprintf("AT%d", calls), "refresh_token": fmt.Sprintf("RT%d", calls), "expires_in": 3600})
	}))
	defer idp.Close()

	var saved []host.Credentials
	c := host.Credentials{TokenEndpoint: idp.URL, RefreshToken: "RT0", Deployment: host.Deployment{ClientID: "c"}}
	tk := NewTokens(c, zitadel.New(), func(h host.Credentials) error { saved = append(saved, h); return nil })
	a1, err := tk.Access(context.Background())
	if err != nil || a1 != "AT1" {
		t.Fatalf("%q %v", a1, err)
	}
	if len(saved) != 1 || saved[0].RefreshToken != "RT1" {
		t.Fatalf("the rotated refresh token must be persisted: %+v", saved)
	}
	if a2, _ := tk.Access(context.Background()); a2 != "AT1" || calls != 1 {
		t.Errorf("a valid token must be reused, not refreshed again (calls=%d)", calls)
	}

	// A save that fails must not hand out the new token: the rotation would
	// be lost at the next restart.
	tk2 := NewTokens(host.Credentials{TokenEndpoint: idp.URL, RefreshToken: "RT9", Deployment: host.Deployment{ClientID: "c"}}, zitadel.New(),
		func(host.Credentials) error { return errors.New("disk full") })
	if _, err := tk2.Access(context.Background()); err == nil {
		t.Fatal("want the save error")
	}

	tk3 := NewTokens(host.Credentials{TokenEndpoint: idp.URL, RefreshToken: "dead", Deployment: host.Deployment{ClientID: "c"}}, zitadel.New(),
		func(host.Credentials) error { return nil })
	_, err = tk3.Access(context.Background())
	if !Permanent(err) {
		t.Fatalf("a dead sign-in is permanent: %v", err)
	}
	_ = time.Second
}

func TestPermanent(t *testing.T) {
	for _, c := range []codes.Code{codes.PermissionDenied, codes.NotFound} {
		if !Permanent(fmt.Errorf("connect: %w", status.Error(c, "x"))) {
			t.Errorf("%v must be permanent", c)
		}
	}
	for _, c := range []codes.Code{codes.Unavailable, codes.Unauthenticated, codes.DeadlineExceeded, codes.Internal} {
		if Permanent(status.Error(c, "x")) {
			t.Errorf("%v must be retried", c)
		}
	}
}
