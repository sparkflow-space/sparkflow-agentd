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
	tk := NewTokens(c, zitadel.New(), func(h host.Credentials) error { saved = append(saved, h); return nil }, nil)
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

	// A save that fails keeps the rotation IN MEMORY and retries it: the IdP
	// has already invalidated the old refresh token, so dropping the new one
	// would end the enrolment over a full disk.
	failing := true
	var reported []error
	var lastSaved host.Credentials
	tk2 := NewTokens(host.Credentials{TokenEndpoint: idp.URL, RefreshToken: "RT9", Deployment: host.Deployment{ClientID: "c"}}, zitadel.New(),
		func(h host.Credentials) error {
			if failing {
				return errors.New("disk full")
			}
			lastSaved = h
			return nil
		}, nil)
	tk2.OnSaveError = func(err error) { reported = append(reported, err) }
	if a, err := tk2.Access(context.Background()); err != nil || a == "" {
		t.Fatalf("a failed save must still hand out the token: %q %v", a, err)
	}
	if len(reported) != 1 {
		t.Fatalf("the failure must be reported: %v", reported)
	}
	failing = false
	_, _ = tk2.Access(context.Background())
	if lastSaved.RefreshToken == "" || lastSaved.RefreshToken == "RT9" {
		t.Fatalf("the kept rotation must be written on the next call: %+v", lastSaved)
	}

	tk3 := NewTokens(host.Credentials{TokenEndpoint: idp.URL, RefreshToken: "dead", Deployment: host.Deployment{ClientID: "c"}}, zitadel.New(),
		func(host.Credentials) error { return nil }, nil)
	_, err = tk3.Access(context.Background())
	if !Permanent(err) {
		t.Fatalf("a dead sign-in is permanent: %v", err)
	}
	_ = time.Second
}

func TestTokens_AStaleProcessNeverOverwritesANewEnrolment(t *testing.T) {
	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "AT", "refresh_token": "RTx", "expires_in": 3600})
	}))
	defer idp.Close()
	onDisk := &host.Credentials{HostID: "h-NEW", OwnerSub: "sub-B"} // init ran again
	writes := 0
	tk := NewTokens(host.Credentials{HostID: "h-old", OwnerSub: "sub-A", TokenEndpoint: idp.URL, RefreshToken: "RT", Deployment: host.Deployment{ClientID: "c"}},
		zitadel.New(), func(host.Credentials) error { writes++; return nil }, func() (*host.Credentials, error) { return onDisk, nil })
	if _, err := tk.Access(context.Background()); !errors.Is(err, ErrReEnrolled) || !Permanent(err) {
		t.Fatalf("a re-enrolled device must stop the stale process: %v", err)
	}
	if writes != 0 {
		t.Fatal("the stale process wrote its old sign-in over the new enrolment")
	}
}

func TestPermanent(t *testing.T) {
	// A proxy's 403/404 (ingress or oauth2-proxy redeploying) arrives with the
	// same codes; it must be retried, not taken as "revoked".
	for _, c := range []codes.Code{codes.PermissionDenied, codes.NotFound} {
		if Permanent(status.Error(c, "unexpected HTTP status code received from server: 403 (Forbidden)")) {
			t.Errorf("a proxy's %v must be retried", c)
		}
	}
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
