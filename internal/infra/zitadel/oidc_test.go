package zitadel_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/zitadel"
)

func idToken(sub, email string) string {
	p, _ := json.Marshal(map[string]string{"sub": sub, "email": email})
	return "e30." + base64.RawURLEncoding.EncodeToString(p) + ".sig"
}

// fakeIdP is an issuer with discovery and a token endpoint.
func fakeIdP(t *testing.T, onToken func(url.Values) (int, any)) *httptest.Server {
	t.Helper()
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]string{
				"authorization_endpoint": srv.URL + "/oauth/v2/authorize",
				"token_endpoint":         srv.URL + "/oauth/v2/token",
			})
		case "/oauth/v2/token":
			_ = r.ParseForm()
			code, body := onToken(r.PostForm)
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(body)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestFlow_DiscoverAuthorizeExchange(t *testing.T) {
	var got url.Values
	idp := fakeIdP(t, func(f url.Values) (int, any) {
		got = f
		return 200, map[string]any{"access_token": "AT", "refresh_token": "RT", "expires_in": 3600, "id_token": idToken("sub-A", "a@x")}
	})
	c := zitadel.New()
	ep, err := c.Discover(context.Background(), idp.URL+"/")
	if err != nil {
		t.Fatal(err)
	}
	p, _ := zitadel.NewPKCE()
	u, _ := url.Parse(zitadel.AuthorizeURL(ep.AuthorizationEndpoint, "client-1", "http://127.0.0.1:4242/", p))
	q := u.Query()
	if q.Get("code_challenge_method") != "S256" || q.Get("code_challenge") != p.Challenge || q.Get("state") != p.State ||
		!strings.Contains(q.Get("scope"), "offline_access") || q.Get("redirect_uri") != "http://127.0.0.1:4242/" {
		t.Fatalf("authorize URL: %v", q)
	}
	if strings.Contains(u.String(), p.Verifier) {
		t.Fatal("the PKCE verifier must never be in the URL")
	}
	tok, err := c.Exchange(context.Background(), ep.TokenEndpoint, "client-1", "CODE", p.Verifier, "http://127.0.0.1:4242/")
	if err != nil {
		t.Fatal(err)
	}
	if tok.AccessToken != "AT" || tok.RefreshToken != "RT" || tok.Sub != "sub-A" || tok.Email != "a@x" || tok.ExpiresAt.IsZero() {
		t.Fatalf("tokens: %+v", tok)
	}
	if got.Get("code_verifier") != p.Verifier || got.Get("redirect_uri") != "http://127.0.0.1:4242/" || got.Get("grant_type") != "authorization_code" {
		t.Fatalf("exchange form: %v", got)
	}
}

func TestRefresh_RotationAndInvalidGrant(t *testing.T) {
	rotate := true
	idp := fakeIdP(t, func(f url.Values) (int, any) {
		if f.Get("refresh_token") == "dead" {
			return 400, map[string]string{"error": "invalid_grant", "error_description": "token revoked"}
		}
		if rotate {
			return 200, map[string]any{"access_token": "AT2", "refresh_token": "RT2", "expires_in": 60}
		}
		return 200, map[string]any{"access_token": "AT3", "expires_in": 60}
	})
	c := zitadel.New()
	tok, err := c.Refresh(context.Background(), idp.URL+"/oauth/v2/token", "c", "RT1")
	if err != nil || tok.RefreshToken != "RT2" {
		t.Fatalf("rotation: %+v %v", tok, err)
	}
	rotate = false
	if tok, _ = c.Refresh(context.Background(), idp.URL+"/oauth/v2/token", "c", "RT2"); tok.RefreshToken != "RT2" {
		t.Errorf("a non-rotating answer keeps the old refresh token: %+v", tok)
	}
	if _, err := c.Refresh(context.Background(), idp.URL+"/oauth/v2/token", "c", "dead"); !errors.Is(err, zitadel.ErrSignInAgain) {
		t.Fatalf("invalid_grant must say 'sign in again': %v", err)
	}
	if _, err := c.Exchange(context.Background(), idp.URL+"/oauth/v2/token", "c", "x", "v", "r"); err != nil && strings.Contains(err.Error(), "RT") {
		t.Fatal("errors must not carry tokens")
	}
}

func TestParsePasted(t *testing.T) {
	ok := map[string]string{
		"abc123":     "abc123",
		"  abc123\n": "abc123",
		"http://127.0.0.1:43117/?code=XYZ&state=S1":           "XYZ",
		"https://sparkflow.ddns.net/agentd/code?code=XYZ":     "XYZ",
		"http://127.0.0.1:43117/callback?state=S1&code=Q%2BW": "Q+W",
	}
	for in, want := range ok {
		if got, err := zitadel.ParsePasted(in, "S1"); err != nil || got != want {
			t.Errorf("ParsePasted(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "http://127.0.0.1/?code=X&state=OTHER", "http://127.0.0.1/?state=S1", "two words", "http://127.0.0.1/?error=access_denied"} {
		if _, err := zitadel.ParsePasted(bad, "S1"); err == nil {
			t.Errorf("ParsePasted(%q) must fail", bad)
		}
	}
}
