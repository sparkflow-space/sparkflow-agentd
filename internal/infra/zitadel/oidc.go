// Package zitadel is the daemon's OIDC client: authorization code + PKCE (S256)
// for a PUBLIC native client (no secret), and refresh-token rotation. The
// shape is sparkflow-sync's login (internal/application/login.go there),
// which has run against this IdP since SYNC-014 — endpoints come from
// discovery, never from a hardcoded path.
package zitadel

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Endpoints are the two the flow needs from the discovery document.
type Endpoints struct {
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
}

// Tokens is one token-endpoint answer, plus who it is for.
type Tokens struct {
	AccessToken  string
	RefreshToken string
	ExpiresAt    time.Time
	// Sub and Email come from the id_token. They are read, not verified: the
	// SERVER verifies every access token; locally they name the owner this
	// daemon will serve, and a wrong value can only make it refuse work.
	Sub   string
	Email string
}

// ErrSignInAgain is a refresh token the IdP no longer accepts — revoked,
// expired, or rotated by another process. Only a new `init` fixes it.
var ErrSignInAgain = errors.New("the stored sign-in is no longer valid — run `sparkflow-agentd init` again")

// Client talks to one issuer.
type Client struct {
	HTTP *http.Client
	Now  func() time.Time
}

func New() *Client {
	return &Client{HTTP: &http.Client{Timeout: 15 * time.Second}, Now: time.Now}
}

// Discover reads <issuer>/.well-known/openid-configuration.
func (c *Client) Discover(ctx context.Context, issuer string) (Endpoints, error) {
	issuer = strings.TrimRight(issuer, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, issuer+"/.well-known/openid-configuration", nil)
	if err != nil {
		return Endpoints{}, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Endpoints{}, fmt.Errorf("OIDC discovery: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return Endpoints{}, fmt.Errorf("OIDC discovery: %s at %s", resp.Status, issuer)
	}
	var e Endpoints
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&e); err != nil {
		return Endpoints{}, fmt.Errorf("OIDC discovery: %w", err)
	}
	if e.AuthorizationEndpoint == "" || e.TokenEndpoint == "" {
		return Endpoints{}, errors.New("OIDC discovery: the document has no authorization or token endpoint")
	}
	return e, nil
}

// PKCE is one sign-in attempt's secrets. The verifier never leaves this
// process; that is what makes a code seen on a screen useless to anyone else.
type PKCE struct {
	Verifier  string
	Challenge string
	State     string
}

func NewPKCE() (PKCE, error) {
	v, err := randomURLSafe(64)
	if err != nil {
		return PKCE{}, err
	}
	s, err := randomURLSafe(16)
	if err != nil {
		return PKCE{}, err
	}
	sum := sha256.Sum256([]byte(v))
	return PKCE{Verifier: v, Challenge: base64.RawURLEncoding.EncodeToString(sum[:]), State: s}, nil
}

// AuthorizeURL builds the link the person opens. `offline_access` is what
// gets the long-lived refresh token; `email` lets init say who signed in.
func AuthorizeURL(authEndpoint, clientID, redirectURI string, p PKCE) string {
	q := url.Values{}
	q.Set("client_id", clientID)
	q.Set("response_type", "code")
	q.Set("redirect_uri", redirectURI)
	q.Set("scope", "openid offline_access email profile")
	q.Set("code_challenge", p.Challenge)
	q.Set("code_challenge_method", "S256")
	q.Set("state", p.State)
	sep := "?"
	if strings.Contains(authEndpoint, "?") {
		sep = "&"
	}
	return authEndpoint + sep + q.Encode()
}

// Exchange redeems a code. redirectURI must be byte-identical to the one in
// the authorize request.
func (c *Client) Exchange(ctx context.Context, tokenEndpoint, clientID, code, verifier, redirectURI string) (Tokens, error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("client_id", clientID)
	form.Set("code", code)
	form.Set("redirect_uri", redirectURI)
	form.Set("code_verifier", verifier)
	t, err := c.token(ctx, tokenEndpoint, form)
	if errors.Is(err, ErrSignInAgain) {
		return Tokens{}, errors.New("the sign-in code was not accepted (it is single-use and expires quickly) — run `sparkflow-agentd init` again")
	}
	return t, err
}

// Refresh exchanges the refresh token. Zitadel ROTATES it: the answer carries
// a new one, and the caller must persist it before using the access token, or
// the next refresh fails with invalid_grant.
func (c *Client) Refresh(ctx context.Context, tokenEndpoint, clientID, refreshToken string) (Tokens, error) {
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("client_id", clientID)
	form.Set("refresh_token", refreshToken)
	t, err := c.token(ctx, tokenEndpoint, form)
	if err != nil {
		return Tokens{}, err
	}
	if t.RefreshToken == "" {
		t.RefreshToken = refreshToken // an IdP that does not rotate
	}
	return t, nil
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	IDToken      string `json:"id_token"`
}

func (c *Client) token(ctx context.Context, endpoint string, form url.Values) (Tokens, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return Tokens{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Tokens{}, fmt.Errorf("token endpoint: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		var e struct {
			Error string `json:"error"`
			Desc  string `json:"error_description"`
		}
		_ = json.Unmarshal(body, &e)
		if e.Error == "invalid_grant" {
			return Tokens{}, ErrSignInAgain
		}
		// The error text only: a token endpoint never echoes a secret in an
		// error, and the body is not logged anyway.
		return Tokens{}, fmt.Errorf("token endpoint %d: %s %s", resp.StatusCode, e.Error, e.Desc)
	}
	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return Tokens{}, fmt.Errorf("token endpoint: decode: %w", err)
	}
	if tr.AccessToken == "" {
		return Tokens{}, errors.New("token endpoint: no access token in the answer")
	}
	t := Tokens{AccessToken: tr.AccessToken, RefreshToken: tr.RefreshToken}
	if tr.ExpiresIn > 0 {
		t.ExpiresAt = c.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)
	}
	if tr.IDToken != "" {
		t.Sub, t.Email = claims(tr.IDToken)
	}
	return t, nil
}

// claims reads sub and email from a JWT payload without verifying it (see
// Tokens.Sub for why that is enough here).
func claims(jwt string) (sub, email string) {
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		return "", ""
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", ""
	}
	var c struct {
		Sub   string `json:"sub"`
		Email string `json:"email"`
	}
	_ = json.Unmarshal(raw, &c)
	return c.Sub, c.Email
}

func randomURLSafe(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// ParsePasted turns what a person pasted — the whole address from the
// browser's address bar, or just the code — into a code. A pasted ADDRESS must
// carry this attempt's state: the IdP always echoes it, so one without it (or
// with another) is not from this sign-in. A bare code has no state to check;
// it is worthless without this process's PKCE verifier anyway.
func ParsePasted(pasted, wantState string) (string, error) {
	s := strings.TrimSpace(pasted)
	if s == "" {
		return "", errors.New("nothing was pasted")
	}
	if strings.Contains(s, "://") || strings.HasPrefix(s, "/") || strings.Contains(s, "?") {
		u, err := url.Parse(s)
		if err != nil {
			return "", fmt.Errorf("that is not an address: %w", err)
		}
		q := u.Query()
		if q.Get("state") != wantState {
			return "", errors.New("that address does not belong to this sign-in attempt (state missing or different)")
		}
		if e := q.Get("error"); e != "" {
			return "", fmt.Errorf("sign-in failed: %s %s", e, q.Get("error_description"))
		}
		code := q.Get("code")
		if code == "" {
			return "", errors.New("the address has no code in it")
		}
		return code, nil
	}
	if strings.ContainsAny(s, " \t") {
		return "", errors.New("a code has no spaces")
	}
	return s, nil
}
