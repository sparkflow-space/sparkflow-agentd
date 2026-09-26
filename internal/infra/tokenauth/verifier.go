// Package tokenauth verifies the Zitadel access token that every RPC carries.
//
// The daemon AUTHENTICATES its caller and never authorises: whether this person
// may touch this project was decided upstream, by a service that knows about
// projects. What the daemon needs is a name it can put in the audit log and
// trust — hence a real token rather than a claimed identity in a field.
//
// The policy is deliberately the strict one: signature, expiry, ISSUER and
// AUDIENCE. This process runs other people's code on a host; a token minted for
// some other client, or by some other issuer, must not open it.
package tokenauth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	jwt "github.com/golang-jwt/jwt/v5"
)

// ErrUnauthenticated is all a caller ever learns. The reason is logged by the
// handler, not returned: a precise refusal is an oracle.
var ErrUnauthenticated = errors.New("agentd: unauthenticated")

type Verifier struct {
	jwks     keyfunc.Keyfunc
	issuer   string
	audience string
}

// New fetches the JWKS once and starts the background refresher.
//
// Misconfiguration is a BOOT ERROR, never a quiet downgrade — there is no
// degraded mode here, because the degraded mode is "run anybody's code".
//
// Two overrides are load-bearing, and both were learned the hard way elsewhere
// in this estate rather than here:
//
//   - keyfunc's default sets NoErrorReturnFirstHTTPReq, so plain construction
//     SUCCEEDS against an unreachable, 404ing or mistyped URL and hands back a
//     verifier holding an EMPTY key set. The process then boots green and every
//     token fails at runtime with a message pointing at the client rather than
//     at the config.
//   - a wrong-but-live URL (an IdP serves several JSON documents) answers 200
//     with no keys, which the override above cannot catch — so the key set is
//     read back and an empty one is refused.
func New(ctx context.Context, jwksURL, issuer, audience string, onRefreshError func(url string, err error)) (*Verifier, error) {
	switch {
	case jwksURL == "":
		return nil, fmt.Errorf("agentd: AUTH_JWKS_URL is required")
	case issuer == "":
		return nil, fmt.Errorf("agentd: AUTH_ISSUER is required")
	case audience == "":
		return nil, fmt.Errorf("agentd: AUTH_AUDIENCE is required")
	}
	returnFirstFetchError := false
	jwks, err := keyfunc.NewDefaultOverrideCtx(ctx, []string{jwksURL}, keyfunc.Override{
		NoErrorReturnFirstHTTPReq: &returnFirstFetchError,
		// Bounded: this runs before the listener opens, and a blackholed JWKS
		// would otherwise hold the boot for a minute with the port closed.
		HTTPTimeout: 5 * time.Second,
		RefreshErrorHandlerFunc: func(u string) func(context.Context, error) {
			return func(_ context.Context, err error) {
				if onRefreshError != nil {
					onRefreshError(u, err)
				}
			}
		},
	})
	if err != nil {
		return nil, fmt.Errorf("agentd: fetch jwks from %q: %w", jwksURL, err)
	}
	keys, err := jwks.VerificationKeySet(ctx)
	if err != nil {
		return nil, fmt.Errorf("agentd: reading the jwks from %q: %w", jwksURL, err)
	}
	if len(keys.Keys) == 0 {
		return nil, fmt.Errorf("agentd: the jwks at %q served no verification keys; "+
			"check the URL points at the IdP's keys document", jwksURL)
	}
	return &Verifier{jwks: jwks, issuer: issuer, audience: audience}, nil
}

// Actor is the verified human behind a call: the token's subject, plus the
// email when the token carries one. Only `Subject` is guaranteed.
type Actor struct {
	Subject string
	Email   string
}

// Verify checks the token and returns who it belongs to.
func (v *Verifier) Verify(raw string) (Actor, error) {
	tok, err := jwt.Parse(raw, v.jwks.Keyfunc,
		jwt.WithIssuer(v.issuer),
		jwt.WithAudience(v.audience),
		jwt.WithExpirationRequired(),
		// Signing methods are pinned: without this, a token could name a method
		// the key set happens to satisfy.
		jwt.WithValidMethods([]string{"RS256", "RS384", "RS512"}),
	)
	if err != nil || !tok.Valid {
		return Actor{}, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	claims, ok := tok.Claims.(jwt.MapClaims)
	if !ok {
		return Actor{}, ErrUnauthenticated
	}
	sub, _ := claims["sub"].(string)
	if sub == "" {
		return Actor{}, fmt.Errorf("%w: no subject", ErrUnauthenticated)
	}
	email, _ := claims["email"].(string)
	return Actor{Subject: sub, Email: email}, nil
}

// Name is what goes in the audit log: the email when there is one, because a
// human reading the log six months later knows an address and does not know a
// Zitadel subject id.
func (a Actor) Name() string {
	if a.Email != "" {
		return a.Email
	}
	return a.Subject
}
