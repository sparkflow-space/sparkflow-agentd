package tokenauth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"

	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/tokenauth"
)

const (
	issuer   = "https://login.example.test"
	audience = "project-42"
)

func TestNew_RefusesAnUnreachableJWKS(t *testing.T) {
	// The trap this test exists for: keyfunc's default construction SUCCEEDS
	// against a dead URL and yields a verifier with an empty key set, so the
	// daemon would boot green and refuse every token at runtime.
	_, err := tokenauth.New(context.Background(), "http://127.0.0.1:1/nope", issuer, audience, nil)
	if err == nil {
		t.Fatal("an unreachable JWKS must be a BOOT error, not a quiet empty key set")
	}
}

func TestNew_RefusesAJWKSWithNoKeys(t *testing.T) {
	// A wrong-but-live URL answers 200 with a valid JSON document that has no
	// keys in it — which the first override cannot catch.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"keys":[]}`))
	}))
	defer srv.Close()

	if _, err := tokenauth.New(context.Background(), srv.URL, issuer, audience, nil); err == nil {
		t.Fatal("a key set with zero keys must be refused at boot")
	}
}

func TestNew_RequiresEveryArgument(t *testing.T) {
	for _, tc := range []struct{ url, iss, aud string }{
		{"", issuer, audience}, {"http://x", "", audience}, {"http://x", issuer, ""},
	} {
		if _, err := tokenauth.New(context.Background(), tc.url, tc.iss, tc.aud, nil); err == nil {
			t.Errorf("New(%q,%q,%q) must be refused", tc.url, tc.iss, tc.aud)
		}
	}
}

func TestVerify(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "RSA", "kid": "k1", "alg": "RS256", "use": "sig",
			"n": b64(key.N.Bytes()), "e": b64(big.NewInt(int64(key.E)).Bytes()),
		}}})
	}))
	defer srv.Close()

	v, err := tokenauth.New(context.Background(), srv.URL, issuer, audience, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	sign := func(claims jwt.MapClaims) string {
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		tok.Header["kid"] = "k1"
		s, err := tok.SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	base := func() jwt.MapClaims {
		return jwt.MapClaims{
			"iss": issuer, "aud": audience, "sub": "user-1",
			"email": "boris@example.test",
			"exp":   time.Now().Add(time.Hour).Unix(),
		}
	}

	t.Run("a good token names the human", func(t *testing.T) {
		actor, err := v.Verify(sign(base()))
		if err != nil {
			t.Fatalf("Verify: %v", err)
		}
		if actor.Subject != "user-1" || actor.Name() != "boris@example.test" {
			t.Fatalf("actor = %+v; the audit log needs an address a human recognises", actor)
		}
	})

	refusals := map[string]func(jwt.MapClaims){
		"wrong audience":   func(c jwt.MapClaims) { c["aud"] = "some-other-client" },
		"wrong issuer":     func(c jwt.MapClaims) { c["iss"] = "https://evil.test" },
		"expired":          func(c jwt.MapClaims) { c["exp"] = time.Now().Add(-time.Minute).Unix() },
		"no subject":       func(c jwt.MapClaims) { delete(c, "sub") },
		"no expiry at all": func(c jwt.MapClaims) { delete(c, "exp") },
	}
	for name, mutate := range refusals {
		t.Run(name+" is refused", func(t *testing.T) {
			c := base()
			mutate(c)
			if _, err := v.Verify(sign(c)); !errors.Is(err, tokenauth.ErrUnauthenticated) {
				t.Fatalf("err = %v, want ErrUnauthenticated", err)
			}
		})
	}

	t.Run("a token signed by another key is refused", func(t *testing.T) {
		other, _ := rsa.GenerateKey(rand.Reader, 2048)
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, base())
		tok.Header["kid"] = "k1" // claims our kid, signed by someone else
		s, _ := tok.SignedString(other)
		if _, err := v.Verify(s); err == nil {
			t.Fatal("a forged signature must not verify")
		}
	})

	t.Run("alg none is refused", func(t *testing.T) {
		tok := jwt.NewWithClaims(jwt.SigningMethodNone, base())
		s, _ := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
		if _, err := v.Verify(s); err == nil {
			t.Fatal("the oldest JWT trick in the book must not work")
		}
	})
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
