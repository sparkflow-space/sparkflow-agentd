package host

import (
	"context"
	"time"
)

// The ports `init` reaches the world through. The composition root adapts the
// infra packages to them.

// Attempt is one sign-in's PKCE secrets and state.
type Attempt struct {
	Verifier, Challenge, State string
}

// SignIn is the outcome of a code exchange.
type SignIn struct {
	AccessToken, RefreshToken string
	ExpiresAt                 time.Time
	Sub, Email                string
}

// IdP is the OIDC provider.
type IdP interface {
	Discover(ctx context.Context, issuer string) (authEndpoint, tokenEndpoint string, err error)
	NewAttempt() (Attempt, error)
	AuthorizeURL(authEndpoint, clientID, redirectURI string, a Attempt) string
	Exchange(ctx context.Context, tokenEndpoint, clientID, code, verifier, redirectURI string) (SignIn, error)
	// ParsePasted turns a pasted address or code into a code.
	ParsePasted(pasted, state string) (string, error)
}

// Redirect is a started loopback redirect target.
type Redirect interface {
	URI() string
	Wait(ctx context.Context) (code string, err error)
	Close()
}

// Redirects starts loopback targets.
type Redirects interface {
	Start(state string) (Redirect, error)
}

// Prompter is the terminal. Line reads the next line typed, or returns when
// ctx ends WITHOUT consuming input — so a line typed after the browser won is
// still there for the next question.
type Prompter interface {
	Say(text string)
	Line(ctx context.Context) (string, error)
}

// Browser opens a URL, best effort.
type Browser interface{ Open(url string) error }

// Machine describes this device.
type Machine interface {
	OSName() string
	Country() string
	Arch() string
	Kinds() []string
}

// Enrolment is what RegisterHost is sent.
type Enrolment struct {
	HostID, Name, OS, Arch, Version string
	Kinds                           []string
}

// Registrar registers the device with agent-sessions.
type Registrar interface {
	Register(ctx context.Context, d Deployment, accessToken string, e Enrolment) (hostID, name string, err error)
}

// CredentialStore keeps Credentials. Load returns (nil, nil) when the device
// is not enrolled.
type CredentialStore interface {
	Load() (*Credentials, error)
	Save(Credentials) error
	Where() string
}

// ServiceResult is what installing the user service did.
type ServiceResult struct {
	Path    string
	Started bool
	Notes   []string
}

// Service installs `run` as a user service.
type Service interface {
	Install(ctx context.Context) (ServiceResult, error)
	// Commands are the manual equivalent, printed when the person declines.
	Commands() []string
}
