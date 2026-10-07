// Package host is this device as an enrolled Sparkflow agent host: which
// deployment it belongs to, who owns it, and the credential it dials with.
// Standard library only.
//
// Since 2026-10-06 the daemon DIALS OUT: `sparkflow-agentd init` signs the
// person in, registers the device as one of their hosts, and stores a
// long-lived credential; `sparkflow-agentd run` then holds one outbound channel
// to the cluster. Design: CLI Agent Sessions §Agent hosts.
package host

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// Deployment is the (server, issuer, client) triple — chosen TOGETHER so the
// three can never be mixed. The flags mirror sparkflow-sync's `login`
// exactly (owner, 2026-10-06): production by default, `--debug-dev` for dev,
// `--server` / `--issuer` / `--client-id` to override one value each.
type Deployment struct {
	ServerURL string
	IssuerURL string
	ClientID  string
}

// Production is what a stranger who `go install`s the daemon gets.
//
// ClientID is EMPTY on purpose: the production IdP is not provisioned yet.
// `init` then says so in a sentence instead of offering the dev client to an
// issuer that has never heard of it — sparkflow-sync's rule (SYNC-024).
var Production = Deployment{
	ServerURL: "https://sparkflow.space",
	IssuerURL: "https://login.sparkflow.space",
	ClientID:  "",
}

// Dev is what `--debug-dev` selects. Its ClientID is the daemon's OWN Zitadel
// client (`agentd`, not sparkflow-sync's `cli`): agent-sessions accepts only
// that client's tokens on the host channel. Created by gitops
// remote-dev/zitadel/provision.sh (GITOPS-179, 2026-10-07): a public native
// client, devMode off, redirects http://127.0.0.1/ (any port) and
// https://sparkflow.ddns.net/agentd/code.
var Dev = Deployment{
	ServerURL: "https://sparkflow.ddns.net",
	IssuerURL: "https://login.sparkflow.ddns.net",
	ClientID:  "394047772828895566",
}

// DeploymentFor is the starting triple; per-flag overrides apply on top.
func DeploymentFor(debugDev bool) Deployment {
	if debugDev {
		return Dev
	}
	return Production
}

// ErrNotProvisioned is an empty client id: the deployment has no daemon client.
var ErrNotProvisioned = errors.New("no OIDC client id for this deployment: its sparkflow-agentd client is not provisioned yet — " +
	"pass `--client-id`, or `--debug-dev` if you meant the development deployment")

// Validate checks a deployment is usable.
func (d Deployment) Validate() error {
	if strings.TrimSpace(d.ServerURL) == "" || strings.TrimSpace(d.IssuerURL) == "" {
		return errors.New("server and issuer URLs are required")
	}
	if strings.TrimSpace(d.ClientID) == "" {
		return ErrNotProvisioned
	}
	return nil
}

// Credentials is what `init` stores and `run` dials with. It is the crown
// jewel of this device: whoever reads it can run the owner's agents here and
// enrol other devices as them. It lives in a 0700 directory, mode 0600, and
// is never printed.
type Credentials struct {
	Deployment    Deployment `json:"deployment"`
	TokenEndpoint string     `json:"token_endpoint"`
	HostID        string     `json:"host_id"`
	HostName      string     `json:"host_name"`
	// OwnerSub is the Zitadel subject of the person who enrolled this device.
	// The daemon serves this person only.
	OwnerSub     string    `json:"owner_sub"`
	OwnerEmail   string    `json:"owner_email,omitempty"`
	RefreshToken string    `json:"refresh_token"`
	AccessToken  string    `json:"access_token,omitempty"`
	ExpiresAt    time.Time `json:"expires_at,omitempty"`
}

// Validate refuses a credential that cannot be used to run.
func (c Credentials) Validate() error {
	var missing []string
	for name, v := range map[string]string{
		"host_id": c.HostID, "owner_sub": c.OwnerSub, "refresh_token": c.RefreshToken,
		"token_endpoint": c.TokenEndpoint, "server": c.Deployment.ServerURL, "client_id": c.Deployment.ClientID,
	} {
		if strings.TrimSpace(v) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("the credentials file is incomplete (%s) — run `sparkflow-agentd init` again", strings.Join(missing, ", "))
	}
	return nil
}

// MaxNameRunes caps the host name (agent-sessions applies the same cap).
const MaxNameRunes = 64

// SuggestName is the name `init` offers: "<OS> · <Country>", or the OS alone
// when the country is unknown. The person accepts it with Enter or types
// another.
func SuggestName(osName, country string) string {
	osName, country = strings.TrimSpace(osName), strings.TrimSpace(country)
	if osName == "" {
		osName = "Host"
	}
	name := osName
	if country != "" {
		name = osName + " · " + country
	}
	return CapName(name)
}

// CapName collapses whitespace and caps the length in runes.
func CapName(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) <= MaxNameRunes {
		return s
	}
	r := []rune(s)
	return strings.TrimSpace(string(r[:MaxNameRunes]))
}

// ErrOtherOwner is a re-run of `init` signed in as somebody else.
var ErrOtherOwner = errors.New("this device is enrolled to another person; run `sparkflow-agentd init --new` to replace that enrolment")

// ReEnrolID decides which host id a re-run of `init` presents: the stored one
// when the same person signs in (one host per device), none for `--new` or a
// first run. A different person is refused unless they asked for `--new`.
func ReEnrolID(existing *Credentials, signedInSub string, forceNew bool) (string, error) {
	if existing == nil || forceNew {
		return "", nil
	}
	if existing.OwnerSub != "" && existing.OwnerSub != signedInSub {
		return "", ErrOtherOwner
	}
	return existing.HostID, nil
}
