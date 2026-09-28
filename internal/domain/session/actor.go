package session

// Actor is the verified human behind a call: the token's subject, plus the
// email when the token carries one. Only Subject is guaranteed.
//
// It lives in the DOMAIN rather than beside the token verifier, and that is
// load-bearing in two ways. It is the ownership key — Session.Owner is a
// Subject — so the rule "a caller reaches only its own sessions" is a domain
// rule with a domain type. And it keeps the handler's Verifier port free of an
// infra import: the verifier is one implementation of an identity the domain
// already names, not the other way round.
type Actor struct {
	// Subject is the IdP's stable identifier for the person (Zitadel `sub`).
	// It is what ownership compares, because an email can be reassigned and a
	// display name can change.
	Subject string
	Email   string
}

// Valid reports whether this actor may be used for authorisation at all. The
// zero Actor is never authorised: every lookup compares Owner to Subject, and
// an empty Subject must not match a session adopted without an owner.
func (a Actor) Valid() bool { return a.Subject != "" }

// Owns reports whether a session belongs to this actor.
//
// An empty Owner (a session adopted from a tmux server that carries no
// ownership label) belongs to NOBODY rather than to everybody. That asymmetry
// is the point: the alternative fails open.
func (a Actor) Owns(s *Session) bool {
	return a.Valid() && s != nil && s.Owner != "" && s.Owner == a.Subject
}

// Name is what goes in the audit log: the email when there is one, because a
// human reading the log six months later knows an address and does not know a
// subject id.
func (a Actor) Name() string {
	if a.Email != "" {
		return a.Email
	}
	return a.Subject
}

// System is the actor the daemon uses for its own bookkeeping (reconciliation
// after a restart). It is not a person and owns nothing.
var System = Actor{Subject: "", Email: "system"}
