// Package enrol is `sparkflow-agentd init`: sign the person in, name the
// device, register it as one of their hosts, keep the credential, and offer
// to run the daemon with the system. Design: CLI Agent Sessions §Agent hosts.
package enrol

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/host"
)

// Deps is what the composition root injects.
type Deps struct {
	IdP       host.IdP
	Redirects host.Redirects
	Prompter  host.Prompter
	Browser   host.Browser
	Machine   host.Machine
	Registrar host.Registrar
	Store     host.CredentialStore
	Service   host.Service
}

// Options are init's flags.
type Options struct {
	Deployment host.Deployment
	// Manual signs in through the server's code page instead of the loopback
	// (for a host whose browser is on another machine, when that page exists).
	Manual    bool
	NoBrowser bool
	// Name skips the name question.
	Name string
	// New drops an existing enrolment of this device.
	New bool
	// NoService prints the service commands instead of asking.
	NoService bool
	// Yes answers every question with its default.
	Yes     bool
	Version string
}

// Result is what init did.
type Result struct {
	HostID, HostName, Email string
	ReEnrolled              bool
	Service                 *host.ServiceResult
}

// Run is the whole of `init`.
func Run(ctx context.Context, d Deps, o Options) (Result, error) {
	if err := o.Deployment.Validate(); err != nil {
		return Result{}, err
	}
	existing, err := d.Store.Load()
	if err != nil {
		return Result{}, err
	}

	signIn, tokenEndpoint, err := signIn(ctx, d, o)
	if err != nil {
		return Result{}, err
	}
	if signIn.Sub == "" {
		return Result{}, errors.New("the sign-in did not say who you are (no subject in the id token)")
	}
	who := signIn.Email
	if who == "" {
		who = signIn.Sub
	}
	d.Prompter.Say("Signed in as " + who)

	hostID, err := host.ReEnrolID(existing, signIn.Sub, o.New)
	if err != nil {
		return Result{}, err
	}

	suggest := host.SuggestName(d.Machine.OSName(), d.Machine.Country())
	if hostID != "" && existing != nil && existing.HostName != "" {
		suggest = existing.HostName // re-running init keeps the name by default
	}
	name := host.CapName(o.Name)
	if name == "" {
		if name, err = ask(ctx, d.Prompter, o.Yes, fmt.Sprintf("Name this host [%s]: ", suggest), suggest); err != nil {
			return Result{}, err
		}
		name = host.CapName(name)
	}

	kinds := d.Machine.Kinds()
	if len(kinds) == 0 {
		d.Prompter.Say("No agent CLIs found on PATH (claude, opencode, qwen, …). The host registers anyway; install one and run init again to offer it.")
	} else {
		d.Prompter.Say("Agents found on PATH: " + strings.Join(kinds, ", "))
	}

	newID, gotName, err := d.Registrar.Register(ctx, o.Deployment, signIn.AccessToken, host.Enrolment{
		HostID: hostID, Name: name, OS: d.Machine.OSName(), Arch: d.Machine.Arch(), Version: o.Version, Kinds: kinds,
	})
	if err != nil {
		return Result{}, fmt.Errorf("registering this host: %w", err)
	}
	creds := host.Credentials{
		Deployment: o.Deployment, TokenEndpoint: tokenEndpoint,
		HostID: newID, HostName: gotName, OwnerSub: signIn.Sub, OwnerEmail: signIn.Email,
		RefreshToken: signIn.RefreshToken, AccessToken: signIn.AccessToken, ExpiresAt: signIn.ExpiresAt,
	}
	if err := d.Store.Save(creds); err != nil {
		return Result{}, fmt.Errorf("saving the credentials: %w", err)
	}
	d.Prompter.Say(fmt.Sprintf("Registered host %q. Credentials: %s (readable by you only)", gotName, d.Store.Where()))

	res := Result{HostID: newID, HostName: gotName, Email: signIn.Email, ReEnrolled: hostID != "" && hostID == newID}
	// A daemon already running holds the OLD enrolment in memory; restart it
	// so it uses the new one (it would refuse to write over it anyway).
	if existing != nil && d.Service.Active(ctx) {
		if err := d.Service.Restart(ctx); err != nil {
			d.Prompter.Say("The running service still uses the previous enrolment; restart it: " + err.Error())
		} else {
			d.Prompter.Say("Restarted the running service with the new enrolment.")
			return res, nil
		}
	}
	if o.NoService {
		say(d.Prompter, "To run it with the system:", d.Service.Commands())
		return res, nil
	}
	yes, err := confirm(ctx, d.Prompter, o.Yes, "Run it with the system? [Y/n] ", true)
	if err != nil {
		return res, err
	}
	if !yes {
		say(d.Prompter, "To run it with the system later:", d.Service.Commands())
		return res, nil
	}
	sr, err := d.Service.Install(ctx)
	if err != nil {
		// Never "installed" for a service that is not running.
		say(d.Prompter, "Could not install the service ("+err.Error()+"). To do it by hand:", d.Service.Commands())
		return res, nil
	}
	res.Service = &sr
	d.Prompter.Say("Installed and started: " + sr.Path)
	for _, n := range sr.Notes {
		d.Prompter.Say("Note: " + n)
	}
	return res, nil
}

// signIn runs the code + PKCE flow. The code comes back through the loopback
// redirect when the browser is on this machine — or the person pastes it (the
// whole address from the browser's address bar, or the code) when the browser
// is elsewhere, e.g. over SSH: whichever arrives first.
func signIn(ctx context.Context, d Deps, o Options) (host.SignIn, string, error) {
	authEP, tokenEP, err := d.IdP.Discover(ctx, o.Deployment.IssuerURL)
	if err != nil {
		return host.SignIn{}, "", err
	}
	att, err := d.IdP.NewAttempt()
	if err != nil {
		return host.SignIn{}, "", err
	}

	var redirectURI string
	var loop host.Redirect
	if o.Manual {
		redirectURI = strings.TrimRight(o.Deployment.ServerURL, "/") + "/agentd/code"
	} else {
		if loop, err = d.Redirects.Start(att.State); err != nil {
			return host.SignIn{}, "", err
		}
		defer loop.Close()
		redirectURI = loop.URI()
	}
	link := d.IdP.AuthorizeURL(authEP, o.Deployment.ClientID, redirectURI, att)
	d.Prompter.Say("Sign in: " + link)
	if o.Manual {
		d.Prompter.Say("After signing in, paste the code shown on the page here and press Enter:")
	} else {
		d.Prompter.Say("Waiting for the browser… (browser on another machine? after signing in, copy the address from its address bar — or just the code — paste it here and press Enter)")
		if !o.NoBrowser {
			_ = d.Browser.Open(link) // best effort: the printed link is the source of truth
		}
	}

	code, err := awaitCode(ctx, d, loop, att.State)
	if err != nil {
		return host.SignIn{}, "", err
	}
	si, err := d.IdP.Exchange(ctx, tokenEP, o.Deployment.ClientID, code, att.Verifier, redirectURI)
	if err != nil {
		return host.SignIn{}, "", err
	}
	return si, tokenEP, nil
}

func awaitCode(ctx context.Context, d Deps, loop host.Redirect, state string) (string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type got struct {
		code string
		err  error
	}
	ch := make(chan got, 2)
	if loop != nil {
		go func() { c, err := loop.Wait(ctx); ch <- got{c, err} }()
	}
	go func() {
		for {
			line, err := d.Prompter.Line(ctx)
			if err != nil {
				// Stdin closed (not a terminal): with a loopback still
				// listening, the browser can finish the sign-in alone.
				if loop == nil || ctx.Err() != nil {
					ch <- got{err: fmt.Errorf("reading the code: %w", err)}
				}
				return
			}
			if strings.TrimSpace(line) == "" {
				continue
			}
			code, perr := d.IdP.ParsePasted(line, state)
			if perr != nil {
				d.Prompter.Say(perr.Error() + " — try again:")
				continue
			}
			ch <- got{code: code}
			return
		}
	}()
	select {
	case g := <-ch:
		return g.code, g.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func ask(ctx context.Context, p host.Prompter, yes bool, q, def string) (string, error) {
	if yes {
		return def, nil
	}
	p.Say(q)
	line, err := p.Line(ctx)
	if err != nil {
		return "", err
	}
	if s := strings.TrimSpace(line); s != "" {
		return s, nil
	}
	return def, nil
}

func confirm(ctx context.Context, p host.Prompter, yes bool, q string, def bool) (bool, error) {
	if yes {
		return def, nil
	}
	for {
		p.Say(q)
		line, err := p.Line(ctx)
		if err != nil {
			return false, err
		}
		switch strings.ToLower(strings.TrimSpace(line)) {
		case "":
			return def, nil
		case "y", "yes", "д", "да":
			return true, nil
		case "n", "no", "н", "нет":
			return false, nil
		}
	}
}

func say(p host.Prompter, head string, cmds []string) {
	p.Say(head)
	for _, c := range cmds {
		p.Say("  " + c)
	}
}
