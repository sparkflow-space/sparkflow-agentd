// Command sparkflow-agentd runs CLI coding agents on this device for one
// person, driven from Sparkflow through an OUTBOUND channel — it opens no port.
//
//	sparkflow-agentd init [--debug-dev] …   sign in, name and register this device, offer a user service
//	sparkflow-agentd run                    hold the channel (what the service executes)
//	sparkflow-agentd service install|uninstall|status
//	sparkflow-agentd status                 who this device is enrolled to, and whether it runs
//	sparkflow-agentd version
//
// See the README for the walk-through and CLAUDE.md for what it refuses.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/sparkflow-space/sparkflow-agentd/internal/application/enrol"
	"github.com/sparkflow-space/sparkflow-agentd/internal/application/sessions"
	"github.com/sparkflow-space/sparkflow-agentd/internal/core"
	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/host"
	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
	"github.com/sparkflow-space/sparkflow-agentd/internal/handler/channel"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/credfile"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/hostchannel"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/hostfs"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/hostinfo"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/lockfile"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/terminal"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/tmux"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/zitadel"
)

// version is stamped at release time by GoReleaser (-X main.version={{.Tag}}).
//
// Without the stamp the module version recorded in the build info is used:
// `go install …@vX.Y.Z` records the tag, and a local `go build` in a checkout
// records a pseudo-version derived from the commit (measured: a build at
// 6ed5e46 reports v0.0.0-20260927105140-6ed5e469b2fd). So the binary reports
// the same version however it was obtained, and "dev" only when there is no
// build info at all.
var version = "dev"

func versionString() string {
	if version != "dev" {
		return version
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return version
}

// exitPermanent tells the service manager not to restart (RestartPreventExitStatus=3).
const exitPermanent = 3

const usage = `sparkflow-agentd — run CLI coding agents on this device, driven from Sparkflow.

  sparkflow-agentd init [flags]       sign in, name this device and register it as one of your hosts
  sparkflow-agentd run                hold the channel to Sparkflow (what the service runs)
  sparkflow-agentd service install    run it with the system (a user service)
  sparkflow-agentd service uninstall
  sparkflow-agentd service status
  sparkflow-agentd status             show this device's enrolment
  sparkflow-agentd version

Run "sparkflow-agentd init -h" for init's flags.
`

func main() {
	args := os.Args[1:]
	// -version is kept from v0.1.0.
	if len(args) == 1 && (args[0] == "-version" || args[0] == "--version" || args[0] == "version") {
		fmt.Println(versionString())
		return
	}
	if len(args) == 0 {
		fmt.Print(usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var err error
	switch args[0] {
	case "init":
		err = cmdInit(ctx, args[1:])
	case "run":
		err = cmdRun(ctx)
	case "service":
		err = cmdService(ctx, args[1:])
	case "status":
		err = cmdStatus(ctx)
	case "-h", "--help", "help":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", args[0], usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "sparkflow-agentd: "+err.Error())
		if errors.Is(err, channel.ErrPermanent) {
			os.Exit(exitPermanent)
		}
		os.Exit(1)
	}
}

func configDir() string {
	d, err := credfile.DefaultDir()
	if err != nil {
		log.Fatalf("sparkflow-agentd: cannot find a config directory: %v", err)
	}
	return d
}

func credStore() credfile.Store {
	return credfile.Store{Path: filepath.Join(configDir(), "credentials.json")}
}

func loadConfig() (*core.Config, error) {
	return core.Load(filepath.Join(configDir(), "config.json"))
}

func cmdInit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	debugDev := fs.Bool("debug-dev", false, "Target the development deployment instead of production (server, issuer AND client id together).")
	server := fs.String("server", "", "Sparkflow server URL. Overrides --debug-dev.")
	issuer := fs.String("issuer", "", "OIDC issuer URL. Overrides --debug-dev.")
	clientID := fs.String("client-id", "", "Public OIDC client id of sparkflow-agentd. Overrides --debug-dev.")
	manual := fs.Bool("manual", false, "Sign in through the server's code page and paste the code (browser on another machine).")
	noBrowser := fs.Bool("no-browser", false, "Don't try to open a browser — just print the sign-in link (over SSH / headless).")
	name := fs.String("name", "", "Name this host without being asked.")
	fresh := fs.Bool("new", false, "Drop this device's existing enrolment and register it as a new host.")
	noService := fs.Bool("no-service", false, "Don't install a user service; print the commands instead.")
	yes := fs.Bool("yes", false, "Accept every default without asking.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	d := host.DeploymentFor(*debugDev)
	if *server != "" {
		d.ServerURL = *server
	}
	if *issuer != "" {
		d.IssuerURL = *issuer
	}
	if *clientID != "" {
		d.ClientID = *clientID
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	svc, err := serviceManager()
	if err != nil {
		return err
	}
	_, inSSH := os.LookupEnv("SSH_CONNECTION")
	term := terminal.New(os.Stdin, os.Stdout)
	_, err = enrol.Run(ctx, enrol.Deps{
		IdP: idpAdapter{zitadel.New()}, Redirects: redirects{}, Prompter: term, Browser: systemBrowser{},
		Machine:   machine{info: hostinfo.New(), kinds: cfg.Kinds},
		Registrar: registrar{}, Store: storeAdapter{credStore()}, Service: serviceAdapter{svc},
	}, enrol.Options{
		Deployment: d, Manual: *manual, NoBrowser: *noBrowser || inSSH, Name: *name, New: *fresh,
		NoService: *noService, Yes: *yes, Version: versionString(),
	})
	return err
}

func cmdRun(ctx context.Context) error {
	store := credStore()
	creds, err := store.Load()
	if err != nil {
		return err
	}
	if err := creds.Validate(); err != nil {
		return err
	}
	lock, err := lockfile.Acquire(store.Path + ".lock")
	if err != nil {
		return err
	}
	defer lock.Release()
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	target, secure, err := hostchannel.Target(creds.Deployment.ServerURL)
	if err != nil {
		return err
	}

	tokens := hostchannel.NewTokens(*creds, zitadel.New(), store.Save, store.Load)
	tokens.OnSaveError = func(err error) {
		log.Printf("warning: could not write the rotated sign-in to %s (%v); keeping it in memory and retrying", store.Path, err)
	}
	dialer := &hostchannel.Dialer{Target: target, Secure: secure, Tokens: tokens}
	defer dialer.Close()

	cli := tmux.NewCLI(cfg.TmuxBin)
	control := tmux.NewControl(cfg.TmuxBin, cfg.StreamBuffer, cfg.StreamBufferBytes, log.Printf)
	defer control.Close()

	audit := newAuditLog(os.Stdout)
	runner := &channel.Runner{
		Audit: audit, Owner: creds.OwnerSub, HostID: creds.HostID, Version: versionString(),
		Kinds:    hostinfo.New().KindsOnPath(cfg.Kinds),
		Classify: hostchannel.Permanent,
		Dial:     func(ctx context.Context) (channel.Stream, error) { return dialer.Connect(ctx) },
	}
	svc := sessions.New(sessions.Deps{
		Tmux: combined{CLI: cli, Control: control}, Workspaces: hostfs.Dirs{}, Audit: audit,
		Clock: realClock{}, IDs: randomIDs{}, Catalog: session.Catalog(cfg.Kinds), Env: cfg.EnvPolicy(),
		Home: home, Events: runner,
		Limits: sessions.Limits{PerOwner: cfg.MaxSessionsPerOwner, Total: cfg.MaxSessions},
	})
	runner.Sessions = svc

	// tmux outlives this process: find out what is actually running first.
	if err := svc.Reconcile(ctx); err != nil {
		log.Printf("warning: reconciling existing tmux sessions failed: %v", err)
	}
	go reconcileLoop(ctx, svc, time.Duration(cfg.ReconcileSeconds)*time.Second)

	log.Printf("sparkflow-agentd %s: host %q (%s) for %s → %s; kinds %v; no listening port",
		versionString(), creds.HostName, creds.HostID, ownerName(creds), creds.Deployment.ServerURL, runner.Kinds)
	return runner.Run(ctx)
}

func ownerName(c *host.Credentials) string {
	if c.OwnerEmail != "" {
		return c.OwnerEmail
	}
	return c.OwnerSub
}

func cmdService(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return errors.New("usage: sparkflow-agentd service install|uninstall|status")
	}
	m, err := serviceManager()
	if err != nil {
		return err
	}
	switch args[0] {
	case "install":
		if _, err := credStore().Load(); err != nil {
			return err // nothing to run before init
		}
		res, err := m.Install(ctx)
		if err != nil {
			return err
		}
		fmt.Println("Installed and started: " + res.UnitPath)
		for _, n := range res.Notes {
			fmt.Println("Note: " + n)
		}
	case "uninstall":
		if err := m.Uninstall(ctx); err != nil {
			return err
		}
		fmt.Println("The service is stopped and removed. The enrolment is kept; revoke the host in \"My hosts\" if this device is done with Sparkflow.")
	case "status":
		fmt.Println(m.Status(ctx))
	default:
		return fmt.Errorf("unknown service command %q", args[0])
	}
	return nil
}

func cmdStatus(ctx context.Context) error {
	c, err := credStore().Load()
	if err != nil {
		return err
	}
	fmt.Printf("host:    %s (%s)\nowner:   %s\nserver:  %s\nversion: %s\n", c.HostName, c.HostID, ownerName(c), c.Deployment.ServerURL, versionString())
	if m, err := serviceManager(); err == nil {
		fmt.Printf("service: %s\n", m.Status(ctx))
	}
	return nil
}

func reconcileLoop(ctx context.Context, svc *sessions.Service, every time.Duration) {
	if every <= 0 {
		every = 30 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := svc.Reconcile(ctx); err != nil {
				log.Printf("warning: reconcile failed: %v", err)
			}
		}
	}
}

var _ io.Writer = os.Stdout
