// Package core is the daemon's process-wide wiring: configuration and the
// objects main() builds once.
package core

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
)

// Config is deliberately a file plus a few env vars, not a flag soup: the
// catalog of agent kinds is the interesting part and belongs in a file an
// operator can review and a package can ship.
type Config struct {
	// Listen is host:port. A wildcard address is REFUSED — this process runs
	// other people's code, and the design puts it on a private interface with
	// the in-cluster service as its only caller.
	Listen string `json:"listen"`
	// TLS secures the cluster→host hop. Required unless the daemon is bound to
	// loopback and plaintext is explicitly allowed.
	TLS TLSConfig `json:"tls"`
	// WorkspaceRoot bounds every session's working directory.
	WorkspaceRoot string `json:"workspace_root"`
	// TmuxBin is "tmux" unless a host needs otherwise.
	TmuxBin string `json:"tmux_bin"`
	// Kinds is the allow-list: kind → argv. A caller passes a kind; it can never
	// pass a command line.
	Kinds map[string][]string `json:"kinds"`
	// EnvAllowPrefixes are the environment-variable name prefixes a caller may
	// set. Empty means "no environment at all", which is the safe default.
	EnvAllowPrefixes []string `json:"env_allow_prefixes"`
	// Auth is where tokens are checked against.
	Auth struct {
		JWKSURL  string `json:"jwks_url"`
		Issuer   string `json:"issuer"`
		Audience string `json:"audience"`
	} `json:"auth"`
	// StreamBuffer and StreamBufferBytes bound the per-session output history
	// kept for reconnects. Both, because one %output line can be a megabyte.
	StreamBuffer      int `json:"stream_buffer"`
	StreamBufferBytes int `json:"stream_buffer_bytes"`
	// MaxSessionsPerOwner and MaxSessions cap concurrency. A CLI agent is a whole
	// interactive process with a model behind it; "start" without a cap is a fork
	// bomb for anyone holding an accepted token.
	MaxSessionsPerOwner int `json:"max_sessions_per_owner"`
	MaxSessions         int `json:"max_sessions"`
	// ReconcileSeconds is how often the daemon re-reads tmux. Reconciling only at
	// boot means a session that dies later reads as alive forever.
	ReconcileSeconds int `json:"reconcile_seconds"`
}

// TLSConfig is mutual TLS on the one hop that leaves the cluster.
//
// The design names this hop explicitly: "the in-cluster↔host hop needs its own
// authentication (mTLS with a private CA, or a mounted shared secret); it is the
// one place where 'we trust the network' does not transfer." Plaintext would put
// a person's bearer token — which grants code execution on this host for its
// whole lifetime — on the wire of the dev host's private segment, replayable by
// anyone who can see it, with no peer authentication to stop a second client
// connecting.
type TLSConfig struct {
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
	// ClientCAFile is the private CA that signs the calling service's
	// certificate. Without it any TLS client could connect, and the token would
	// be the only gate.
	ClientCAFile string `json:"client_ca_file"`
	// AllowPlaintextLoopback is the ONLY way to run without TLS, and it is
	// refused on any address but loopback. It exists for a developer running the
	// daemon and its caller on one machine; there is no other honest use.
	AllowPlaintextLoopback bool `json:"allow_plaintext_loopback"`
}

// Enabled reports whether the server should serve TLS.
func (t TLSConfig) Enabled() bool { return t.CertFile != "" || t.KeyFile != "" || t.ClientCAFile != "" }

// Load reads the file, applies env overrides, and validates.
//
// Env wins over the file so a deployment can inject the auth block without
// templating a file, which is also what keeps secrets out of a public repo's
// examples.
func Load(path string) (*Config, error) {
	c := &Config{
		Listen: "127.0.0.1:50077", TmuxBin: "tmux",
		StreamBuffer: 4096, StreamBufferBytes: 1 << 20,
		MaxSessionsPerOwner: 8, MaxSessions: 32, ReconcileSeconds: 30,
		EnvAllowPrefixes: []string{"SPARKFLOW_"},
	}
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("agentd: reading %s: %w", path, err)
		}
		if err := json.Unmarshal(b, c); err != nil {
			return nil, fmt.Errorf("agentd: parsing %s: %w", path, err)
		}
	}
	env(&c.Listen, "AGENTD_LISTEN")
	env(&c.WorkspaceRoot, "AGENTD_WORKSPACE_ROOT")
	env(&c.TmuxBin, "AGENTD_TMUX_BIN")
	env(&c.Auth.JWKSURL, "AGENTD_JWKS_URL")
	env(&c.Auth.Issuer, "AGENTD_ISSUER")
	env(&c.Auth.Audience, "AGENTD_AUDIENCE")
	env(&c.TLS.CertFile, "AGENTD_TLS_CERT_FILE")
	env(&c.TLS.KeyFile, "AGENTD_TLS_KEY_FILE")
	env(&c.TLS.ClientCAFile, "AGENTD_TLS_CLIENT_CA_FILE")
	envInt(&c.MaxSessions, "AGENTD_MAX_SESSIONS")
	envInt(&c.MaxSessionsPerOwner, "AGENTD_MAX_SESSIONS_PER_OWNER")
	return c, c.Validate()
}

func env(dst *string, key string) {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		*dst = v
	}
}

func envInt(dst *int, key string) {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			*dst = n
		}
	}
}

// EnvPolicy is the domain policy this configuration describes.
func (c *Config) EnvPolicy() session.EnvPolicy {
	return session.EnvPolicy{AllowPrefixes: c.EnvAllowPrefixes}
}

// Validate refuses a configuration that would be dangerous rather than merely
// wrong.
func (c *Config) Validate() error {
	if err := c.validateListen(); err != nil {
		return err
	}
	if err := c.validateTLS(); err != nil {
		return err
	}
	if err := c.validateWorkspaceRoot(); err != nil {
		return err
	}
	if len(c.Kinds) == 0 {
		return fmt.Errorf("agentd: kinds is empty — a daemon that can start nothing is a misconfiguration, not a safe default")
	}
	for kind, argv := range c.Kinds {
		if len(argv) == 0 {
			return fmt.Errorf("agentd: kind %q has no argv", kind)
		}
	}
	if err := c.EnvPolicy().AllowPrefixesSane(); err != nil {
		return err
	}
	return nil
}

// validateListen parses the address instead of comparing spellings.
//
// The string comparison this replaces refused "0.0.0.0", "" and "::" and let
// "[::0]:50077" and "[0:0:0:0:0:0:0:0]:50077" through — both of which bind every
// interface, which is the exact thing the check exists to prevent. A hostname
// that is not an IP is refused too: whether it resolves publicly is not
// something this process can know, and the whole point is a private interface
// named explicitly.
func (c *Config) validateListen() error {
	host, port, err := net.SplitHostPort(c.Listen)
	if err != nil {
		return fmt.Errorf("agentd: listen %q: %w", c.Listen, err)
	}
	if _, err := strconv.Atoi(port); err != nil {
		return fmt.Errorf("agentd: listen %q: %q is not a port number", c.Listen, port)
	}
	ip := net.ParseIP(host)
	if host == "" || ip == nil || ip.IsUnspecified() {
		return fmt.Errorf("agentd: refusing to listen on %q — bind an explicit private "+
			"IP address of this host; a wildcard, an empty host or a name that may resolve "+
			"anywhere all expose a process that runs other people's code", c.Listen)
	}
	return nil
}

func (c *Config) validateTLS() error {
	host, _, _ := net.SplitHostPort(c.Listen)
	loopback := net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback()

	if !c.TLS.Enabled() {
		if c.TLS.AllowPlaintextLoopback && loopback {
			return nil
		}
		return fmt.Errorf("agentd: tls.cert_file, tls.key_file and tls.client_ca_file are "+
			"required — the caller's bearer token crosses this hop and grants code execution "+
			"on this host; set tls.allow_plaintext_loopback only with a loopback listen "+
			"address (this one is %q)", c.Listen)
	}
	missing := []string{}
	if c.TLS.CertFile == "" {
		missing = append(missing, "tls.cert_file")
	}
	if c.TLS.KeyFile == "" {
		missing = append(missing, "tls.key_file")
	}
	if c.TLS.ClientCAFile == "" {
		// Server-only TLS would encrypt the hop and authenticate nothing: any
		// client could connect and present a token. The design asks for MUTUAL
		// TLS, so a half-configuration is refused rather than silently downgraded.
		missing = append(missing, "tls.client_ca_file")
	}
	if len(missing) > 0 {
		return fmt.Errorf("agentd: tls is partly configured; %s missing — mutual TLS is "+
			"all three or none", strings.Join(missing, ", "))
	}
	for _, f := range []string{c.TLS.CertFile, c.TLS.KeyFile, c.TLS.ClientCAFile} {
		if _, err := os.Stat(f); err != nil {
			return fmt.Errorf("agentd: tls file %q: %w", f, err)
		}
	}
	return nil
}

// validateWorkspaceRoot refuses a root that would make the containment check
// meaningless. "/" passes every prefix test, and a relative root cannot be
// compared against the absolute paths callers send.
func (c *Config) validateWorkspaceRoot() error {
	if c.WorkspaceRoot == "" {
		return fmt.Errorf("agentd: workspace_root is required")
	}
	if !filepath.IsAbs(c.WorkspaceRoot) {
		return fmt.Errorf("agentd: workspace_root %q must be an absolute path", c.WorkspaceRoot)
	}
	clean := filepath.Clean(c.WorkspaceRoot)
	if clean == string(filepath.Separator) {
		return fmt.Errorf("agentd: workspace_root %q would allow every path on the host", c.WorkspaceRoot)
	}
	c.WorkspaceRoot = clean
	return nil
}
