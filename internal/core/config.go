// Package core is the daemon's process-wide wiring: configuration and the
// objects main() builds once.
package core

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
)

// Config is deliberately a file plus a few env vars, not a flag soup: the
// catalog of agent kinds is the interesting part and belongs in a file an
// operator can review and a package can ship.
type Config struct {
	// Listen is host:port. A wildcard address is REFUSED — this process runs
	// other people's code, and the design puts it on a private interface with
	// the in-cluster service as its only caller.
	Listen string `json:"listen"`
	// WorkspaceRoot bounds every session's working directory.
	WorkspaceRoot string `json:"workspace_root"`
	// TmuxBin is "tmux" unless a host needs otherwise.
	TmuxBin string `json:"tmux_bin"`
	// Kinds is the allow-list: kind → argv. A caller passes a kind; it can
	// never pass a command line.
	Kinds map[string][]string `json:"kinds"`
	// Auth is where tokens are checked against.
	Auth struct {
		JWKSURL  string `json:"jwks_url"`
		Issuer   string `json:"issuer"`
		Audience string `json:"audience"`
	} `json:"auth"`
	// StreamBuffer bounds the per-session output history kept for reconnects.
	StreamBuffer int `json:"stream_buffer"`
}

// Load reads the file, applies env overrides, and validates.
//
// Env wins over the file so a deployment can inject the auth block without
// templating a file, which is also what keeps secrets out of a public repo's
// examples.
func Load(path string) (*Config, error) {
	c := &Config{Listen: "127.0.0.1:50077", TmuxBin: "tmux", StreamBuffer: 4096}
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
	return c, c.Validate()
}

func env(dst *string, key string) {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		*dst = v
	}
}

// Validate refuses a configuration that would be dangerous rather than merely
// wrong. The wildcard check is the important one: it is the difference between
// a daemon the cluster can reach and a daemon the internet can reach.
func (c *Config) Validate() error {
	host, _, err := net.SplitHostPort(c.Listen)
	if err != nil {
		return fmt.Errorf("agentd: listen %q: %w", c.Listen, err)
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		return fmt.Errorf("agentd: refusing to listen on a wildcard address (%q): "+
			"bind the host's private interface — this process runs other people's code", c.Listen)
	}
	if c.WorkspaceRoot == "" {
		return fmt.Errorf("agentd: workspace_root is required")
	}
	if len(c.Kinds) == 0 {
		return fmt.Errorf("agentd: kinds is empty — a daemon that can start nothing is a misconfiguration, not a safe default")
	}
	for kind, argv := range c.Kinds {
		if len(argv) == 0 {
			return fmt.Errorf("agentd: kind %q has no argv", kind)
		}
	}
	return nil
}
