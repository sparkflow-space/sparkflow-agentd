// Package core is the daemon's process-wide wiring: configuration and the
// objects main() builds once.
package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
)

// Config is OPTIONAL since AGENTD-003: `init` and `run` work with no file at
// all, and ~/.config/sparkflow-agentd/config.json only overrides defaults.
// Who the daemon talks to and as whom is NOT here — that is the enrolment, in
// credentials.json, written by `init`.
//
// Gone with the inbound server: listen, tls, auth and workspace_root. The
// daemon opens no port, and the working folder is the one the person chose
// under their own home.
type Config struct {
	// TmuxBin is "tmux" unless a host needs otherwise.
	TmuxBin string `json:"tmux_bin"`
	// Kinds is the allow-list: kind → argv. A caller passes a kind; it can never
	// pass a command line. `init` registers only the kinds whose program is on
	// PATH.
	Kinds map[string][]string `json:"kinds"`
	// EnvAllowPrefixes are the environment-variable name prefixes a caller may
	// set. Empty means "no environment at all".
	EnvAllowPrefixes []string `json:"env_allow_prefixes"`
	// StreamBuffer and StreamBufferBytes bound the per-session output history
	// kept for reconnects. Both, because one %output line can be a megabyte.
	StreamBuffer      int `json:"stream_buffer"`
	StreamBufferBytes int `json:"stream_buffer_bytes"`
	// MaxSessionsPerOwner and MaxSessions cap concurrency: a CLI agent is a
	// whole interactive process with a model behind it.
	MaxSessionsPerOwner int `json:"max_sessions_per_owner"`
	MaxSessions         int `json:"max_sessions"`
	// ReconcileSeconds is how often the daemon re-reads tmux.
	ReconcileSeconds int `json:"reconcile_seconds"`
}

// DefaultKinds are the agent CLIs the daemon knows by default — ids from CLI
// Agent Sessions §Session model. A config file can add or replace them.
func DefaultKinds() map[string][]string {
	return map[string][]string{
		"claude":      {"claude"},
		"opencode-go": {"opencode"},
		"jcode":       {"jcode"},
		"qwen":        {"qwen"},
		"kimi":        {"kimi"},
	}
}

// Load reads the file when it exists, applies env overrides, and validates.
// A missing file is the normal case.
func Load(path string) (*Config, error) {
	c := &Config{
		TmuxBin: "tmux", Kinds: DefaultKinds(),
		StreamBuffer: 4096, StreamBufferBytes: 1 << 20,
		MaxSessionsPerOwner: 8, MaxSessions: 32, ReconcileSeconds: 30,
		EnvAllowPrefixes: []string{"SPARKFLOW_"},
	}
	if path != "" {
		b, err := os.ReadFile(path)
		switch {
		case errors.Is(err, fs.ErrNotExist):
		case err != nil:
			return nil, fmt.Errorf("agentd: reading %s: %w", path, err)
		default:
			// Kinds are decoded apart: json.Unmarshal MERGES into an existing
			// map, so decoding over the defaults would let a file add kinds but
			// never narrow them — `{"kinds": {}}` would silently keep all of
			// them. A file's kinds REPLACE the defaults.
			c.Kinds = nil
			if err := json.Unmarshal(b, c); err != nil {
				return nil, fmt.Errorf("agentd: parsing %s: %w", path, err)
			}
			var probe struct {
				Kinds *map[string][]string `json:"kinds"`
			}
			_ = json.Unmarshal(b, &probe)
			if probe.Kinds == nil {
				c.Kinds = DefaultKinds()
			}
		}
	}
	env(&c.TmuxBin, "AGENTD_TMUX_BIN")
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
	if len(c.Kinds) == 0 {
		return fmt.Errorf("agentd: kinds is empty — a daemon that can start nothing is a misconfiguration, not a safe default")
	}
	for kind, argv := range c.Kinds {
		if len(argv) == 0 || strings.TrimSpace(argv[0]) == "" {
			return fmt.Errorf("agentd: kind %q has no argv", kind)
		}
	}
	return c.EnvPolicy().AllowPrefixesSane()
}
