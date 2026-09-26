package session

import (
	"fmt"
	"strings"
)

// EnvPolicy decides which environment variables a caller may set on a session.
//
// WHY AN ALLOW-LIST AND NOT A CHECK FOR "DANGEROUS" NAMES.
//
// The daemon's headline invariant is that a caller passes a KIND, never a
// command line (see Catalog). An unfiltered environment quietly gives that
// invariant away: `LD_PRELOAD=/srv/agents/p1/x.so` runs the caller's code
// inside the allow-listed binary before main() does anything, and it passes
// every other gate — the kind resolves, the workspace is inside the root.
// MEASURED on tmux 3.4 on the dev host: a session started with
// `-e LD_PRELOAD=…` printed the injected library's constructor output before
// the command ran. BASH_ENV, NODE_OPTIONS, PYTHONSTARTUP, GIT_SSH_COMMAND and
// PERL5OPT are the same hole with different spellings, and the list grows with
// every runtime the catalog gains.
//
// So the rule is inverted: only names the OPERATOR opted into are accepted,
// and the deny-list below is a second fence that no configuration can lower.
type EnvPolicy struct {
	// AllowPrefixes are the name prefixes a caller may set — e.g. "SPARKFLOW_".
	// An empty policy accepts NO environment at all, which is the safe default
	// for a daemon whose callers mostly need none.
	AllowPrefixes []string
	// MaxEntries and MaxLen bound the total, so a caller cannot push megabytes
	// through tmux's command line.
	MaxEntries int
	MaxLen     int
}

// Default bounds, applied when a policy leaves them at zero.
const (
	DefaultEnvMaxEntries = 32
	DefaultEnvMaxLen     = 4096
)

// envDeniedPrefixes and envDeniedNames are refused whatever AllowPrefixes says.
// Every entry is here because it makes some runtime execute code, or read its
// startup file from, a path the caller controls.
var (
	envDeniedPrefixes = []string{
		"LD_", "DYLD_", "GLIBC_", "MALLOC_", "BASH_", "ZSH_", "PYTHON",
		"PERL5", "PERLLIB", "RUBY", "NODE_", "JAVA_", "_JAVA", "GIT_",
		"SSH_", "GODEBUG", "ELECTRON_",
	}
	envDeniedNames = []string{
		"PATH", "SHELL", "ENV", "IFS", "PS1", "PS2", "PS4", "FPATH", "CDPATH",
		"PROMPT_COMMAND", "SHELLOPTS", "BASHOPTS", "ZDOTDIR", "HOME", "USER",
		"LOGNAME", "TMPDIR", "PAGER", "EDITOR", "VISUAL", "TERMINFO",
		"LOCALDOMAIN", "HOSTALIASES", "NIS_PATH", "RUBYOPT", "RUBYLIB",
	}
)

// Validate checks every entry and refuses the whole request on the first
// problem — a partially-applied environment is harder to reason about than a
// refusal, and the caller can fix its request.
func (p EnvPolicy) Validate(env []string) error {
	maxEntries, maxLen := p.MaxEntries, p.MaxLen
	if maxEntries <= 0 {
		maxEntries = DefaultEnvMaxEntries
	}
	if maxLen <= 0 {
		maxLen = DefaultEnvMaxLen
	}
	if len(env) > maxEntries {
		return fmt.Errorf("%w: %d entries, at most %d", ErrEnvNotAllowed, len(env), maxEntries)
	}
	seen := make(map[string]bool, len(env))
	for _, e := range env {
		if len(e) > maxLen {
			return fmt.Errorf("%w: an entry is %d bytes, at most %d", ErrEnvNotAllowed, len(e), maxLen)
		}
		name, value, ok := strings.Cut(e, "=")
		if !ok {
			return fmt.Errorf("%w: %q is not NAME=VALUE", ErrEnvNotAllowed, e)
		}
		if err := validEnvName(name); err != nil {
			return err
		}
		if strings.ContainsAny(value, "\x00\n\r") {
			return fmt.Errorf("%w: the value of %s contains a control character", ErrEnvNotAllowed, name)
		}
		if seen[name] {
			// tmux would accept both and the last would win; refusing is
			// clearer than silently picking one.
			return fmt.Errorf("%w: %s is set twice", ErrEnvNotAllowed, name)
		}
		seen[name] = true
		if denied(name) {
			return fmt.Errorf("%w: %s is never allowed — it makes a runtime load code from a path the caller chooses", ErrEnvNotAllowed, name)
		}
		if !p.allowed(name) {
			return fmt.Errorf("%w: %s matches none of the allowed prefixes %v", ErrEnvNotAllowed, name, p.AllowPrefixes)
		}
	}
	return nil
}

// Names returns just the names, for the audit line. VALUES ARE NEVER LOGGED: a
// caller may legitimately pass a per-session token, and the audit sink is read
// by more people than the session is.
func EnvNames(env []string) []string {
	out := make([]string, 0, len(env))
	for _, e := range env {
		name, _, _ := strings.Cut(e, "=")
		out = append(out, name)
	}
	return out
}

func (p EnvPolicy) allowed(name string) bool {
	for _, pref := range p.AllowPrefixes {
		if pref != "" && strings.HasPrefix(name, pref) {
			return true
		}
	}
	return false
}

// denied compares case-insensitively. On Linux the dynamic loader honours only
// the uppercase spellings, but an agent that is itself a shell script may read
// a lowercase one, and the cost of being strict here is zero.
func denied(name string) bool {
	up := strings.ToUpper(name)
	if strings.Contains(up, "PRELOAD") {
		return true
	}
	for _, p := range envDeniedPrefixes {
		if strings.HasPrefix(up, p) {
			return true
		}
	}
	for _, n := range envDeniedNames {
		if up == n {
			return true
		}
	}
	return false
}

// validEnvName enforces the POSIX shape. A name containing "=", a space or a
// NUL is not a name; accepting one would let a caller smuggle a second
// assignment past the prefix check.
func validEnvName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: an entry has an empty name", ErrEnvNotAllowed)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c == '_':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return fmt.Errorf("%w: %q is not a valid variable name", ErrEnvNotAllowed, name)
		}
	}
	return nil
}

// AllowPrefixesSane rejects a policy that would defeat itself: an empty prefix
// matches every name, and a prefix the deny-list already covers is a
// configuration mistake worth naming at boot rather than at the first Start.
func (p EnvPolicy) AllowPrefixesSane() error {
	for _, pref := range p.AllowPrefixes {
		if strings.TrimSpace(pref) == "" {
			return fmt.Errorf("%w: an empty allow-prefix would accept every variable", ErrEnvNotAllowed)
		}
		if denied(pref) {
			return fmt.Errorf("%w: the allow-prefix %q is on the deny-list", ErrEnvNotAllowed, pref)
		}
	}
	return nil
}
