package core_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sparkflow-space/sparkflow-agentd/internal/core"
)

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoad_NoFileIsTheNormalCase(t *testing.T) {
	c, err := core.Load(filepath.Join(t.TempDir(), "absent.json"))
	if err != nil {
		t.Fatalf("a missing config file must not be an error: %v", err)
	}
	if c.TmuxBin != "tmux" || len(c.Kinds) == 0 || c.Kinds["claude"][0] != "claude" {
		t.Errorf("defaults: %+v", c)
	}
	if strings.Join(c.EnvAllowPrefixes, ",") != "SPARKFLOW_" || c.MaxSessionsPerOwner != 8 || c.MaxSessions != 32 {
		t.Errorf("the safe defaults changed: %+v", c)
	}
}

func TestLoad_FileOverridesDefaults(t *testing.T) {
	c, err := core.Load(write(t, `{"kinds": {"claude": ["claude", "--verbose"]}, "max_sessions": 4}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Kinds) != 1 || c.Kinds["claude"][1] != "--verbose" || c.MaxSessions != 4 {
		t.Errorf("%+v", c)
	}
}

func TestLoad_Refusals(t *testing.T) {
	for name, body := range map[string]string{
		"empty catalog":                     `{"kinds": {}}`,
		"a kind with no argv":               `{"kinds": {"claude": []}}`,
		"an env policy that defeats itself": `{"env_allow_prefixes": [""]}`,
		"not JSON":                          `{`,
	} {
		if _, err := core.Load(write(t, body)); err == nil {
			t.Errorf("%s: must be refused", name)
		}
	}
}

func TestLoad_EnvOverridesTheFile(t *testing.T) {
	t.Setenv("AGENTD_MAX_SESSIONS", "3")
	c, err := core.Load(write(t, `{"max_sessions": 9}`))
	if err != nil || c.MaxSessions != 3 {
		t.Fatalf("%+v %v", c, err)
	}
}
