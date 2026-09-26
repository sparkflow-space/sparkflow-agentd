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
	p := filepath.Join(t.TempDir(), "agentd.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const good = `{"listen":"127.0.0.1:50077","workspace_root":"/srv/agents",
 "kinds":{"claude":["claude"]},
 "auth":{"jwks_url":"https://x/keys","issuer":"https://x","audience":"p"}}`

func TestLoad_RefusesAWildcardBind(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:50077", ":50077", "[::]:50077"} {
		p := write(t, strings.Replace(good, "127.0.0.1:50077", addr, 1))
		if _, err := core.Load(p); err == nil {
			t.Errorf("listen %q must be refused — this daemon runs other people's code", addr)
		}
	}
}

func TestLoad_RefusesAnEmptyCatalog(t *testing.T) {
	p := write(t, strings.Replace(good, `"kinds":{"claude":["claude"]},`, `"kinds":{},`, 1))
	if _, err := core.Load(p); err == nil {
		t.Fatal("an empty allow-list is a misconfiguration, not a safe default")
	}
}

func TestLoad_EnvOverridesTheFile(t *testing.T) {
	p := write(t, good)
	t.Setenv("AGENTD_AUDIENCE", "from-env")
	c, err := core.Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Auth.Audience != "from-env" {
		t.Fatalf("audience = %q; env must win so a deployment need not template a file", c.Auth.Audience)
	}
}

func TestLoad_RequiresAWorkspaceRoot(t *testing.T) {
	p := write(t, strings.Replace(good, `"workspace_root":"/srv/agents",`, "", 1))
	if _, err := core.Load(p); err == nil {
		t.Fatal("without a root every path is permitted")
	}
}
