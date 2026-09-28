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

// good is the developer configuration: loopback, and plaintext explicitly
// allowed. Every other shape needs mutual TLS, which is what validateTLS tests
// below assert.
const good = `{"listen":"127.0.0.1:50077","workspace_root":"/srv/agents",
 "kinds":{"claude":["claude"]},
 "tls":{"allow_plaintext_loopback":true},
 "auth":{"jwks_url":"https://x/keys","issuer":"https://x","audience":"p"}}`

// The address is PARSED, not compared against a list of spellings. The three
// obvious ones were the only ones an earlier version caught, and "[::0]" and
// "[0:0:0:0:0:0:0:0]" both bind every interface while passing that check — which
// is precisely the exposure the rule exists to prevent.
func TestLoad_RefusesAWildcardBind(t *testing.T) {
	for _, addr := range []string{
		"0.0.0.0:50077", ":50077", "[::]:50077",
		"[::0]:50077", "[0:0:0:0:0:0:0:0]:50077", "0.0.0.0.0:50077",
		"localhost:50077", // a name may resolve anywhere; the interface must be explicit
		"127.0.0.1:http",  // a port that is not a number
	} {
		p := write(t, strings.Replace(good, "127.0.0.1:50077", addr, 1))
		if _, err := core.Load(p); err == nil {
			t.Errorf("listen %q must be refused — this daemon runs other people's code", addr)
		}
	}
}

func TestLoad_AcceptsAnExplicitPrivateAddressWithTLS(t *testing.T) {
	dir := t.TempDir()
	certs := map[string]string{}
	for _, n := range []string{"cert.pem", "key.pem", "ca.pem"} {
		f := filepath.Join(dir, n)
		if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		certs[n] = f
	}
	body := `{"listen":"10.1.2.3:50077","workspace_root":"/srv/agents",
	 "kinds":{"claude":["claude"]},
	 "tls":{"cert_file":"` + certs["cert.pem"] + `","key_file":"` + certs["key.pem"] + `","client_ca_file":"` + certs["ca.pem"] + `"},
	 "auth":{"jwks_url":"https://x/keys","issuer":"https://x","audience":"p"}}`
	if _, err := core.Load(write(t, body)); err != nil {
		t.Fatalf("a private address with mutual TLS must be accepted: %v", err)
	}
}

// The design names this hop: a person's bearer token crosses it and grants code
// execution on the host for its whole lifetime. Plaintext is allowed only on
// loopback, and only when it is asked for by name.
func TestLoad_RefusesPlaintextOffLoopback(t *testing.T) {
	body := strings.Replace(good, "127.0.0.1:50077", "10.1.2.3:50077", 1)
	if _, err := core.Load(write(t, body)); err == nil {
		t.Fatal("allow_plaintext_loopback must not excuse a routable address")
	}
	// ...and without the flag even loopback needs TLS configured.
	bare := strings.Replace(good, `"tls":{"allow_plaintext_loopback":true},`, "", 1)
	if _, err := core.Load(write(t, bare)); err == nil {
		t.Fatal("plaintext must be opted into explicitly")
	}
}

// Server-only TLS would encrypt the hop and authenticate nobody, so a half
// configuration is refused rather than silently downgraded.
func TestLoad_RefusesHalfConfiguredTLS(t *testing.T) {
	f := filepath.Join(t.TempDir(), "cert.pem")
	if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	body := strings.Replace(good, `"tls":{"allow_plaintext_loopback":true},`,
		`"tls":{"cert_file":"`+f+`","key_file":"`+f+`"},`, 1)
	if _, err := core.Load(write(t, body)); err == nil {
		t.Fatal("mutual TLS is all three files or none")
	}
}

func TestLoad_RefusesAWorkspaceRootThatPermitsEverything(t *testing.T) {
	for _, root := range []string{"/", "//", "srv/agents", "./agents"} {
		body := strings.Replace(good, `"workspace_root":"/srv/agents"`, `"workspace_root":"`+root+`"`, 1)
		if _, err := core.Load(write(t, body)); err == nil {
			t.Errorf("workspace_root %q must be refused", root)
		}
	}
}

func TestLoad_RefusesAnEnvPolicyThatDefeatsItself(t *testing.T) {
	for _, prefixes := range []string{`[""]`, `["LD_"]`, `["  "]`} {
		body := strings.Replace(good, `"kinds":{"claude":["claude"]},`,
			`"kinds":{"claude":["claude"]},"env_allow_prefixes":`+prefixes+`,`, 1)
		if _, err := core.Load(write(t, body)); err == nil {
			t.Errorf("env_allow_prefixes %s must be refused", prefixes)
		}
	}
}

func TestLoad_DefaultsAreTheSafeOnes(t *testing.T) {
	c, err := core.Load(write(t, good))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.EnvPolicy().AllowPrefixes; len(got) != 1 || got[0] != "SPARKFLOW_" {
		t.Errorf("default env prefixes = %v", got)
	}
	if c.MaxSessions <= 0 || c.MaxSessionsPerOwner <= 0 || c.ReconcileSeconds <= 0 {
		t.Errorf("limits must default to something finite: %+v", c)
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
