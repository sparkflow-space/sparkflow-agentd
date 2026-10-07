package hostinfo_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/hostinfo"
)

func fixture(t *testing.T, files map[string]string, localtime string) string {
	t.Helper()
	root := t.TempDir()
	for p, body := range files {
		full := filepath.Join(root, p)
		_ = os.MkdirAll(filepath.Dir(full), 0o755)
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if localtime != "" {
		_ = os.MkdirAll(filepath.Join(root, "etc"), 0o755)
		if err := os.Symlink(localtime, filepath.Join(root, "etc/localtime")); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

var tz = map[string]string{
	"/usr/share/zoneinfo/zone.tab":    "# comment\nDE\t+5230+01322\tEurope/Berlin\tmost of Germany\nES\t+4024-00341\tEurope/Madrid\tSpain (mainland)\n",
	"/usr/share/zoneinfo/iso3166.tab": "# comment\nDE\tGermany\nES\tSpain\n",
}

func TestOSNameAndCountry(t *testing.T) {
	files := map[string]string{"/etc/os-release": "PRETTY_NAME=\"Ubuntu 24.04 LTS\"\nNAME=\"Ubuntu\"\nID=ubuntu\n"}
	for k, v := range tz {
		files[k] = v
	}
	root := fixture(t, files, "/usr/share/zoneinfo/Europe/Berlin")
	i := hostinfo.Info{Root: root, GOOS: "linux"}
	if got := i.OSName(); got != "Ubuntu" {
		t.Errorf("OSName = %q", got)
	}
	if got := i.Country(); got != "Germany" {
		t.Errorf("Country from /etc/localtime = %q", got)
	}
	i.TZ = "Europe/Madrid"
	if got := i.Country(); got != "Spain" {
		t.Errorf("$TZ must win: %q", got)
	}
	i.TZ = "Etc/UTC"
	if got := i.Country(); got != "" {
		t.Errorf("UTC has no country: %q", got)
	}
	if got := (hostinfo.Info{Root: root, GOOS: "darwin"}).OSName(); got != "macOS" {
		t.Errorf("darwin: %q", got)
	}
	if got := (hostinfo.Info{Root: t.TempDir(), GOOS: "linux"}).OSName(); got != "Linux" {
		t.Errorf("no os-release: %q", got)
	}
}

func TestKindsOnPath(t *testing.T) {
	i := hostinfo.Info{LookExe: func(p string) (string, error) {
		if strings.HasPrefix(p, "cl") || p == "qwen" {
			return "/usr/bin/" + p, nil
		}
		return "", errors.New("not found")
	}}
	got := i.KindsOnPath(map[string][]string{"claude": {"claude"}, "qwen": {"qwen", "--tui"}, "kimi": {"kimi"}, "empty": {}})
	if strings.Join(got, ",") != "claude,qwen" {
		t.Errorf("kinds = %v", got)
	}
}
