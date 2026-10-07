// Package hostinfo answers what `init` suggests and reports: the OS name, the
// country, the architecture, and which agent CLIs are on PATH.
//
// The country comes from the system TIME ZONE and the tzdata tables that ship
// with the OS — offline, on purpose. A geo-IP lookup would send this device's
// address to a third party to fill in a default the person can overwrite with
// one keystroke.
package hostinfo

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

// Info reads from a filesystem root ("/" in production, a fixture in tests).
type Info struct {
	Root    string
	GOOS    string
	TZ      string // $TZ, when set
	LookExe func(string) (string, error)
}

func New() Info {
	return Info{Root: "/", GOOS: runtime.GOOS, TZ: os.Getenv("TZ"), LookExe: exec.LookPath}
}

func (i Info) path(p string) string { return filepath.Join(i.Root, p) }

// OSName is /etc/os-release NAME ("Ubuntu"), "macOS" on darwin, else GOOS.
func (i Info) OSName() string {
	if i.GOOS == "darwin" {
		return "macOS"
	}
	for _, p := range []string{"/etc/os-release", "/usr/lib/os-release"} {
		f, err := os.Open(i.path(p))
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if v, ok := strings.CutPrefix(sc.Text(), "NAME="); ok {
				_ = f.Close()
				return strings.Trim(strings.TrimSpace(v), `"'`)
			}
		}
		_ = f.Close()
	}
	if i.GOOS == "" {
		return "Linux"
	}
	return strings.ToUpper(i.GOOS[:1]) + i.GOOS[1:]
}

// Arch is GOARCH.
func (Info) Arch() string { return runtime.GOARCH }

// Zone is the IANA time zone: $TZ, else /etc/localtime's link target, else
// /etc/timezone.
func (i Info) Zone() string {
	if z := strings.TrimPrefix(strings.TrimSpace(i.TZ), ":"); z != "" && !strings.HasPrefix(z, "/") {
		return z
	}
	if target, err := os.Readlink(i.path("/etc/localtime")); err == nil {
		if _, after, ok := strings.Cut(target, "zoneinfo/"); ok {
			return after
		}
	}
	if b, err := os.ReadFile(i.path("/etc/timezone")); err == nil {
		return strings.TrimSpace(string(b))
	}
	return ""
}

// Country maps the zone to a country name through zone.tab (one country per
// zone) and iso3166.tab. "" when unknown — UTC, a VM without tzdata.
func (i Info) Country() string {
	zone := i.Zone()
	if zone == "" {
		return ""
	}
	code := ""
	for _, dir := range []string{"/usr/share/zoneinfo", "/usr/share/lib/zoneinfo"} {
		code = scanTab(i.path(dir+"/zone.tab"), func(f []string) (string, bool) {
			return f[0], len(f) >= 3 && f[2] == zone
		})
		if code == "" {
			continue
		}
		if name := scanTab(i.path(dir+"/iso3166.tab"), func(f []string) (string, bool) {
			return f[1], len(f) >= 2 && f[0] == code
		}); name != "" {
			return name
		}
	}
	return ""
}

func scanTab(path string, match func([]string) (string, bool)) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		if v, ok := match(strings.Split(line, "\t")); ok {
			return v
		}
	}
	return ""
}

// KindsOnPath returns the catalog kinds whose program is on PATH, sorted. The
// host offers only these: the product's picker then lists, per host, exactly
// what that host can run.
func (i Info) KindsOnPath(catalog map[string][]string) []string {
	var out []string
	for kind, argv := range catalog {
		if len(argv) == 0 {
			continue
		}
		if _, err := i.LookExe(argv[0]); err == nil {
			out = append(out, kind)
		}
	}
	sort.Strings(out)
	return out
}
