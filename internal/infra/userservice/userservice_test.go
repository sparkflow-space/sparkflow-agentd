package userservice_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/userservice"
)

type fakeRunner struct {
	calls  []string
	linger string
	fail   map[string]bool
}

func (f *fakeRunner) Run(_ context.Context, name string, args ...string) (string, error) {
	c := name + " " + strings.Join(args, " ")
	f.calls = append(f.calls, c)
	if f.fail[c] {
		return "denied", errors.New("exit 1")
	}
	if strings.HasPrefix(c, "loginctl show-user") {
		return f.linger, nil
	}
	return "", nil
}

func mgr(t *testing.T, goos string, r *fakeRunner) userservice.Manager {
	return userservice.Manager{GOOS: goos, Home: t.TempDir(), User: "boris", UID: 1000,
		Binary: "/home/boris/go/bin/sparkflow-agentd", PATH: "/home/boris/.local/bin:/usr/bin", Run: r}
}

func TestSystemdUnit_Golden(t *testing.T) {
	got, err := mgr(t, "linux", &fakeRunner{}).Render()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"ExecStart=/home/boris/go/bin/sparkflow-agentd run\n",
		"Restart=on-failure\n",
		"RestartPreventExitStatus=3\n",
		`Environment="PATH=/home/boris/.local/bin:/usr/bin"`,
		"WantedBy=default.target\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("unit lacks %q:\n%s", want, got)
		}
	}
}

func TestLaunchAgent_Golden(t *testing.T) {
	got, _ := mgr(t, "darwin", &fakeRunner{}).Render()
	for _, want := range []string{"<string>space.sparkflow.agentd</string>", "<string>/home/boris/go/bin/sparkflow-agentd</string><string>run</string>", "<key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>"} {
		if !strings.Contains(got, want) {
			t.Errorf("plist lacks %q:\n%s", want, got)
		}
	}
}

func TestRender_RefusesUnsafeInput(t *testing.T) {
	m := mgr(t, "linux", &fakeRunner{})
	m.Binary = "sparkflow-agentd"
	if _, err := m.Render(); err == nil {
		t.Error("a relative binary path must be refused")
	}
	m = mgr(t, "linux", &fakeRunner{})
	m.PATH = "/bin\nExecStartPre=/bin/evil"
	if _, err := m.Render(); err == nil {
		t.Error("a newline in PATH would inject a directive")
	}
}

func TestInstall_Linux_LingerTriedThenHinted(t *testing.T) {
	r := &fakeRunner{linger: "Linger=no", fail: map[string]bool{"loginctl enable-linger boris": true}}
	m := mgr(t, "linux", r)
	res, err := m.Install(context.Background())
	if err != nil || !res.Started {
		t.Fatalf("%+v %v", res, err)
	}
	if _, err := os.Stat(res.UnitPath); err != nil || !strings.HasSuffix(res.UnitPath, ".config/systemd/user/sparkflow-agentd.service") {
		t.Fatalf("unit path %q: %v", res.UnitPath, err)
	}
	joined := strings.Join(r.calls, "\n")
	if !strings.Contains(joined, "systemctl --user enable --now sparkflow-agentd.service") || strings.Contains(joined, "sudo") {
		t.Errorf("calls: %s", joined)
	}
	if len(res.Notes) != 1 || !strings.Contains(res.Notes[0], "sudo loginctl enable-linger boris") {
		t.Errorf("a failed linger must say what to run: %v", res.Notes)
	}

	r2 := &fakeRunner{linger: "Linger=yes"}
	if res, _ := mgr(t, "linux", r2).Install(context.Background()); len(res.Notes) != 0 {
		t.Errorf("already lingering: %v", res.Notes)
	}
}

func TestInstall_AFailedStartIsAnError(t *testing.T) {
	r := &fakeRunner{fail: map[string]bool{"systemctl --user enable --now sparkflow-agentd.service": true}}
	if res, err := mgr(t, "linux", r).Install(context.Background()); err == nil || res.Started {
		t.Fatalf("must not report 'installed' for a service that did not start: %+v %v", res, err)
	}
}
