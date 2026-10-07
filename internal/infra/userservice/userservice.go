// Package userservice installs `sparkflow-agentd run` as a USER service:
// a systemd user unit on Linux, a LaunchAgent on macOS. Never a system service
// and never with sudo — the daemon runs agents as the person who enrolled it,
// with their CLIs, their logins and their files.
package userservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
)

// Runner runs a command; faked in tests so no test ever touches the real
// service manager.
type Runner interface {
	Run(ctx context.Context, name string, args ...string) (string, error)
}

// ExecRunner is the real one.
type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, name string, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// Manager knows where the unit goes and how to start it.
type Manager struct {
	GOOS   string
	Home   string
	User   string
	UID    int
	Binary string // absolute path of sparkflow-agentd
	PATH   string // the PATH init ran with — so the service finds the same agent CLIs
	Run    Runner
}

// Result is what happened, as sentences for the person.
type Result struct {
	UnitPath string
	Started  bool
	// Notes are things the person should know or do (the linger hint).
	Notes []string
}

const unitName = "sparkflow-agentd.service"
const launchLabel = "space.sparkflow.agentd"

func (m Manager) unitPath() string {
	if m.GOOS == "darwin" {
		return filepath.Join(m.Home, "Library", "LaunchAgents", launchLabel+".plist")
	}
	return filepath.Join(m.Home, ".config", "systemd", "user", unitName)
}

var systemdUnit = template.Must(template.New("unit").Parse(`[Unit]
Description=Sparkflow agent host (sparkflow-agentd)
Documentation=https://github.com/sparkflow-space/sparkflow-agentd
After=network-online.target
Wants=network-online.target

[Service]
ExecStart={{.Binary}} run
Restart=on-failure
RestartSec=5
# Exit status 3 means the host was revoked or the sign-in is gone: restarting
# cannot fix it, only running "sparkflow-agentd init" again can.
RestartPreventExitStatus=3
# The PATH sparkflow-agentd init ran with: a user service starts with a
# minimal PATH and would not find agent CLIs installed in ~/.local/bin or by nvm.
Environment="PATH={{.PATH}}"

[Install]
WantedBy=default.target
`))

var launchAgent = template.Must(template.New("plist").Parse(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>` + launchLabel + `</string>
  <key>ProgramArguments</key>
  <array><string>{{.Binary}}</string><string>run</string></array>
  <key>EnvironmentVariables</key>
  <dict><key>PATH</key><string>{{.PATH}}</string></dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>
  <key>StandardOutPath</key><string>{{.Home}}/Library/Logs/sparkflow-agentd.log</string>
  <key>StandardErrorPath</key><string>{{.Home}}/Library/Logs/sparkflow-agentd.log</string>
</dict>
</plist>
`))

// Render returns the unit or plist text (golden-tested).
func (m Manager) Render() (string, error) {
	if !filepath.IsAbs(m.Binary) {
		return "", fmt.Errorf("the service needs an absolute path to sparkflow-agentd, got %q", m.Binary)
	}
	if strings.ContainsAny(m.Binary+m.PATH, "\n\"<>&") {
		return "", errors.New("the binary path or PATH contains characters a unit file cannot carry safely")
	}
	var b bytes.Buffer
	t := systemdUnit
	if m.GOOS == "darwin" {
		t = launchAgent
	}
	if err := t.Execute(&b, m); err != nil {
		return "", err
	}
	return b.String(), nil
}

// Commands are what Install runs — and what `init --no-service` prints instead.
func (m Manager) Commands() [][]string {
	if m.GOOS == "darwin" {
		return [][]string{{"launchctl", "bootstrap", "gui/" + strconv.Itoa(m.UID), m.unitPath()}}
	}
	return [][]string{{"systemctl", "--user", "daemon-reload"}, {"systemctl", "--user", "enable", "--now", unitName}}
}

// Install writes the unit and starts it.
func (m Manager) Install(ctx context.Context) (Result, error) {
	if m.GOOS != "linux" && m.GOOS != "darwin" {
		return Result{}, fmt.Errorf("installing a service is supported on Linux and macOS, not %s — run `sparkflow-agentd run` yourself", m.GOOS)
	}
	text, err := m.Render()
	if err != nil {
		return Result{}, err
	}
	path := m.unitPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return Result{}, err
	}
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		return Result{}, err
	}
	res := Result{UnitPath: path}
	for _, c := range m.Commands() {
		if out, err := m.Run.Run(ctx, c[0], c[1:]...); err != nil {
			return res, fmt.Errorf("`%s` failed: %v %s", strings.Join(c, " "), err, out)
		}
	}
	res.Started = true
	if m.GOOS == "linux" {
		res.Notes = append(res.Notes, m.linger(ctx)...)
	}
	return res, nil
}

// linger keeps a user service running after logout. Without it the daemon
// dies with the last session — which on a laptop is "when I close the lid
// and log out", silently. Try without privileges (polkit usually lets a user
// enable their own), and say exactly what to run when it cannot.
func (m Manager) linger(ctx context.Context) []string {
	out, _ := m.Run.Run(ctx, "loginctl", "show-user", m.User, "--property=Linger")
	if strings.TrimSpace(out) == "Linger=yes" {
		return nil
	}
	if _, err := m.Run.Run(ctx, "loginctl", "enable-linger", m.User); err == nil {
		return []string{"enabled lingering, so the service keeps running after you log out"}
	}
	return []string{"the service stops when you log out; to keep it running, run: sudo loginctl enable-linger " + m.User}
}

// Uninstall stops the service and removes the unit.
func (m Manager) Uninstall(ctx context.Context) error {
	if m.GOOS == "darwin" {
		_, _ = m.Run.Run(ctx, "launchctl", "bootout", "gui/"+strconv.Itoa(m.UID)+"/"+launchLabel)
	} else {
		_, _ = m.Run.Run(ctx, "systemctl", "--user", "disable", "--now", unitName)
	}
	if err := os.Remove(m.unitPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if m.GOOS == "linux" {
		_, _ = m.Run.Run(ctx, "systemctl", "--user", "daemon-reload")
	}
	return nil
}

// Status is the service manager's own answer.
func (m Manager) Status(ctx context.Context) string {
	if m.GOOS == "darwin" {
		out, err := m.Run.Run(ctx, "launchctl", "print", "gui/"+strconv.Itoa(m.UID)+"/"+launchLabel)
		if err != nil {
			return "not installed"
		}
		if strings.Contains(out, "state = running") {
			return "running"
		}
		return "installed, not running"
	}
	out, _ := m.Run.Run(ctx, "systemctl", "--user", "is-active", unitName)
	return out
}
