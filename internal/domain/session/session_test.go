package session_test

import (
	"errors"
	"testing"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
)

func TestCatalog_OnlyAllowListedKinds(t *testing.T) {
	c := session.Catalog{"claude": {"claude"}, "opencode-go": {"opencode", "--tui"}}

	if _, err := c.Resolve("claude"); err != nil {
		t.Fatalf("an allow-listed kind must resolve: %v", err)
	}
	for _, kind := range []string{"", "bash", "claude; rm -rf /", "CLAUDE", "../claude"} {
		if _, err := c.Resolve(kind); !errors.Is(err, session.ErrUnknownKind) {
			t.Errorf("Resolve(%q) must refuse with ErrUnknownKind, got %v", kind, err)
		}
	}
}

func TestCatalog_ResolveReturnsACopy(t *testing.T) {
	c := session.Catalog{"claude": {"claude", "--print"}}
	got, _ := c.Resolve("claude")
	got[0] = "evil"
	again, _ := c.Resolve("claude")
	if again[0] != "claude" {
		t.Fatal("Resolve must not hand out the catalog's own slice — a caller mutated it")
	}
}

func TestValidateWorkspace(t *testing.T) {
	const root = "/srv/agents"
	ok := []struct{ in, want string }{
		{"/srv/agents", "/srv/agents"},
		{"/srv/agents/p1", "/srv/agents/p1"},
		{"/srv/agents/p1/../p2", "/srv/agents/p2"},
	}
	for _, tc := range ok {
		got, err := session.ValidateWorkspace(root, tc.in)
		if err != nil || got != tc.want {
			t.Errorf("ValidateWorkspace(%q) = (%q, %v), want (%q, nil)", tc.in, got, err, tc.want)
		}
	}

	bad := []string{
		"",                      // empty
		"relative/path",         // not absolute
		"/etc/passwd",           // plainly outside
		"/srv/agents/../../etc", // climbs out after cleaning
		"/srv/agents-evil",      // prefix without a separator — the classic
	}
	for _, in := range bad {
		if _, err := session.ValidateWorkspace(root, in); err == nil {
			t.Errorf("ValidateWorkspace(%q) must be refused", in)
		}
	}
}

func TestState_ExitedIsTerminal(t *testing.T) {
	if session.StateExited.CanMoveTo(session.StateIdle) {
		t.Fatal("a dead process does not come back; a restart is a NEW session id")
	}
	if !session.StateWorking.CanMoveTo(session.StateWaitingInput) {
		t.Error("working → waiting_input is the everyday transition")
	}
	if session.StateIdle.CanMoveTo(session.StateStarting) {
		t.Error("only Start mints starting")
	}
}

func TestTmuxNaming_RoundTrips(t *testing.T) {
	const id = "9f2c"
	name := session.TmuxName(id)
	if !session.IsOurs(name) {
		t.Fatalf("%q must be recognised as ours", name)
	}
	got, ok := session.IDFromTmuxName(name)
	if !ok || got != id {
		t.Fatalf("IDFromTmuxName(%q) = (%q, %v), want (%q, true)", name, got, ok, id)
	}
	if _, ok := session.IDFromTmuxName("partner-claude"); ok {
		t.Error("a session the daemon did not create must not be adopted — there are such sessions on the dev host right now")
	}
}

func TestNormaliseSize_NeverReturnsTheTmuxDefault(t *testing.T) {
	cols, rows := session.NormaliseSize(0, 0)
	if cols == 80 && rows == 24 {
		t.Fatal("0 must not fall through to tmux's own 80x24 — that is the bug this function exists to prevent")
	}
	if cols <= 0 || rows <= 0 {
		t.Fatal("defaults must be positive")
	}
	if c, r := session.NormaliseSize(200, 60); c != 200 || r != 60 {
		t.Errorf("an explicit size must survive, got %dx%d", c, r)
	}
}
