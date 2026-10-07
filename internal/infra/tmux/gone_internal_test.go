package tmux

import (
	"errors"
	"testing"
)

func TestGone_KnowsEveryWayTmuxSaysTheSessionIsGone(t *testing.T) {
	for _, msg := range []string{
		"tmux kill-session -t =sfagent-1: exit status 1: can't find session: sfagent-1",
		"tmux display-message: exit status 1: can't find pane: %3",
		"no server running on /tmp/tmux-0/default",
		// measured on dev 2026-10-07, tmux 3.4: an empty server kept alive
		// by the control-mode attach
		"tmux kill-session -t =sfagent-1: exit status 1: no current target",
	} {
		if !gone(errors.New(msg)) {
			t.Errorf("gone(%q) = false", msg)
		}
	}
	if gone(errors.New("permission denied")) || gone(nil) {
		t.Error("only a missing session is 'gone'")
	}
}
