//go:build tmux

package tmux_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/tmux"
)

// Stop's KILL must succeed for a session that is already gone, whatever tmux
// says about it. Found on dev 2026-10-07: with the daemon's control-mode
// attach keeping an otherwise EMPTY server alive, `kill-session` answered
// "no current target" — which gone() did not know — so Stop failed for a
// session that no longer existed.
func TestKill_IsIdempotentWithAControlAttach(t *testing.T) {
	cli := tmux.NewCLI("")
	ctrl := tmux.NewControl("", 1024, 1<<20, func(format string, v ...any) { fmt.Printf("control: "+format+"\n", v...) })
	defer ctrl.Close()
	sess := session.Session{ID: fmt.Sprintf("%016x", time.Now().UnixNano()), Owner: "sub-it", Kind: "bash", Workspace: "/tmp", Cols: 80, Rows: 24}
	sess.TmuxName = session.TmuxName(sess.ID)
	ctx := context.Background()
	pane, err := cli.Start(ctx, sess, []string{"bash", "--norc"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sess.PaneID = pane
	if err := ctrl.Watch(sess.TmuxName, pane); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if err := cli.Signal(ctx, sess, session.SignalKill); err != nil {
		t.Fatalf("first KILL: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if err := cli.Signal(ctx, sess, session.SignalKill); err != nil {
		t.Fatalf("KILL of a session that is already gone must succeed: %v", err)
	}
}
