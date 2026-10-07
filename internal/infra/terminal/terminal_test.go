package terminal_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/terminal"
)

func TestLine_CancelledReadConsumesNothing(t *testing.T) {
	pr, pw := io.Pipe()
	var out bytes.Buffer
	term := terminal.New(pr, &out)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if _, err := term.Line(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want the deadline, got %v", err)
	}
	cancel()
	go func() { _, _ = pw.Write([]byte("Work laptop\r\n")) }()
	got, err := term.Line(context.Background())
	if err != nil || got != "Work laptop" {
		t.Fatalf("the next question must get the next line: %q %v", got, err)
	}
	_ = pw.Close()
	if _, err := term.Line(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("closed stdin: %v", err)
	}
	term.Say("hello")
	if !strings.Contains(out.String(), "hello\n") {
		t.Errorf("out = %q", out.String())
	}
}
