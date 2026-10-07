// Package terminal is init's prompter: text to stdout (a CLI's user-facing
// output is never a log line), and lines from ONE stdin reader goroutine, so a
// question asked after the browser finished the sign-in does not race a reader
// still waiting for a pasted code.
package terminal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
)

// Terminal reads lines from in and writes to out.
type Terminal struct {
	out   io.Writer
	lines chan string
	mu    sync.Mutex
	err   error
}

func New(in io.Reader, out io.Writer) *Terminal {
	t := &Terminal{out: out, lines: make(chan string, 16)}
	go func() {
		sc := bufio.NewScanner(in)
		sc.Buffer(make([]byte, 64<<10), 64<<10)
		for sc.Scan() {
			t.lines <- strings.TrimRight(sc.Text(), "\r")
		}
		t.mu.Lock()
		t.err = sc.Err()
		if t.err == nil {
			t.err = io.EOF
		}
		t.mu.Unlock()
		close(t.lines)
	}()
	return t
}

func (t *Terminal) Say(s string) { _, _ = fmt.Fprintln(t.out, s) }

// Line returns the next line, or ctx's error without consuming anything.
func (t *Terminal) Line(ctx context.Context) (string, error) {
	select {
	case l, ok := <-t.lines:
		if !ok {
			t.mu.Lock()
			defer t.mu.Unlock()
			return "", t.err
		}
		return l, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}
