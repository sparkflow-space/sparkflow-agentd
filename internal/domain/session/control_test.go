package session_test

import (
	"testing"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
)

func TestDecodeOutput_TheLineTmuxActuallySent(t *testing.T) {
	// Captured verbatim from `tmux -C` on tmux 3.4, 2026-09-26. Keeping the
	// real payload as the fixture is the point: a hand-written one only ever
	// confirms the forms you already suspect.
	const raw = `printf "тик\134t\134033[31mred\134033[0m\134\134end\134n"\015\012\033[?2004l\015тик\011\033[31mred\033[0m`

	got := string(session.DecodeOutput(raw))

	if want := "\r\n"; !contains(got, want) {
		t.Fatalf("\\015\\012 must decode to CRLF; got %q", got)
	}
	if !contains(got, "\x1b[31mred\x1b[0m") {
		t.Errorf("\\033 must decode to ESC; got %q", got)
	}
	if !contains(got, `\t`) {
		t.Errorf(`\134t must decode to a literal backslash + t; got %q`, got)
	}
	if !contains(got, "тик") {
		t.Errorf("UTF-8 passes through untouched — the design's first draft claimed otherwise; got %q", got)
	}
	if !contains(got, "\x09") {
		t.Errorf("\\011 must decode to TAB; got %q", got)
	}
}

func TestDecodeOutput_MalformedEscapesSurvive(t *testing.T) {
	// This decoder sits on the path of untrusted program output.
	for _, tc := range []struct{ in, want string }{
		{`\19`, `\19`},     // not three octal digits
		{`\8888`, `\8888`}, // 8 is not octal
		{`\01`, `\01`},     // truncated at end of line
		{`\`, `\`},         // a lone backslash
		{``, ``},           // empty
		// Exactly three digits, greedily: \010 is octal 8 (backspace) and the
		// trailing 1 is data. Written the other way round first — expecting
		// \101 = 'A' — which is the mistake this case now pins.
		{`\0101`, "\x081"},
	} {
		if got := string(session.DecodeOutput(tc.in)); got != tc.want {
			t.Errorf("DecodeOutput(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestParseLine(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		kind session.EventKind
		num  int
		pane string
	}{
		{"output", `%output %5 hello\015`, session.EventOutput, 0, "%5"},
		{"begin", "%begin 1790429699 327 0", session.EventBegin, 327, ""},
		{"end", "%end 1790429699 327 0", session.EventEnd, 327, ""},
		{"error", "%error 1790429699 328 0", session.EventError, 328, ""},
		{"exit", "%exit", session.EventExit, 0, ""},
		{"unknown notification is not an error", "%window-renamed @3 sleep", session.EventOther, 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev := session.ParseLine(tc.in)
			if ev.Kind != tc.kind {
				t.Fatalf("kind = %v, want %v", ev.Kind, tc.kind)
			}
			if tc.num != 0 && ev.CommandNum != tc.num {
				t.Errorf("command number = %d, want %d — replies correlate by number, never by arrival order", ev.CommandNum, tc.num)
			}
			if tc.pane != "" && ev.Pane != tc.pane {
				t.Errorf("pane = %q, want %q", ev.Pane, tc.pane)
			}
		})
	}
}

func contains(h, n string) bool {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return true
		}
	}
	return false
}
