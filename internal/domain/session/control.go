// Package session holds the daemon's rules with no dependency on tmux, gRPC or
// the network: what a session is, what state it may move to next, and how to
// read the bytes tmux control mode gives us.
package session

import (
	"strings"
)

// DecodeOutput undoes tmux control-mode escaping.
//
// MEASURED on tmux 3.4, 2026-09-26, rather than assumed — an earlier draft of
// the design claimed UTF-8 arrives escaped, and it does not. One %output line
// for `printf "тик\t\033[31mred\033[0m\\end\n"` came back as:
//
//	printf "тик\134t…"\015\012\033[?2004l\015тик\011\033[31mred\033[0m…
//
// So the rule is exactly: a backslash followed by THREE octal digits is one
// byte; everything else — including multi-byte UTF-8 — passes through
// untouched. The backslash itself is escaped the same way, as \134, not as \\.
//
// Anything that is not a well-formed escape is kept verbatim: this decoder is
// on the path of untrusted program output and must never panic or drop bytes
// it does not recognise.
func DecodeOutput(s string) []byte {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); {
		if s[i] == '\\' && i+3 < len(s) &&
			isOctal(s[i+1]) && isOctal(s[i+2]) && isOctal(s[i+3]) {
			v := (int(s[i+1]-'0') << 6) | (int(s[i+2]-'0') << 3) | int(s[i+3]-'0')
			out = append(out, byte(v))
			i += 4
			continue
		}
		out = append(out, s[i])
		i++
	}
	return out
}

func isOctal(b byte) bool { return b >= '0' && b <= '7' }

// EventKind classifies one line of the control-mode stream.
type EventKind int

const (
	EventOther EventKind = iota
	EventBegin
	EventEnd
	EventError
	EventOutput
	EventExit
)

// Event is one parsed control-mode line.
//
// Correlation is by CommandNum, never by arrival order: a reply block is framed
// by %begin/%end (or %error) carrying the command number tmux assigned, and two
// commands in flight interleave. Ordering the replies by when they arrive is
// the bug this type exists to prevent.
type Event struct {
	Kind       EventKind
	CommandNum int    // %begin / %end / %error
	Pane       string // %output
	Payload    string // %output data (still escaped — call DecodeOutput), or the rest of the line
}

// ParseLine reads one raw line from `tmux -C`. It never returns an error: an
// unrecognised line is EventOther, because a future tmux may add notifications
// we have no opinion about and dropping the stream over one would be worse.
func ParseLine(line string) Event {
	switch {
	case strings.HasPrefix(line, "%output "):
		rest := line[len("%output "):]
		pane, data, _ := strings.Cut(rest, " ")
		return Event{Kind: EventOutput, Pane: pane, Payload: data}
	case strings.HasPrefix(line, "%begin "):
		return Event{Kind: EventBegin, CommandNum: commandNum(line), Payload: line}
	case strings.HasPrefix(line, "%end "):
		return Event{Kind: EventEnd, CommandNum: commandNum(line), Payload: line}
	case strings.HasPrefix(line, "%error "):
		return Event{Kind: EventError, CommandNum: commandNum(line), Payload: line}
	case strings.HasPrefix(line, "%exit"):
		return Event{Kind: EventExit, Payload: line}
	default:
		return Event{Kind: EventOther, Payload: line}
	}
}

// commandNum reads the second field of "%begin <time> <num> <flags>".
func commandNum(line string) int {
	f := strings.Fields(line)
	if len(f) < 3 {
		return -1
	}
	n := 0
	for _, c := range f[2] {
		if c < '0' || c > '9' {
			return -1
		}
		n = n*10 + int(c-'0')
	}
	return n
}
