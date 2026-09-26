package session

// Signal is what the caller may send to a running agent. Explicit, rather than
// "write the escape sequence yourself": a CLI agent handles Ctrl-C meaningfully
// (it can flush and exit), and only an unresponsive one earns KILL.
type Signal int

const (
	SignalInt Signal = iota + 1
	SignalTerm
	SignalKill
)

func (s Signal) String() string {
	switch s {
	case SignalInt:
		return "INT"
	case SignalTerm:
		return "TERM"
	case SignalKill:
		return "KILL"
	default:
		return "unknown"
	}
}

// Valid reports whether s is one of the three the daemon accepts. An unknown
// signal is refused rather than defaulted: defaulting to KILL would be
// destructive, and defaulting to INT would silently do the wrong thing.
func (s Signal) Valid() bool { return s >= SignalInt && s <= SignalKill }
