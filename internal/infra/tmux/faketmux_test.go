package tmux_test

// A FAKE TMUX, so the control-mode adapter has tests that do not need a real one.
//
// Why this exists: the adapter had no unit tests at all, and three of the bugs
// the first review found — chunks delivered out of order on a reconnect, a
// subscriber channel that is never closed when the session dies, and a sequence
// counter that restarted on re-attach — are all invisible to a test that only
// ever watches a real tmux print "hello". A scripted fake can produce a 2 000-line
// burst, a %output for a pane we do not own, a %error, and an attach that dies,
// which is exactly the set the task spec's test plan asks for.
//
// The fake is THIS TEST BINARY re-executed with AGENTD_FAKE_DIR set (the trick
// os/exec's own tests use), so there is no shell script and no build step: the
// adapter is handed os.Args[0] as its tmux binary.

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const fakeDirEnv = "AGENTD_FAKE_DIR"

func TestMain(m *testing.M) {
	if dir := os.Getenv(fakeDirEnv); dir != "" {
		os.Exit(fakeTmux(dir, os.Args[1:]))
	}
	os.Exit(m.Run())
}

// fakeDir is one test's scripted tmux.
type fakeDir struct{ path string }

func newFakeDir(t *testing.T) *fakeDir {
	t.Helper()
	d := &fakeDir{path: t.TempDir()}
	d.write("pane", "%0")
	d.write("alive", "1")
	t.Setenv(fakeDirEnv, d.path)
	return d
}

func (d *fakeDir) write(name, body string) {
	if err := os.WriteFile(filepath.Join(d.path, name), []byte(body), 0o600); err != nil {
		panic(err)
	}
}

func (d *fakeDir) read(name string) string {
	b, _ := os.ReadFile(filepath.Join(d.path, name))
	return string(b)
}

// bin is what the adapter is told to run.
func (d *fakeDir) bin() string { return os.Args[0] }

// --- the fake itself ------------------------------------------------------

func fakeTmux(dir string, args []string) int {
	sub := ""
	for _, a := range args {
		if !strings.HasPrefix(a, "-") {
			sub = a
			break
		}
	}
	if len(args) > 0 && args[0] == "-C" {
		sub = "attach"
	}
	appendLine(dir, "calls", sub+" "+strings.Join(args, " "))

	if msg, fail := failureFor(dir, sub); fail {
		fmt.Fprintln(os.Stderr, msg)
		return 1
	}

	switch sub {
	case "attach":
		return runScript(dir)
	case "display-message":
		fmt.Println(strings.TrimSpace(readFile(dir, "pane")))
		return 0
	case "has-session":
		if strings.TrimSpace(readFile(dir, "alive")) == "1" {
			return 0
		}
		fmt.Fprintln(os.Stderr, "can't find session")
		return 1
	case "list-sessions":
		out := readFile(dir, "sessions")
		if out != "" {
			fmt.Print(out)
		}
		return 0
	case "show-options":
		// The option name is the last argument.
		want := args[len(args)-1]
		for _, line := range strings.Split(readFile(dir, "labels"), "\n") {
			name, value, ok := strings.Cut(line, "\t")
			if ok && name == want {
				fmt.Println(value)
				return 0
			}
		}
		return 0 // -q: an unset option is an empty answer
	default: // new-session, set-option, send-keys, capture-pane, kill-session
		return 0
	}
}

// runScript plays the attach script. Directives:
//
//	raw <text>        print verbatim
//	out <pane> <s>    print "%output <pane> <s>"
//	burst <pane> <n>  print n %output lines, "chunk-<i>"
//	sleep <duration>
//	exit              print %exit and stop
//	hold              block until stdin closes (what a real attach does)
func runScript(dir string) int {
	w := bufio.NewWriter(os.Stdout)
	for _, line := range strings.Split(readFile(dir, "script"), "\n") {
		verb, rest, _ := strings.Cut(strings.TrimRight(line, "\r"), " ")
		switch verb {
		case "":
			continue
		case "raw":
			fmt.Fprintln(w, rest)
		case "out":
			pane, data, _ := strings.Cut(rest, " ")
			fmt.Fprintf(w, "%%output %s %s\n", pane, data)
		case "burst":
			pane, n, _ := strings.Cut(rest, " ")
			count, _ := strconv.Atoi(n)
			for i := 0; i < count; i++ {
				fmt.Fprintf(w, "%%output %s chunk-%d\\015\\012\n", pane, i)
			}
		case "sleep":
			w.Flush()
			d, _ := time.ParseDuration(rest)
			time.Sleep(d)
		case "exit":
			w.WriteString("%exit\n")
			w.Flush()
			return 0
		case "hold":
			w.Flush()
			_, _ = io.Copy(io.Discard, os.Stdin)
			return 0
		}
	}
	w.Flush()
	return 0
}

func failureFor(dir, sub string) (string, bool) {
	for _, line := range strings.Split(readFile(dir, "fail"), "\n") {
		name, msg, ok := strings.Cut(line, "\t")
		if ok && name == sub {
			return msg, true
		}
	}
	return "", false
}

func readFile(dir, name string) string {
	b, _ := os.ReadFile(filepath.Join(dir, name))
	return string(b)
}

func appendLine(dir, name, line string) {
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintln(f, line)
}

// Small helpers so the tests can format without importing fmt in two files.
func fmtSprintf(format string, v ...any) string { return fmt.Sprintf(format, v...) }

func itoa(i int) string { return strconv.Itoa(i) }
