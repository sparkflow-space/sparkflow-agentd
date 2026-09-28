package tmux_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/tmux"
)

type recorder struct {
	mu    sync.Mutex
	lines []string
}

func (r *recorder) logf(format string, v ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, strings.TrimSpace(sprintf(format, v...)))
}

func (r *recorder) contains(sub string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, l := range r.lines {
		if strings.Contains(l, sub) {
			return true
		}
	}
	return false
}

func sprintf(format string, v ...any) string {
	return strings.TrimSpace(fmtSprintf(format, v...))
}

// drain reads chunks until `want` of them arrive, the channel closes, or the
// deadline passes. It returns what it got and whether the channel closed.
func drain(t *testing.T, ch <-chan session.Chunk, want int, within time.Duration) ([]session.Chunk, bool) {
	t.Helper()
	var got []session.Chunk
	deadline := time.After(within)
	for len(got) < want {
		select {
		case ck, ok := <-ch:
			if !ok {
				return got, true
			}
			got = append(got, ck)
		case <-deadline:
			return got, false
		}
	}
	return got, false
}

// The spec's test plan, in one test: a %output for a pane we do not own is
// ignored, %begin/%end framing is not mistaken for output, and a %error is
// LOGGED rather than swallowed or published.
func TestControl_FiltersForeignPanesAndFramesReplies(t *testing.T) {
	d := newFakeDir(t)
	d.write("script", strings.Join([]string{
		"raw %begin 1758900000 268 0",
		"raw %end 1758900000 268 0",
		"out %0 first\\015\\012",
		"out %9 SECONDWINDOW",
		"raw %error 1758900000 269 0 nope",
		"out %0 second",
		"hold",
	}, "\n"))

	rec := &recorder{}
	c := tmux.NewControl(d.bin(), 1024, 1<<20, rec.logf)
	defer c.Close()

	if err := c.Watch("sfagent-0123456789abcdef", "%0"); err != nil {
		t.Fatal(err)
	}
	ch, err := c.Subscribe(context.Background(), "sfagent-0123456789abcdef", 0)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := drain(t, ch, 2, 3*time.Second)
	if len(got) != 2 {
		t.Fatalf("got %d chunks, want 2: %q", len(got), texts(got))
	}
	if string(got[0].Data) != "first\r\n" {
		t.Errorf("chunk 0 = %q; the octal escapes must be decoded", got[0].Data)
	}
	if string(got[1].Data) != "second" {
		t.Errorf("chunk 1 = %q — output from pane %%9 must not be interleaved", got[1].Data)
	}
	if got[0].Seq != 0 || got[1].Seq != 1 {
		t.Errorf("seqs = %d,%d, want 0,1 (a skipped pane must not consume a number)", got[0].Seq, got[1].Seq)
	}
	if !rec.contains("control error") {
		t.Errorf("a %%error line must be logged; log = %v", rec.lines)
	}
}

// The reconnect path, which is the one the buffer exists for. A backlog replay
// racing the live fan-out used to interleave: measured 562 inversions in 16 570
// chunks, the first delivering seq 796 after seq 1358.
func TestControl_ReplayAndLiveOutputStayInOrder(t *testing.T) {
	const n = 2000
	d := newFakeDir(t)
	d.write("script", "burst %0 2000\nhold\n")

	c := tmux.NewControl(d.bin(), 8192, 1<<24, func(string, ...any) {})
	defer c.Close()
	const name = "sfagent-0123456789abcdef"
	if err := c.Watch(name, "%0"); err != nil {
		t.Fatal(err)
	}
	// Subscribe WHILE the burst is flowing, from the beginning: part of the
	// stream comes out of the ring and the rest arrives live.
	ch, err := c.Subscribe(context.Background(), name, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := drain(t, ch, n, 10*time.Second)
	if len(got) != n {
		t.Fatalf("got %d chunks, want %d — nothing should be dropped with a buffer this size", len(got), n)
	}
	for i, ck := range got {
		if ck.Seq != uint64(i) {
			t.Fatalf("chunk %d carries seq %d: the replay and the live stream are interleaved", i, ck.Seq)
		}
		if want := "chunk-" + itoa(i) + "\r\n"; string(ck.Data) != want {
			t.Fatalf("chunk %d = %q, want %q", i, ck.Data, want)
		}
	}
}

// When the session ends, a Stream must END. It used to hang forever: the
// subscriber channels were never closed, so the client saw a live stream that
// would never deliver another byte — and because GracefulStop waits for
// in-flight RPCs, the daemon could then only be stopped with SIGKILL.
func TestControl_ClosesSubscribersWhenTheSessionEnds(t *testing.T) {
	d := newFakeDir(t)
	d.write("script", "out %0 bye\nexit\n")
	d.write("alive", "0") // has-session says it is gone

	c := tmux.NewControl(d.bin(), 1024, 1<<20, func(string, ...any) {})
	defer c.Close()
	const name = "sfagent-0123456789abcdef"
	ch, err := c.Subscribe(context.Background(), name, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, closed := drain(t, ch, 3, 5*time.Second)
	if !closed {
		t.Fatal("the channel must close when the session ends, not block forever")
	}
	if len(got) != 1 || string(got[0].Data) != "bye" {
		t.Errorf("the tail must still be delivered before the close, got %q", texts(got))
	}
}

// The sequence counter and the buffer belong to the SESSION, not to one attach.
// A client resuming at from_seq=N against a counter that restarted at 0 receives
// numbers it has already seen and throws away real output.
func TestControl_SeqSurvivesAReattach(t *testing.T) {
	d := newFakeDir(t)
	d.write("script", "out %0 a\nout %0 b\nexit\n")
	d.write("alive", "1") // the SESSION is fine; only this attach died

	c := tmux.NewControl(d.bin(), 1024, 1<<20, func(string, ...any) {})
	defer c.Close()
	const name = "sfagent-0123456789abcdef"
	if err := c.Watch(name, "%0"); err != nil {
		t.Fatal(err)
	}
	// Let the first attach finish and die.
	first, _ := c.Subscribe(context.Background(), name, 0)
	if got, _ := drain(t, first, 2, 3*time.Second); len(got) != 2 {
		t.Fatalf("first attach delivered %q", texts(got))
	}

	// Re-subscribing replays seq 0,1 from the ring; the attach the daemon puts
	// back must continue the numbering at 2, not start again at 0.
	ch, err := c.Subscribe(context.Background(), name, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := drain(t, ch, 4, 10*time.Second)
	if len(got) < 4 {
		t.Fatalf("got %d chunks after the re-attach, want at least 4: %q", len(got), texts(got))
	}
	for i, ck := range got[:4] {
		if ck.Seq != uint64(i) {
			t.Fatalf("chunk %d carries seq %d; the counter restarted", i, ck.Seq)
		}
	}
	if string(got[2].Data) != "a" || string(got[3].Data) != "b" {
		t.Errorf("the replayed attach delivered %q", texts(got[:4]))
	}
}

// A client disconnecting mid-burst must not take the daemon down. Publishing
// outside the lock while an unsubscribing goroutine closed the channel outside
// the same lock is a `send on closed channel` panic in a goroutine nothing
// recovers. Run with -race, which the gates do.
func TestControl_ASubscriberLeavingMidBurstIsSafe(t *testing.T) {
	d := newFakeDir(t)
	d.write("script", "burst %0 4000\nhold\n")

	c := tmux.NewControl(d.bin(), 8192, 1<<24, func(string, ...any) {})
	defer c.Close()
	const name = "sfagent-0123456789abcdef"
	if err := c.Watch(name, "%0"); err != nil {
		t.Fatal(err)
	}

	stay, err := c.Subscribe(context.Background(), name, 0)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		ch, err := c.Subscribe(ctx, name, 0)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer cancel()
			n := 0
			for range ch {
				if n++; n > 5 {
					return // leave early, mid-flight
				}
			}
		}()
	}
	if got, _ := drain(t, stay, 4000, 15*time.Second); len(got) != 4000 {
		t.Fatalf("the surviving subscriber got %d of 4000 chunks", len(got))
	}
	wg.Wait()
}

// Release drops a dead session's attach and buffer, and finishes whoever was
// watching — otherwise an idle stream keeps its process and its ring for the
// daemon's lifetime.
func TestControl_ReleaseFinishesSubscribers(t *testing.T) {
	d := newFakeDir(t)
	d.write("script", "out %0 x\nhold\n")

	c := tmux.NewControl(d.bin(), 1024, 1<<20, func(string, ...any) {})
	defer c.Close()
	const name = "sfagent-0123456789abcdef"
	ch, err := c.Subscribe(context.Background(), name, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := drain(t, ch, 1, 3*time.Second); len(got) != 1 {
		t.Fatalf("expected the first chunk, got %q", texts(got))
	}
	c.Release(name)
	select {
	case _, ok := <-ch:
		if ok {
			// A chunk may still be in flight; the next read must be the close.
			if _, ok := <-ch; ok {
				t.Fatal("Release must finish the subscriber")
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Release did not close the subscriber's channel")
	}
}

func texts(cks []session.Chunk) []string {
	out := make([]string, 0, len(cks))
	for _, c := range cks {
		out = append(out, string(c.Data))
	}
	return out
}
