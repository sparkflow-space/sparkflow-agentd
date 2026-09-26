package tmux

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
)

// Control is the push half of the port: one `tmux -C attach` per session, whose
// stdout is a stream of %output notifications as the bytes appear.
//
// Why not poll capture-pane: capture-pane shows the SCREEN, so anything that
// scrolls past between two polls is gone, and a poll interval is either a
// latency floor or a busy loop. Control mode has neither problem.
//
// # Lifetimes
//
// A `stream` is keyed by tmux session name and OUTLIVES a single attach. That is
// not an implementation detail: the sequence counter and the replay buffer live
// on it, so an attach that dies (tmux server restart, a read error) and is
// replaced does not restart numbering at zero. An earlier version allocated both
// per attach, which meant a client resuming at from_seq=5000 was silently handed
// a stream whose seqs began again at 0 and threw away every chunk it received.
type Control struct {
	bin       string
	maxChunks int
	maxBytes  int
	logf      func(string, ...any)

	mu      sync.Mutex
	streams map[string]*stream
}

// Buffer defaults. Chunks alone are not a bound — one %output line can carry a
// megabyte — so the ring is capped by both.
const (
	DefaultBufChunks = 4096
	DefaultBufBytes  = 1 << 20
)

func NewControl(bin string, bufChunks, bufBytes int, logf func(string, ...any)) *Control {
	if bin == "" {
		bin = "tmux"
	}
	if bufChunks <= 0 {
		bufChunks = DefaultBufChunks
	}
	if bufBytes <= 0 {
		bufBytes = DefaultBufBytes
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Control{bin: bin, maxChunks: bufChunks, maxBytes: bufBytes, logf: logf,
		streams: map[string]*stream{}}
}

// stream owns one session's buffered output and fans it out to however many
// subscribers are watching.
type stream struct {
	c    *Control
	name string

	mu    sync.Mutex
	ring  []session.Chunk
	bytes int
	next  uint64
	subs  map[*subscriber]struct{}
	// pane is the pane whose output belongs to this session. Output from any
	// other pane — a window the agent opened for itself — is dropped rather than
	// interleaved into a transcript the client is painting as one terminal.
	pane     string
	attached bool
	ended    bool
	cancel   func()
	stdin    io.Closer
	// attachedAt and fastFails bound the automatic re-attach below: an attach
	// that dies instantly, over and over, must not become a fork loop.
	attachedAt time.Time
	fastFails  int
}

// maxFastReattaches is how many times in a row an attach may die within a
// second before the daemon stops trying and tells the subscribers.
const maxFastReattaches = 5

// subscriber is one Stream RPC.
//
// Each has a PRIVATE queue and exactly one writer goroutine, and that goroutine
// is the only thing that ever closes its channel. Both halves are load-bearing:
//
//   - sending to subscribers outside the lock while an unsubscribing goroutine
//     closed the channel outside the same lock is a `send on closed channel`
//     panic, in a goroutine nothing recovers — one client disconnecting at the
//     wrong microsecond took down the serving for every session. (`select` with
//     `default` does not help: the send case on a closed channel is always ready
//     and panics.)
//   - a replay goroutine draining the backlog into the same channel that live
//     publishing writes to interleaves them. Measured on a fake tmux emitting
//     20 000 lines: 562 of 16 570 chunks arrived out of order, the first
//     inversion delivering seq 796 after seq 1358 — corrupting the screen
//     exactly on the reconnect path the buffer exists to serve.
type subscriber struct {
	ch    chan session.Chunk
	queue []session.Chunk
	wake  chan struct{}
	// fin means nothing more will be enqueued: drain what is left, then close.
	fin bool
}

// Watch starts buffering a session's output without subscribing to it.
//
// Called right after Start so the transcript begins at the agent's first byte:
// a client that connects a second later would otherwise never see the banner or
// the first question.
func (c *Control) Watch(name, paneID string) error {
	s := c.streamFor(name)
	s.mu.Lock()
	if paneID != "" {
		s.pane = paneID
	}
	s.mu.Unlock()
	return s.ensureAttached()
}

// Subscribe delivers everything from `from` that is still buffered, then live
// output, until ctx is done or the session ends.
func (c *Control) Subscribe(ctx context.Context, name string, from uint64) (<-chan session.Chunk, error) {
	s := c.streamFor(name)
	if err := s.ensureAttached(); err != nil {
		return nil, err
	}

	sub := &subscriber{ch: make(chan session.Chunk, 64), wake: make(chan struct{}, 1)}
	s.mu.Lock()
	for _, ck := range s.ring {
		if ck.Seq >= from {
			sub.queue = append(sub.queue, ck)
		}
	}
	sub.fin = s.ended // an ended session replays its tail and then closes
	s.subs[sub] = struct{}{}
	s.mu.Unlock()

	go s.serve(ctx, sub)
	return sub.ch, nil
}

// Release drops a session's attach and buffer. Called when the session is known
// to be dead, so the ring is not retained for something that will never produce
// another byte.
func (c *Control) Release(name string) {
	c.mu.Lock()
	s, ok := c.streams[name]
	if ok {
		delete(c.streams, name)
	}
	c.mu.Unlock()
	if ok {
		s.shutdown()
	}
}

// Close stops every attached control-mode process. Called on daemon shutdown;
// the tmux sessions themselves are untouched, which is the point — they outlive
// us and are re-adopted on the next boot.
func (c *Control) Close() {
	c.mu.Lock()
	all := make([]*stream, 0, len(c.streams))
	for name, s := range c.streams {
		all = append(all, s)
		delete(c.streams, name)
	}
	c.mu.Unlock()
	for _, s := range all {
		s.shutdown()
	}
}

func (c *Control) streamFor(name string) *stream {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.streams[name]; ok {
		return s
	}
	s := &stream{c: c, name: name, subs: map[*subscriber]struct{}{}}
	c.streams[name] = s
	return s
}

// ensureAttached starts the control-mode client if it is not running. An ended
// session is never re-attached: there is nothing to attach to.
func (s *stream) ensureAttached() error {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return nil // the tail is still in the ring; subscribers get it, then EOF
	}
	if s.attached {
		s.mu.Unlock()
		return nil
	}
	s.attached = true
	s.mu.Unlock()

	if err := s.attach(); err != nil {
		s.mu.Lock()
		s.attached = false
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *stream) attach() error {
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, s.c.bin, "-C", "attach", "-t", exact(s.name))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return err
	}
	// Hold stdin open for the life of the attach. A control-mode client reads
	// commands from stdin and exits on EOF — with stdin left at /dev/null it
	// quits before the first %output, and the stream is silently empty. That is
	// exactly how this failed the first time it ran.
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return err
	}
	// tmux's own complaints go somewhere readable rather than to /dev/null: an
	// attach that dies for a reason nobody logged is the hardest kind of hang to
	// diagnose.
	var errb syncBuffer
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		cancel()
		return fmt.Errorf("tmux -C attach -t %s: %w", s.name, err)
	}

	s.mu.Lock()
	s.cancel, s.stdin, s.attachedAt = cancel, stdin, time.Now()
	s.mu.Unlock()

	if s.paneUnknown() {
		// A stream created by Subscribe (rather than Watch) does not know the
		// pane yet. Resolve it once, so filtering is possible at all.
		if p, err := s.c.paneOf(s.name); err == nil {
			s.mu.Lock()
			s.pane = p
			s.mu.Unlock()
		} else {
			s.c.logf("agentd: cannot resolve the pane of %s: %v; output from every pane of that session will be streamed", s.name, err)
		}
	}

	go s.read(stdout, cmd, &errb)
	return nil
}

func (c *Control) paneOf(name string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli := CLI{Bin: c.bin}
	out, err := cli.run(ctx, "display-message", "-p", "-t", name, "#{"+optPane+"}")
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(out, "%") {
		// Fall back to the active pane: a session adopted from an older daemon
		// carries no label.
		out, err = cli.run(ctx, "display-message", "-p", "-t", name, "#{pane_id}")
		if err != nil {
			return "", err
		}
	}
	if !strings.HasPrefix(out, "%") {
		return "", fmt.Errorf("tmux: no pane id for %s (got %q)", name, out)
	}
	return out, nil
}

func (s *stream) paneUnknown() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pane == ""
}

func (s *stream) read(stdout io.ReadCloser, cmd *exec.Cmd, errb *syncBuffer) {
	exited := false
	defer func() {
		_ = cmd.Wait()
		if msg := strings.TrimSpace(errb.String()); msg != "" {
			s.c.logf("agentd: tmux attach %s said: %s", s.name, msg)
		}
		s.detached(exited)
	}()

	sc := bufio.NewScanner(stdout)
	// Control-mode lines carry a whole burst of program output, which can be far
	// longer than bufio's default 64 KiB.
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	for sc.Scan() {
		ev := session.ParseLine(sc.Text())
		switch ev.Kind {
		case session.EventOutput:
			if !s.wants(ev.Pane) {
				continue
			}
			s.publish(session.Chunk{Data: session.DecodeOutput(ev.Payload), At: time.Now()})
		case session.EventExit:
			// The session is gone, or tmux is detaching us. Either way this
			// attach is over; whether the SESSION is over is checked below.
			exited = true
			return
		case session.EventError:
			// This stream issues no commands, so a %error is tmux complaining
			// about something we did not ask for — worth a line, not a crash.
			s.c.logf("agentd: tmux control error on %s: %s", s.name, ev.Payload)
		default:
			// %begin/%end frame command replies and other notifications (a
			// rename, a resize) are not errors. Ignored on purpose.
		}
	}
	if err := sc.Err(); err != nil {
		// Reaching here with an error and saying nothing is how a session goes
		// permanently silent with no explanation: bufio.ErrTooLong on a single
		// enormous %output line does exactly that.
		s.c.logf("agentd: reading the tmux stream of %s failed: %v", s.name, err)
	}
}

// detached runs when an attach ends. It always finishes the subscribers, so no
// Stream RPC is left waiting on a channel that will never deliver or close —
// which, because GracefulStop waits for in-flight RPCs, also used to mean the
// daemon could not be stopped without SIGKILL.
func (s *stream) detached(sawExit bool) {
	s.mu.Lock()
	alreadyEnded := s.ended
	lasted := time.Since(s.attachedAt)
	s.attached = false
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	if s.stdin != nil {
		_ = s.stdin.Close()
		s.stdin = nil
	}
	if lasted < time.Second {
		s.fastFails++
	} else {
		s.fastFails = 0
	}
	fails := s.fastFails
	hasSubs := len(s.subs) > 0
	s.mu.Unlock()

	// Only ask tmux when the answer can still change something: a stream already
	// released or shut down knows it is over, and probing for it would spawn a
	// subprocess for a session nobody will read again. The question is asked
	// whether or not %exit arrived, because an attach can also die on a read
	// error, and "the session is gone" is a different fact from "our pipe broke".
	dead := !alreadyEnded && !s.c.alive(s.name)

	if !dead && !alreadyEnded {
		// The session is alive and only our attach died (and nobody has released
		// this stream in the meantime — after Close or Release there is nothing
		// to go back to, and sleeping first would only delay the goroutine's
		// exit). Put it back rather than
		// ending the client's stream: the seq counter and the ring are on the
		// stream, so a new attach resumes the numbering and the client never
		// notices. Bounded, so an attach that cannot survive a second does not
		// become a loop.
		if hasSubs && fails <= maxFastReattaches {
			backoff := time.Duration(100*(1<<min(fails, 4))) * time.Millisecond
			time.Sleep(backoff)
			if err := s.ensureAttached(); err == nil {
				return
			}
			s.c.logf("agentd: re-attaching to %s failed; ending the stream", s.name)
		} else if hasSubs {
			s.c.logf("agentd: the attach to %s died %d times in a row; ending the stream", s.name, fails)
		}
	}

	s.mu.Lock()
	if dead {
		s.ended = true
	}
	subs := make([]*subscriber, 0, len(s.subs))
	for sub := range s.subs {
		sub.fin = true
		subs = append(subs, sub)
	}
	s.mu.Unlock()

	for _, sub := range subs {
		wake(sub)
	}
}

// alive asks tmux whether the session still exists. `has-session` accepts the
// exact-match target form, so a prefix cannot answer for a session that is gone.
func (c *Control) alive(name string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli := CLI{Bin: c.bin}
	_, err := cli.run(ctx, "has-session", "-t", exact(name))
	return err == nil
}

// shutdown stops the attach and finishes every subscriber.
func (s *stream) shutdown() {
	s.mu.Lock()
	s.ended = true
	s.attached = false
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	if s.stdin != nil {
		_ = s.stdin.Close()
		s.stdin = nil
	}
	s.ring, s.bytes = nil, 0
	subs := make([]*subscriber, 0, len(s.subs))
	for sub := range s.subs {
		sub.fin = true
		sub.queue = nil // dropped: the buffer is gone with the session
		subs = append(subs, sub)
	}
	s.mu.Unlock()
	for _, sub := range subs {
		wake(sub)
	}
}

func (s *stream) wants(pane string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	// An unknown pane streams everything: losing an agent's output is worse than
	// occasionally including a window it opened for itself, and the pane is
	// unknown only when tmux would not tell us.
	return s.pane == "" || pane == "" || pane == s.pane
}

// publish numbers a chunk, files it in the ring and hands it to every
// subscriber's queue. Everything happens under the one mutex: no channel send
// outside it, so there is no window in which a closing subscriber can be written
// to.
func (s *stream) publish(ck session.Chunk) {
	s.mu.Lock()
	ck.Seq = s.next
	s.next++
	s.ring = append(s.ring, ck)
	s.bytes += len(ck.Data)
	for len(s.ring) > s.c.maxChunks || (s.bytes > s.c.maxBytes && len(s.ring) > 1) {
		s.bytes -= len(s.ring[0].Data)
		s.ring = s.ring[1:]
	}
	subs := make([]*subscriber, 0, len(s.subs))
	for sub := range s.subs {
		if sub.fin {
			continue
		}
		sub.queue = append(sub.queue, ck)
		// A subscriber that cannot keep up loses its OLDEST pending chunks
		// rather than the newest: a client behind by more than the buffer wants
		// to be current, and the jump in Seq is its signal to refetch a
		// snapshot.
		if over := len(sub.queue) - s.c.maxChunks; over > 0 {
			sub.queue = sub.queue[over:]
		}
		subs = append(subs, sub)
	}
	s.mu.Unlock()

	for _, sub := range subs {
		wake(sub)
	}
}

func wake(sub *subscriber) {
	select {
	case sub.wake <- struct{}{}:
	default: // already signalled; the writer will see the queue
	}
}

// serve is the ONE writer of sub.ch, and the only thing that closes it.
func (s *stream) serve(ctx context.Context, sub *subscriber) {
	defer func() {
		s.drop(sub)
		close(sub.ch)
	}()
	for {
		s.mu.Lock()
		if len(sub.queue) == 0 {
			fin := sub.fin
			s.mu.Unlock()
			if fin {
				return
			}
			select {
			case <-sub.wake:
			case <-ctx.Done():
				return
			}
			continue
		}
		ck := sub.queue[0]
		sub.queue = sub.queue[1:]
		s.mu.Unlock()

		select {
		case sub.ch <- ck:
		case <-ctx.Done():
			return
		}
	}
}

func (s *stream) drop(sub *subscriber) {
	s.mu.Lock()
	delete(s.subs, sub)
	s.mu.Unlock()
}

// syncBuffer is a writer an exec.Cmd goroutine and the reader can share.
type syncBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Bounded: tmux's stderr is diagnostics, not a log sink.
	if len(b.buf) < 8192 {
		b.buf = append(b.buf, p...)
	}
	return len(p), nil
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}
