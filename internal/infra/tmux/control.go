package tmux

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
)

// Control is the push half of the port: one `tmux -C attach` per session,
// whose stdout is a stream of %output notifications as the bytes appear.
//
// Why not poll capture-pane: capture-pane shows the SCREEN, so anything that
// scrolls past between two polls is gone, and a poll interval is either a
// latency floor or a busy loop. Control mode has neither problem.
type Control struct {
	bin string

	mu      sync.Mutex
	streams map[string]*stream // by tmux session name
	bufSize int
}

func NewControl(bin string, bufSize int) *Control {
	if bin == "" {
		bin = "tmux"
	}
	if bufSize <= 0 {
		bufSize = 4096
	}
	return &Control{bin: bin, streams: map[string]*stream{}, bufSize: bufSize}
}

// stream owns one attached control-mode process and fans its output out to
// however many subscribers are watching that session.
type stream struct {
	mu   sync.Mutex
	ring []session.Chunk // bounded history, so a reconnecting client can catch up
	max  int
	next uint64
	subs map[chan session.Chunk]uint64 // subscriber → next seq it wants
	stop func()
	done chan struct{}
}

// Subscribe delivers everything from `from` that is still buffered, then live
// output, until ctx is done.
func (c *Control) Subscribe(ctx context.Context, name string, from uint64) (<-chan session.Chunk, error) {
	s, err := c.streamFor(name)
	if err != nil {
		return nil, err
	}
	ch := make(chan session.Chunk, 256)

	s.mu.Lock()
	backlog := make([]session.Chunk, 0, len(s.ring))
	for _, ck := range s.ring {
		if ck.Seq >= from {
			backlog = append(backlog, ck)
		}
	}
	s.subs[ch] = from
	s.mu.Unlock()

	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.subs, ch)
			s.mu.Unlock()
			close(ch)
		}()
		for _, ck := range backlog {
			select {
			case ch <- ck:
			case <-ctx.Done():
				return
			}
		}
		<-ctx.Done()
	}()
	return ch, nil
}

// Close stops every attached control-mode process. Called on daemon shutdown;
// the tmux sessions themselves are untouched, which is the point — they outlive
// us and are re-adopted on the next boot.
func (c *Control) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, s := range c.streams {
		s.stop()
		delete(c.streams, name)
	}
}

func (c *Control) streamFor(name string) (*stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.streams[name]; ok {
		select {
		case <-s.done: // the attach died; drop it and start a fresh one
			delete(c.streams, name)
		default:
			return s, nil
		}
	}
	s, err := c.attach(name)
	if err != nil {
		return nil, err
	}
	c.streams[name] = s
	return s, nil
}

func (c *Control) attach(name string) (*stream, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, c.bin, "-C", "attach", "-t", name)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	// Hold stdin open for the life of the attach. A control-mode client reads
	// commands from stdin and exits on EOF — with stdin left at /dev/null it
	// quits before the first %output, and the stream is silently empty. That is
	// exactly how this failed the first time it ran.
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("tmux -C attach -t %s: %w", name, err)
	}
	s := &stream{
		max:  c.bufSize,
		subs: map[chan session.Chunk]uint64{},
		stop: func() { _ = stdin.Close(); cancel() },
		done: make(chan struct{}),
	}
	go s.read(stdout, cmd)
	return s, nil
}

func (s *stream) read(stdout io.ReadCloser, cmd *exec.Cmd) {
	defer close(s.done)
	defer func() { _ = cmd.Wait() }()

	sc := bufio.NewScanner(stdout)
	// Control-mode lines carry a whole burst of program output, which can be
	// far longer than bufio's default 64 KiB.
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	for sc.Scan() {
		ev := session.ParseLine(sc.Text())
		if ev.Kind != session.EventOutput {
			// %begin/%end/%error frame command replies, and this stream issues
			// no commands: it only listens. Other notifications (a rename, a
			// resize) are not errors and are ignored on purpose.
			continue
		}
		s.publish(session.Chunk{Data: session.DecodeOutput(ev.Payload), At: time.Now()})
	}
}

func (s *stream) publish(ck session.Chunk) {
	s.mu.Lock()
	ck.Seq = s.next
	s.next++
	s.ring = append(s.ring, ck)
	if len(s.ring) > s.max {
		s.ring = s.ring[len(s.ring)-s.max:]
	}
	subs := make([]chan session.Chunk, 0, len(s.subs))
	for ch := range s.subs {
		subs = append(subs, ch)
	}
	s.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- ck:
		default:
			// A subscriber that cannot keep up is skipped rather than blocking
			// the reader: one slow client must not stall the session's output
			// for everyone, and the gap is recoverable — the client sees a jump
			// in Seq and refetches a snapshot.
		}
	}
}
