// Package lockfile enforces "one daemon per device": `run` takes an exclusive
// flock beside the credentials, and a second `run` exits naming the first.
// The kernel drops the lock when the process dies, so a crash never leaves a
// stale lock behind.
package lockfile

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// ErrHeld is another live daemon on this device.
var ErrHeld = errors.New("sparkflow-agentd is already running on this device")

// Lock is a held lock; Release frees it.
type Lock struct{ f *os.File }

// Acquire takes the lock at path, or returns ErrHeld with the holder's pid.
func Acquire(path string) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		b := make([]byte, 32)
		n, _ := f.ReadAt(b, 0)
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			if pid, perr := strconv.Atoi(strings.TrimSpace(string(b[:n]))); perr == nil {
				return nil, fmt.Errorf("%w (pid %d)", ErrHeld, pid)
			}
			return nil, ErrHeld
		}
		return nil, err
	}
	_ = f.Truncate(0)
	_, _ = f.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\n"), 0)
	return &Lock{f: f}, nil
}

// Release frees the lock.
func (l *Lock) Release() {
	if l == nil || l.f == nil {
		return
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	_ = l.f.Close()
}
