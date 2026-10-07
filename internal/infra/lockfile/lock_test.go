package lockfile_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/lockfile"
)

func TestAcquire_SecondHolderIsRefusedWithThePid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json.lock")
	first, err := lockfile.Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	// flock locks belong to the open file description, so a second open in
	// the same process conflicts exactly as a second process would.
	_, err = lockfile.Acquire(path)
	if !errors.Is(err, lockfile.ErrHeld) || !strings.Contains(err.Error(), fmt.Sprint(os.Getpid())) {
		t.Fatalf("second acquire: %v", err)
	}
	first.Release()
	again, err := lockfile.Acquire(path)
	if err != nil {
		t.Fatalf("after release: %v", err)
	}
	again.Release()
}
