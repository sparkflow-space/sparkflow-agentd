package credfile

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/host"
)

// A write that fails at its last step leaves the previous credentials intact
// and no temp file behind. (An earlier version made the directory read-only
// to cause the failure: Save itself restores the directory to 0700, so the
// write succeeded — and as root the test skipped, which hid that until CI ran
// it as a normal user.)
func TestSave_AFailedWriteKeepsTheOldFile(t *testing.T) {
	dir := t.TempDir()
	s := Store{Path: filepath.Join(dir, "credentials.json")}
	if err := s.Save(host.Credentials{RefreshToken: "old"}); err != nil {
		t.Fatal(err)
	}
	s.rename = func(string, string) error { return errors.New("EIO") }
	if err := s.Save(host.Credentials{RefreshToken: "new"}); err == nil {
		t.Fatal("want the write error")
	}
	s.rename = nil
	got, err := s.Load()
	if err != nil || got.RefreshToken != "old" {
		t.Fatalf("the old file was damaged: %+v %v", got, err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Errorf("a temp file was left behind: %v", entries)
	}
}
