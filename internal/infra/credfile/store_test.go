package credfile_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/host"
	"github.com/sparkflow-space/sparkflow-agentd/internal/infra/credfile"
)

func TestSaveLoad_Modes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cfg", "sparkflow-agentd")
	s := credfile.Store{Path: filepath.Join(dir, "credentials.json")}
	if _, err := s.Load(); !errors.Is(err, credfile.ErrNone) {
		t.Fatalf("absent file: %v", err)
	}
	c := host.Credentials{HostID: "h1", OwnerSub: "A", RefreshToken: "secret-rt"}
	if err := s.Save(c); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(s.Path)
	di, _ := os.Stat(dir)
	if fi.Mode().Perm() != 0o600 || di.Mode().Perm() != 0o700 {
		t.Fatalf("modes: file %o dir %o", fi.Mode().Perm(), di.Mode().Perm())
	}
	got, err := s.Load()
	if err != nil || got.RefreshToken != "secret-rt" || got.HostID != "h1" {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	// A loosened dir is tightened on the next save.
	_ = os.Chmod(dir, 0o755)
	_ = s.Save(c)
	if di, _ = os.Stat(dir); di.Mode().Perm() != 0o700 {
		t.Errorf("dir mode after save: %o", di.Mode().Perm())
	}
	// No temp files are left behind.
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Errorf("leftovers: %v", entries)
	}
}

func TestLoad_RefusesAFileOthersCanRead(t *testing.T) {
	s := credfile.Store{Path: filepath.Join(t.TempDir(), "credentials.json")}
	_ = s.Save(host.Credentials{RefreshToken: "secret-rt"})
	_ = os.Chmod(s.Path, 0o644)
	_, err := s.Load()
	if err == nil || !strings.Contains(err.Error(), "readable by other users") {
		t.Fatalf("a 0644 credentials file must be refused: %v", err)
	}
	if strings.Contains(err.Error(), "secret-rt") {
		t.Fatal("the error must not carry the content")
	}
}
