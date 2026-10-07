// Package credfile keeps this device's host credentials in
// ~/.config/sparkflow-agentd/credentials.json: directory 0700, file 0600,
// written atomically, and refused when anyone but the owner can read it.
package credfile

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/host"
)

// ErrNone is "this device has not been enrolled".
var ErrNone = errors.New("this device is not enrolled — run `sparkflow-agentd init`")

// Store is one credentials file.
type Store struct {
	Path string
	// rename is os.Rename; a test swaps it to fail the last step.
	rename func(oldpath, newpath string) error
}

// DefaultDir is $XDG_CONFIG_HOME/sparkflow-agentd, else ~/.config/sparkflow-agentd.
func DefaultDir() (string, error) {
	if x := os.Getenv("XDG_CONFIG_HOME"); x != "" && filepath.IsAbs(x) {
		return filepath.Join(x, "sparkflow-agentd"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "sparkflow-agentd"), nil
}

// Load reads the file. A file readable by group or others is REFUSED, not
// silently tightened: someone else may already have read it, and the person
// should know (revoke the host and enrol again).
func (s Store) Load() (*host.Credentials, error) {
	fi, err := os.Stat(s.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNone
	}
	if err != nil {
		return nil, err
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%s is readable by other users (mode %o): treat the sign-in as exposed — revoke this host in \"My hosts\", "+
			"delete the file and run `sparkflow-agentd init` again", s.Path, fi.Mode().Perm())
	}
	b, err := os.ReadFile(s.Path)
	if err != nil {
		return nil, err
	}
	var c host.Credentials
	if err := json.Unmarshal(b, &c); err != nil {
		// The parse error text, never the content.
		return nil, fmt.Errorf("%s is not valid: %v", s.Path, err)
	}
	return &c, nil
}

// Save writes atomically: a temp file in the same directory, fsync, rename.
// A crash between the two leaves the previous credentials intact — which
// matters, because a refresh token the IdP has already rotated away cannot
// be got back.
func (s Store) Save(c host.Credentials) error {
	dir := filepath.Dir(s.Path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	// MkdirAll leaves an existing directory's mode alone.
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".credentials-*.json")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // a no-op after the rename
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	rename := s.rename
	if rename == nil {
		rename = os.Rename
	}
	return rename(tmpName, s.Path)
}

// Remove deletes the file (uninstall).
func (s Store) Remove() error {
	err := os.Remove(s.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}
