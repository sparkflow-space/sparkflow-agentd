// Package hostfs answers questions about the host's filesystem and prepares
// working folders: the application's Workspaces port.
package hostfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/session"
)

// Dirs is the real filesystem.
type Dirs struct {
	// Git is the git binary ("git" when empty).
	Git string
}

// IsDir follows symlinks: it answers only "is this a directory".
func (Dirs) IsDir(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

// Resolve returns the absolute path with every symlink resolved, so a
// containment check compares where a path really goes — a symlink inside the
// home pointing at /etc is /etc.
func (Dirs) Resolve(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(abs)
}

// IsGitRoot reports whether path is the top of a git work tree (has .git —
// a directory, or the file a worktree/submodule carries).
func (Dirs) IsGitRoot(path string) bool {
	_, err := os.Lstat(filepath.Join(path, ".git"))
	return err == nil
}

// ListDirs returns the directories directly inside path, hidden ones skipped,
// sorted. Files are not listed: the picker chooses a FOLDER.
func (d Dirs) ListDirs(path string) ([]session.Dir, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var out []session.Dir
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		full := filepath.Join(path, name)
		if !d.IsDir(full) {
			continue
		}
		out = append(out, session.Dir{Name: name, Git: d.IsGitRoot(full)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// AddWorktree creates a fresh worktree of repo at path on a new branch, and
// keeps `.claude/` out of the repository's `git status` (the project's own
// convention for where worktrees live) by adding it to info/exclude — a
// local, unversioned file, so nothing is committed on the person's behalf.
func (d Dirs) AddWorktree(ctx context.Context, repo, path, branch string) error {
	git := d.Git
	if git == "" {
		git = "git"
	}
	if out, err := exec.CommandContext(ctx, git, "-C", repo, "worktree", "add", "-b", branch, path).CombinedOutput(); err != nil {
		return fmt.Errorf("git worktree add: %v: %s", err, strings.TrimSpace(string(out)))
	}
	common, err := exec.CommandContext(ctx, git, "-C", repo, "rev-parse", "--path-format=absolute", "--git-common-dir").Output()
	if err != nil {
		return nil // the worktree exists; the exclude is a courtesy
	}
	exclude := filepath.Join(strings.TrimSpace(string(common)), "info", "exclude")
	b, err := os.ReadFile(exclude)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(line) == ".claude/" {
			return nil
		}
	}
	_ = os.MkdirAll(filepath.Dir(exclude), 0o755)
	f, err := os.OpenFile(exclude, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil
	}
	defer f.Close()
	_, _ = f.WriteString("\n# sparkflow-agentd: agent worktrees\n.claude/\n")
	return nil
}
