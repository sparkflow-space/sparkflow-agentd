// Package hostfs answers questions about the host's filesystem. It is the
// application's Workspaces port and nothing more.
package hostfs

import "os"

// Dirs reports whether a path is a directory.
type Dirs struct{}

// IsDir follows symlinks, deliberately: the workspace root is a directory the
// operator created, and a symlinked workspace inside it is a legitimate layout.
// Containment is the lexical check in the domain plus the daemon running as an
// unprivileged user; this call answers only "does it exist as a directory".
func (Dirs) IsDir(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}
