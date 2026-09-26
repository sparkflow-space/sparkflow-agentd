// Package archtest enforces the Go layering rule from docs/CLAUDE.md
// §"Layering rule for every Go-service task":
//
//	Handler → Application → Domain ← Infra
//
// It parses the import block of every non-test .go file under internal/ and
// fails on a forbidden edge. The rule set and its rationale live in the vault:
// [[Layering Gates]]. This file is identical in every Go repo except for the
// applicationThirdParty allow-list below.
//
// What a green run proves — and what it does not. It proves that no source
// file WRITES a forbidden import. It does not prove that application code only
// touches domain types: a value whose concrete type lives in infra can still
// reach application through an interface-typed port or a constructor's return
// value without any import being written. Review still owns that half.
//
// The self-tests at the bottom plant each kind of violation in a scratch
// module and assert the checker reports it, so a checker that silently checks
// nothing cannot pass.
package archtest

import (
	"bufio"
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// applicationThirdParty lists the non-stdlib import prefixes an application
// package may use. Anything else outside the module's own domain/application
// packages — generated pb types, gRPC, transport, infra — is a violation.
// Adding an entry is a design decision: say why in the MR.
var applicationThirdParty = []string{
	// Nothing yet, and that is the point: this daemon's use-case layer reaches
	// tmux and the token verifier only through its own ports. An entry here is
	// a design decision — say why in the MR that adds it.
}

// skipDirs are never scanned: generated code is not hand-written layering.
var skipDirs = map[string]bool{"generated": true, "testdata": true, "vendor": true}

type violation struct {
	file, imp, reason string
}

func (v violation) String() string { return fmt.Sprintf("%s imports %q: %s", v.file, v.imp, v.reason) }

// layerOf returns the layer of a module-relative path ("internal/domain/x/y.go"
// → "domain"), or "" when the path is outside the four ruled layers.
func layerOf(rel string) string {
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) < 2 || parts[0] != "internal" {
		return ""
	}
	switch parts[1] {
	case "domain", "application", "infra", "handler":
		return parts[1]
	case "cli":
		// CLAUDE.md names the CLI as a handler ("gRPC servers, HTTP handlers,
		// WS bridges, CLI"); sparkflow-sync keeps its cobra commands in
		// internal/cli. Without this alias an infra → cli import is invisible.
		return "handler"
	}
	return ""
}

func isStdlib(imp string) bool {
	first, _, _ := strings.Cut(imp, "/")
	return !strings.Contains(first, ".")
}

func hasPrefix(imp string, prefixes []string) bool {
	for _, p := range prefixes {
		if imp == p || strings.HasPrefix(imp, p+"/") {
			return true
		}
	}
	return false
}

// judge applies the rule to one import of one file. It returns "" when the
// edge is allowed.
func judge(module, fileLayer, imp string) string {
	var impLayer string
	internal := strings.HasPrefix(imp, module+"/")
	if internal {
		impLayer = layerOf(strings.TrimPrefix(imp, module+"/"))
	}
	switch fileLayer {
	case "domain":
		if isStdlib(imp) || (internal && impLayer == "domain") {
			return ""
		}
		return "domain may import only the standard library and other domain packages"
	case "application":
		if isStdlib(imp) || (internal && (impLayer == "domain" || impLayer == "application")) {
			return ""
		}
		if !internal && hasPrefix(imp, applicationThirdParty) {
			return ""
		}
		return "application may import only domain, application, the standard library and the applicationThirdParty allow-list"
	case "infra":
		if internal && (impLayer == "application" || impLayer == "handler") {
			return "infra implements domain ports and must not import application or handler"
		}
	}
	return ""
}

func modulePath(root string) (string, error) {
	f, err := os.Open(filepath.Join(root, "go.mod"))
	if err != nil {
		return "", err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(s.Text()), "module "); ok {
			return strings.Trim(strings.TrimSpace(rest), `"`), nil
		}
	}
	return "", fmt.Errorf("no module line in %s/go.mod", root)
}

// check scans root/internal and returns every violation plus the number of
// files it judged per layer.
func check(root string) ([]violation, map[string]int, error) {
	module, err := modulePath(root)
	if err != nil {
		return nil, nil, err
	}
	counts := map[string]int{}
	var out []violation
	fset := token.NewFileSet()
	err = filepath.WalkDir(filepath.Join(root, "internal"), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skipDirs[d.Name()] || (strings.HasPrefix(d.Name(), ".") && path != root) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		layer := layerOf(rel)
		if layer == "" {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		counts[layer]++
		for _, is := range f.Imports {
			imp, _ := strconv.Unquote(is.Path.Value)
			if reason := judge(module, layer, imp); reason != "" {
				out = append(out, violation{filepath.ToSlash(rel), imp, reason})
			}
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out, counts, err
}

// repoRoot walks up from the test's working directory to the go.mod.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func TestLayering(t *testing.T) {
	violations, counts, err := check(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	// Guard against a vacuous pass: a repo with application code must have
	// had it scanned. (Scanning the wrong directory would find nothing and
	// report nothing.)
	if counts["application"] == 0 || counts["domain"] == 0 {
		t.Fatalf("scanned no application or domain files (counts %v) — the checker is looking in the wrong place", counts)
	}
	for _, v := range violations {
		t.Error(v)
	}
	if len(violations) > 0 {
		t.Log("See docs [[Layering Gates]]: introduce a domain port instead of importing across the layer.")
	}
}

// --- self-tests: the checker must fail on each kind of violation ----------

func writeModule(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	files["go.mod"] = "module example.com/svc\n\ngo 1.22\n"
	for name, body := range files {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestCheckerReportsPlantedViolations(t *testing.T) {
	root := writeModule(t, map[string]string{
		"internal/domain/order/order.go":     "package order\nimport (\n\t\"time\"\n\t\"github.com/google/uuid\"\n)\nvar _ = time.Now\nvar _ = uuid.New\n",
		"internal/application/order/app.go":  "package order\nimport _ \"example.com/svc/internal/infra/db\"\n",
		"internal/application/order/grpc.go": "package order\nimport _ \"google.golang.org/grpc\"\n",
		"internal/application/order/gen.go":  "package order\nimport _ \"example.com/svc/internal/generated/orderv1\"\n",
		"internal/infra/db/db.go":            "package db\nimport _ \"example.com/svc/internal/application/order\"\n",
		"internal/infra/db/handler.go":       "package db\nimport _ \"example.com/svc/internal/handler/http\"\n",
		"internal/infra/db/cli.go":           "package db\nimport _ \"example.com/svc/internal/cli\"\n",
		// ok.go is the NEGATIVE control: a file the checker must NOT flag. Its
		// third import is drawn from applicationThirdParty when that list has
		// an entry, and omitted when it is empty — this repo's list is empty on
		// purpose, and a fixture hard-coding zerolog turned the control into a
		// false positive the moment it was. Derived rather than fixed so the
		// self-test is honest in every repo, whatever its allow-list holds.
		"internal/application/order/ok.go":      "package order\nimport (\n\t_ \"context\"\n\t_ \"example.com/svc/internal/domain/order\"\n" + allowedThirdPartyImportLine() + ")\n",
		"internal/application/order/ok_test.go": "package order\nimport _ \"example.com/svc/internal/infra/db\"\n",
		"internal/generated/orderv1/x.go":       "package orderv1\nimport _ \"google.golang.org/grpc\"\n",
	})
	got, _, err := check(root)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"internal/domain/order/order.go":     "github.com/google/uuid",
		"internal/application/order/app.go":  "example.com/svc/internal/infra/db",
		"internal/application/order/grpc.go": "google.golang.org/grpc",
		"internal/application/order/gen.go":  "example.com/svc/internal/generated/orderv1",
		"internal/infra/db/db.go":            "example.com/svc/internal/application/order",
		"internal/infra/db/handler.go":       "example.com/svc/internal/handler/http",
		"internal/infra/db/cli.go":           "example.com/svc/internal/cli",
	}
	if len(got) != len(want) {
		t.Errorf("got %d violations, want %d:\n%v", len(got), len(want), got)
	}
	for _, v := range got {
		if want[v.file] != v.imp {
			t.Errorf("unexpected violation %v", v)
		}
	}
}

func TestCheckerAcceptsCleanModule(t *testing.T) {
	root := writeModule(t, map[string]string{
		"internal/domain/order/order.go":    "package order\nimport _ \"net/http\"\n",
		"internal/application/order/app.go": "package order\nimport _ \"example.com/svc/internal/domain/order\"\n",
		"internal/infra/db/db.go":           "package db\nimport _ \"example.com/svc/internal/domain/order\"\n",
		"internal/handler/http/h.go":        "package http\nimport _ \"example.com/svc/internal/application/order\"\n",
		"internal/core/wire.go":             "package core\nimport _ \"example.com/svc/internal/infra/db\"\n",
	})
	got, counts, err := check(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("clean module reported violations: %v", got)
	}
	if counts["domain"] != 1 || counts["application"] != 1 || counts["infra"] != 1 || counts["handler"] != 1 {
		t.Errorf("layer counts = %v, want one file per layer", counts)
	}
}

// allowedThirdPartyImportLine renders one import line for the first entry of
// applicationThirdParty, or nothing when the allow-list is empty.
func allowedThirdPartyImportLine() string {
	if len(applicationThirdParty) == 0 {
		return ""
	}
	return "\t_ \"" + applicationThirdParty[0] + "\"\n"
}
