// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"

// TestAuditEventsAreReferenced statically verifies that every audit event registered in
// AuditEvents is referenced somewhere in the codebase outside of its definition in
// audit_events.go. This catches events that were declared and documented in the events table,
// but never actually wired up to a base.Audit (or base.AuditEventIsEnabled) call site.
//
// This is a name-based heuristic, not a dataflow analysis: it looks for any AST identifier
// matching a defined event's const name anywhere else in the module. Test files (anything named
// *_test.go, or containing "testing") are skipped, so only production code counts as evidence
// that the event is actually emitted. It will not catch an event ID that is only ever referenced
// in an unrelated context (e.g. a stray comment-adjacent list), but in practice these consts
// exist for exactly one purpose, so any production reference outside the definition table is
// meaningful.
func TestAuditEventsAreReferenced(t *testing.T) {
	repoRoot := findRepoRoot(t)
	definitionFile := filepath.Join(repoRoot, "base", "audit_events.go")

	fset := token.NewFileSet()
	definitionAST, err := parser.ParseFile(fset, definitionFile, nil, 0)
	require.NoError(t, err)

	defined := extractDefinedAuditEventNames(t, definitionAST)
	require.NotEmpty(t, defined, "expected to find entries in the AuditEvents table")

	used, err := findReferencedNames(fset, repoRoot, definitionFile, defined)
	require.NoError(t, err)

	unreferenced := slices.Sorted(maps.Keys(defined))
	unreferenced = slices.DeleteFunc(unreferenced, func(name string) bool { return used[name] })
	assert.Empty(t, unreferenced, "audit events are defined in AuditEvents but never referenced outside audit_events.go - "+
		"did you forget to add the base.Audit call for these events?")
}

// findReferencedNames walks every .go file under root (other than definitionFile) and returns
// the subset of names that appear as an identifier somewhere in that tree.
func findReferencedNames(fset *token.FileSet, root, definitionFile string, names map[string]struct{}) (map[string]bool, error) {
	used := make(map[string]bool, len(names))
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		base := filepath.Base(path)
		if !strings.HasSuffix(base, ".go") || path == definitionFile {
			return nil
		}
		if strings.Contains(base, "testing") || strings.HasSuffix(base, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return fmt.Errorf("parsing %s: %w", path, err)
		}
		for n := range ast.Preorder(file) {
			ident, ok := n.(*ast.Ident)
			if !ok {
				continue
			}
			if _, isEventName := names[ident.Name]; isEventName {
				used[ident.Name] = true
			}
		}
		return nil
	})
	return used, err
}

// extractDefinedAuditEventNames returns the set of const names used as keys in the AuditEvents
// table declared in audit_events.go.
func extractDefinedAuditEventNames(t *testing.T, file *ast.File) map[string]struct{} {
	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.VAR {
			continue
		}
		for _, spec := range genDecl.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok || len(valueSpec.Names) != 1 || valueSpec.Names[0].Name != "AuditEvents" {
				continue
			}
			require.Len(t, valueSpec.Values, 1)
			composite, ok := valueSpec.Values[0].(*ast.CompositeLit)
			require.True(t, ok, "expected AuditEvents to be initialized with a composite literal")

			defined := make(map[string]struct{}, len(composite.Elts))
			for _, elt := range composite.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				require.True(t, ok, "expected AuditEvents entries to be key/value pairs")
				keyIdent, ok := kv.Key.(*ast.Ident)
				require.True(t, ok, "expected AuditEvents keys to be identifiers")
				defined[keyIdent.Name] = struct{}{}
			}
			return defined
		}
	}
	require.Fail(t, "could not find `var AuditEvents = events{...}` declaration")
	return nil
}

// findRepoRoot walks up from the current working directory to find the module root (identified
// by go.mod). Tests run with their package directory as the working directory.
func findRepoRoot(t *testing.T) string {
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "could not find repo root (go.mod) starting from %s", dir)
		dir = parent
	}
}
