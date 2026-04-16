// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockQueryResultIterator is an in-memory QueryResultIterator backed by a slice of raw JSON
// byte slices, used to test unionDedupIterator without a real database.
type mockQueryResultIterator struct {
	rows     [][]byte
	pos      int
	closed   bool
	closeErr error
}

func newMockIterator(rows [][]byte) *mockQueryResultIterator {
	return &mockQueryResultIterator{rows: rows}
}

func (m *mockQueryResultIterator) NextBytes() []byte {
	if m.pos >= len(m.rows) {
		return nil
	}
	raw := m.rows[m.pos]
	m.pos++
	return raw
}

func (m *mockQueryResultIterator) Next(_ context.Context, valuePtr any) bool {
	raw := m.NextBytes()
	if raw == nil {
		return false
	}
	return base.JSONUnmarshal(raw, valuePtr) == nil
}

func (m *mockQueryResultIterator) One(ctx context.Context, valuePtr any) error {
	if !m.Next(ctx, valuePtr) {
		return fmt.Errorf("no rows")
	}
	return m.Close()
}

func (m *mockQueryResultIterator) Close() error {
	m.closed = true
	return m.closeErr
}

// marshalRow builds a minimal principal-row JSON byte slice with id and name fields,
// mimicking a row from the UNION ALL query used by buildDualMetadataUnionStatement.
func marshalRow(id, name string) []byte {
	row, err := base.JSONMarshal(map[string]string{"id": id, "name": name})
	if err != nil {
		panic(fmt.Sprintf("marshalRow: %v", err))
	}
	return row
}

// TestUnionDedupIteratorPrimaryOnly verifies that all primary rows are emitted when there are
// no fallback rows in the UNION ALL stream.
func TestUnionDedupIteratorPrimaryOnly(t *testing.T) {
	ctx := base.TestCtx(t)
	// Simulate UNION ALL output with only primary rows (no fallback rows).
	stream := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice"),
		marshalRow("_sync:user:bob", "bob"),
	})

	iter := newUnionDedupIterator(stream)
	var rows []principalRow
	for {
		var row principalRow
		if !iter.Next(ctx, &row) {
			break
		}
		rows = append(rows, row)
	}
	require.NoError(t, iter.Close())
	require.Len(t, rows, 2)
	assert.Equal(t, "_sync:user:alice", rows[0].Id)
	assert.Equal(t, "_sync:user:bob", rows[1].Id)
}

// TestUnionDedupIteratorFallbackOnly verifies that all fallback rows are emitted when there
// are no primary rows in the UNION ALL stream.
func TestUnionDedupIteratorFallbackOnly(t *testing.T) {
	ctx := base.TestCtx(t)
	// Simulate UNION ALL output with only fallback rows.
	stream := newMockIterator([][]byte{
		marshalRow("_sync:user:carol", "carol"),
		marshalRow("_sync:role:editor", "editor"),
	})

	iter := newUnionDedupIterator(stream)
	var rows []principalRow
	for {
		var row principalRow
		if !iter.Next(ctx, &row) {
			break
		}
		rows = append(rows, row)
	}
	require.NoError(t, iter.Close())
	require.Len(t, rows, 2)
	assert.Equal(t, "_sync:user:carol", rows[0].Id)
	assert.Equal(t, "_sync:role:editor", rows[1].Id)
}

// TestUnionDedupIteratorEmpty verifies that an empty UNION ALL stream is handled cleanly.
func TestUnionDedupIteratorEmpty(t *testing.T) {
	iter := newUnionDedupIterator(newMockIterator(nil))
	var row principalRow
	assert.False(t, iter.Next(context.Background(), &row))
	require.NoError(t, iter.Close())
}

// TestUnionDedupIteratorDeduplicatesPrimaryPreferred verifies that when the same META().id
// appears in both the primary and fallback sections of the UNION ALL stream, only the primary
// version (which appears first) is emitted. The Name field is different per store so the test
// can confirm which copy was chosen.
func TestUnionDedupIteratorDeduplicatesPrimaryPreferred(t *testing.T) {
	ctx := base.TestCtx(t)
	// Primary rows appear first in the UNION ALL stream, fallback rows second.
	stream := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice-primary"),
		marshalRow("_sync:user:bob", "bob-primary"),
		// Fallback section: alice and bob are duplicates, carol is new.
		marshalRow("_sync:user:alice", "alice-fallback"),
		marshalRow("_sync:user:bob", "bob-fallback"),
		marshalRow("_sync:user:carol", "carol-fallback"),
	})

	iter := newUnionDedupIterator(stream)
	var rows []principalRow
	for {
		var row principalRow
		if !iter.Next(ctx, &row) {
			break
		}
		rows = append(rows, row)
	}
	require.NoError(t, iter.Close())

	require.Len(t, rows, 3)
	assert.Equal(t, "_sync:user:alice", rows[0].Id)
	assert.Equal(t, "alice-primary", rows[0].Name, "primary version should be preferred")
	assert.Equal(t, "_sync:user:bob", rows[1].Id)
	assert.Equal(t, "bob-primary", rows[1].Name, "primary version should be preferred")
	assert.Equal(t, "_sync:user:carol", rows[2].Id)
	assert.Equal(t, "carol-fallback", rows[2].Name, "fallback-only doc should be emitted")
}

// TestUnionDedupIteratorAllDuplicates verifies that when every fallback doc is a duplicate of
// a primary doc, only the primary docs are returned (no extras).
func TestUnionDedupIteratorAllDuplicates(t *testing.T) {
	ctx := base.TestCtx(t)
	stream := newMockIterator([][]byte{
		// Primary section.
		marshalRow("_sync:user:alice", "alice-primary"),
		marshalRow("_sync:user:bob", "bob-primary"),
		// Fallback section — identical IDs.
		marshalRow("_sync:user:alice", "alice"),
		marshalRow("_sync:user:bob", "bob"),
	})
	iter := newUnionDedupIterator(stream)
	var result []principalRow
	for {
		var row principalRow
		if !iter.Next(ctx, &row) {
			break
		}
		result = append(result, row)
	}
	require.NoError(t, iter.Close())
	require.Len(t, result, 2)
	assert.Equal(t, "_sync:user:alice", result[0].Id)
	assert.Equal(t, "_sync:user:bob", result[1].Id)
	assert.Equal(t, "alice-primary", result[0].Name)
	assert.Equal(t, "bob-primary", result[1].Name)
}

// TestUnionDedupIteratorNextBytes verifies that NextBytes returns raw JSON and that
// deduplication still works through the raw-bytes path.
func TestUnionDedupIteratorNextBytes(t *testing.T) {
	stream := newMockIterator([][]byte{
		// Primary section.
		marshalRow("_sync:user:alice", "alice"),
		// Fallback section: alice is a duplicate, bob is new.
		marshalRow("_sync:user:alice", "alice-dup"),
		marshalRow("_sync:user:bob", "bob"),
	})

	iter := newUnionDedupIterator(stream)
	var rawRows [][]byte
	for {
		raw := iter.NextBytes()
		if raw == nil {
			break
		}
		rawRows = append(rawRows, raw)
	}
	require.NoError(t, iter.Close())
	require.Len(t, rawRows, 2, "duplicate alice row from fallback should be suppressed")
	assert.Contains(t, string(rawRows[0]), "alice")
	assert.Contains(t, string(rawRows[1]), "bob")
}

// TestUnionDedupIteratorClosesProperly verifies that Close propagates to the underlying
// iterator, even after partial iteration.
func TestUnionDedupIteratorClosesProperly(t *testing.T) {
	ctx := base.TestCtx(t)
	mock := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice"),
		marshalRow("_sync:user:bob", "bob"),
	})

	iter := newUnionDedupIterator(mock)
	var row principalRow
	require.True(t, iter.Next(ctx, &row))
	require.NoError(t, iter.Close())
	assert.True(t, mock.closed, "underlying iterator should be closed")
}

// TestUnionDedupIteratorOne verifies the One helper returns the first row and closes the
// underlying iterator.
func TestUnionDedupIteratorOne(t *testing.T) {
	ctx := base.TestCtx(t)
	mock := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice"),
	})

	iter := newUnionDedupIterator(mock)
	var row principalRow
	require.NoError(t, iter.One(ctx, &row))
	assert.Equal(t, "_sync:user:alice", row.Id)
	assert.True(t, mock.closed)
}

// TestExtractRowID verifies that extractRowID correctly extracts the META().id value from
// various raw JSON byte slices.
func TestExtractRowID(t *testing.T) {
	tests := []struct {
		name     string
		raw      []byte
		expected string
	}{
		{
			name:     "well-formed row",
			raw:      []byte(`{"id":"_sync:user:alice","name":"alice"}`),
			expected: "_sync:user:alice",
		},
		{
			name:     "id field only",
			raw:      []byte(`{"id":"_sync:role:editor"}`),
			expected: "_sync:role:editor",
		},
		{
			name:     "empty id",
			raw:      []byte(`{"id":""}`),
			expected: "",
		},
		{
			name:     "missing id field",
			raw:      []byte(`{"name":"alice"}`),
			expected: "",
		},
		{
			name:     "malformed JSON",
			raw:      []byte(`not-json`),
			expected: "",
		},
		{
			name:     "nil bytes",
			raw:      nil,
			expected: "",
		},
		{
			name:     "row with sort_order column",
			raw:      []byte(`{"id":"_sync:user:alice","name":"alice","sort_order":0}`),
			expected: "_sync:user:alice",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractRowID(tc.raw)
			assert.Equal(t, tc.expected, got)
		})
	}
}

// TestBuildDualMetadataUnionStatement verifies that buildDualMetadataUnionStatement produces
// well-formed UNION ALL N1QL statements with the correct keyspace substitutions, sort_order
// literal injection, inner ORDER BY stripping, and outer ORDER BY construction.
func TestBuildDualMetadataUnionStatement(t *testing.T) {
	primaryKS := "`bucket`.`_system`.`_mobile`"
	fallbackKS := "`bucket`.`_default`.`_default`"
	alias := base.KeyspaceQueryAlias

	tests := []struct {
		name              string
		statement         string
		wantPrimaryKSInQ  string
		wantFallbackKSInQ string
		wantOuterOrderBy  string // expected suffix after "ORDER BY"
		wantLimitInArm    string // expected LIMIT token present in each arm (empty = no LIMIT)
	}{
		{
			name: "basic query without ORDER BY",
			statement: "SELECT META($_keyspace).id AS id, name " +
				"FROM $_keyspace WHERE type = 'user'",
			wantPrimaryKSInQ:  primaryKS,
			wantFallbackKSInQ: fallbackKS,
			wantOuterOrderBy:  dualMetadataSortOrderKey,
		},
		{
			name: "query with alias, no ORDER BY",
			statement: "SELECT META(sgQueryKeyspaceAlias).id AS id, name " +
				"FROM $_keyspace AS sgQueryKeyspaceAlias WHERE type = 'role'",
			wantPrimaryKSInQ:  primaryKS,
			wantFallbackKSInQ: fallbackKS,
			wantOuterOrderBy:  dualMetadataSortOrderKey,
		},
		{
			name: "query with ORDER BY META(alias).id",
			statement: fmt.Sprintf(
				"SELECT META(%s).id, name FROM $_keyspace AS %s WHERE type = 'user' ORDER BY META(%s).id",
				alias, alias, alias,
			),
			wantPrimaryKSInQ:  primaryKS,
			wantFallbackKSInQ: fallbackKS,
			wantOuterOrderBy:  dualMetadataSortOrderKey + ", id",
		},
		{
			name: "query with ORDER BY META(alias).id and LIMIT",
			statement: fmt.Sprintf(
				"SELECT META(%s).id, name FROM $_keyspace AS %s WHERE type = 'user' ORDER BY META(%s).id LIMIT 10",
				alias, alias, alias,
			),
			wantPrimaryKSInQ:  primaryKS,
			wantFallbackKSInQ: fallbackKS,
			wantOuterOrderBy:  dualMetadataSortOrderKey + ", id",
			wantLimitInArm:    "LIMIT 10",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := buildDualMetadataUnionStatement(tc.statement, primaryKS, fallbackKS)

			assert.True(t, strings.HasPrefix(result, "("), "should start with opening paren")
			assert.Contains(t, result, ") UNION ALL (", "should contain UNION ALL joining the sub-queries")

			// The result must end with the outer ORDER BY clause.
			assert.True(t, strings.HasSuffix(result, " ORDER BY "+tc.wantOuterOrderBy),
				"result should end with outer ORDER BY %q; got: %s", tc.wantOuterOrderBy, result)

			// Both keyspaces should appear in the output.
			assert.Contains(t, result, tc.wantPrimaryKSInQ, "primary keyspace should be in result")
			assert.Contains(t, result, tc.wantFallbackKSInQ, "fallback keyspace should be in result")

			// sort_order literals 0 (primary) and 1 (fallback) must both be injected.
			assert.Contains(t, result, "0 AS "+dualMetadataSortOrderKey, "primary arm must have sort_order=0")
			assert.Contains(t, result, "1 AS "+dualMetadataSortOrderKey, "fallback arm must have sort_order=1")

			// KeyspaceQueryToken must not appear in the output.
			assert.NotContains(t, result, base.KeyspaceQueryToken, "token must be fully replaced")

			// Split on UNION ALL to inspect each arm individually.
			// The second part will include the trailing outer ORDER BY; both arms must contain
			// the correct FROM clause.
			parts := strings.SplitN(result, " UNION ALL ", 2)
			require.Len(t, parts, 2, "should have exactly two UNION ALL arms")
			assert.Contains(t, parts[0], "FROM "+tc.wantPrimaryKSInQ)
			assert.Contains(t, parts[1], "FROM "+tc.wantFallbackKSInQ)

			// Inner ORDER BY must not appear inside either arm (it was lifted to outer scope).
			assert.NotContains(t, parts[0], " ORDER BY ", "primary arm must not contain inner ORDER BY")
			// parts[1] includes the outer ORDER BY suffix; strip it before checking.
			armTwo := strings.TrimSuffix(parts[1], " ORDER BY "+tc.wantOuterOrderBy)
			assert.NotContains(t, armTwo, " ORDER BY ", "fallback arm must not contain inner ORDER BY")

			// If the original statement had a LIMIT it must be re-attached to each arm.
			if tc.wantLimitInArm != "" {
				assert.Contains(t, parts[0], tc.wantLimitInArm, "primary arm must contain LIMIT")
				assert.Contains(t, armTwo, tc.wantLimitInArm, "fallback arm must contain LIMIT")
			}
		})
	}
}

// TestSplitStatement verifies that splitStatement correctly separates the core query, ORDER BY
// columns, and LIMIT clause for a variety of input shapes.
func TestSplitStatement(t *testing.T) {
	tests := []struct {
		name            string
		stmt            string
		wantCore        string
		wantOrderBy     string
		wantLimitClause string
	}{
		{
			name:        "no ORDER BY, no LIMIT",
			stmt:        "SELECT name FROM ks WHERE type = 'user'",
			wantCore:    "SELECT name FROM ks WHERE type = 'user'",
			wantOrderBy: "",
		},
		{
			name:        "ORDER BY only",
			stmt:        "SELECT name FROM ks WHERE type = 'user' ORDER BY META(alias).id",
			wantCore:    "SELECT name FROM ks WHERE type = 'user'",
			wantOrderBy: "META(alias).id",
		},
		{
			name:            "ORDER BY and LIMIT",
			stmt:            "SELECT name FROM ks WHERE type = 'user' ORDER BY META(alias).id LIMIT 100",
			wantCore:        "SELECT name FROM ks WHERE type = 'user'",
			wantOrderBy:     "META(alias).id",
			wantLimitClause: " LIMIT 100",
		},
		{
			name:            "bare LIMIT, no ORDER BY",
			stmt:            "SELECT name FROM ks LIMIT 50",
			wantCore:        "SELECT name FROM ks",
			wantOrderBy:     "",
			wantLimitClause: " LIMIT 50",
		},
		{
			name:            "ORDER BY with multiple columns and LIMIT",
			stmt:            "SELECT a, b FROM ks ORDER BY a, b DESC LIMIT 10",
			wantCore:        "SELECT a, b FROM ks",
			wantOrderBy:     "a, b DESC",
			wantLimitClause: " LIMIT 10",
		},
		{
			name:        "case-insensitive ORDER BY",
			stmt:        "SELECT name FROM ks order by name",
			wantCore:    "SELECT name FROM ks",
			wantOrderBy: "name",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			core, orderBy, limit := splitStatement(tc.stmt)
			assert.Equal(t, tc.wantCore, core)
			assert.Equal(t, tc.wantOrderBy, orderBy)
			assert.Equal(t, tc.wantLimitClause, limit)
		})
	}
}
