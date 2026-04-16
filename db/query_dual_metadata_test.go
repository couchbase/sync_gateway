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
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockQueryResultIterator is an in-memory QueryResultIterator backed by a slice of raw JSON
// byte slices, used to test mergeDedupIterator without a real database.
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

// marshalRow builds a minimal principal-row JSON byte slice from id and name fields.
func marshalRow(id, name string) []byte {
	row, err := base.JSONMarshal(map[string]string{"id": id, "name": name})
	if err != nil {
		panic(fmt.Sprintf("marshalRow: %v", err))
	}
	return row
}

// TestMergeDedupIterator_PrimaryOnly verifies that all primary rows are emitted when the
// fallback iterator is empty.
func TestMergeDedupIteratorPrimaryOnly(t *testing.T) {
	ctx := base.TestCtx(t)
	primary := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice"),
		marshalRow("_sync:user:bob", "bob"),
	})
	fallback := newMockIterator(nil)

	iter := newMergeDedupIterator(primary, fallback)
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

// TestMergeDedupIterator_FallbackOnly verifies that all fallback rows are emitted when the
// primary iterator is empty.
func TestMergeDedupIteratorFallbackOnly(t *testing.T) {
	ctx := base.TestCtx(t)
	primary := newMockIterator(nil)
	fallback := newMockIterator([][]byte{
		marshalRow("_sync:user:carol", "carol"),
		marshalRow("_sync:role:editor", "editor"),
	})

	iter := newMergeDedupIterator(primary, fallback)
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

// TestMergeDedupIterator_BothEmpty verifies that an empty merged stream is handled cleanly.
func TestMergeDedupIteratorBothEmpty(t *testing.T) {
	iter := newMergeDedupIterator(newMockIterator(nil), newMockIterator(nil))
	var row principalRow
	assert.False(t, iter.Next(context.Background(), &row))
	require.NoError(t, iter.Close())
}

// TestMergeDedupIterator_DeduplicatesPrimaryPreferred verifies that when the same META().id
// appears in both primary and fallback, only the primary version is emitted.  The Name field
// is deliberately different between stores to confirm which version was chosen.
func TestMergeDedupIteratorDeduplicatesPrimaryPreferred(t *testing.T) {
	ctx := base.TestCtx(t)
	primary := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice-primary"),
		marshalRow("_sync:user:bob", "bob-primary"),
	})
	// fallback has the same two IDs plus one new one
	fallback := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice-fallback"),
		marshalRow("_sync:user:bob", "bob-fallback"),
		marshalRow("_sync:user:carol", "carol-fallback"),
	})

	iter := newMergeDedupIterator(primary, fallback)
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

// TestMergeDedupIterator_AllDuplicates verifies that when every fallback doc is a duplicate
// of a primary doc, only the primary docs are returned (no extras).
func TestMergeDedupIteratorAllDuplicates(t *testing.T) {
	ctx := base.TestCtx(t)
	rows := [][]byte{
		marshalRow("_sync:user:alice", "alice"),
		marshalRow("_sync:user:bob", "bob"),
	}
	iter := newMergeDedupIterator(newMockIterator(rows), newMockIterator(rows))
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
}

// TestMergeDedupIterator_NextBytes verifies that NextBytes returns raw JSON and that
// deduplication still works through the raw-bytes path.
func TestMergeDedupIteratorNextBytes(t *testing.T) {
	primary := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice"),
	})
	fallback := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice-dup"),
		marshalRow("_sync:user:bob", "bob"),
	})

	iter := newMergeDedupIterator(primary, fallback)
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

// TestMergeDedupIterator_CloseBothIterators verifies that Close is called on both underlying
// iterators, even after partial iteration.
func TestMergeDedupIteratorCloseBothIterators(t *testing.T) {
	ctx := base.TestCtx(t)
	primary := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice"),
	})
	fallback := newMockIterator([][]byte{
		marshalRow("_sync:user:bob", "bob"),
	})

	iter := newMergeDedupIterator(primary, fallback)
	// Read one row then close without exhausting the iterators.
	var row principalRow
	require.True(t, iter.Next(ctx, &row))
	require.NoError(t, iter.Close())
	assert.True(t, primary.closed, "primary iterator should be closed")
	assert.True(t, fallback.closed, "fallback iterator should be closed")
}

// TestMergeDedupIterator_One verifies the One helper returns the first row and closes both
// underlying iterators.
func TestMergeDedupIteratorOne(t *testing.T) {
	ctx := base.TestCtx(t)
	primary := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice"),
	})
	fallback := newMockIterator(nil)

	iter := newMergeDedupIterator(primary, fallback)
	var row principalRow
	require.NoError(t, iter.One(ctx, &row))
	assert.Equal(t, "_sync:user:alice", row.Id)
	assert.True(t, primary.closed)
	assert.True(t, fallback.closed)
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractRowID(tc.raw)
			assert.Equal(t, tc.expected, got)
		})
	}
}
