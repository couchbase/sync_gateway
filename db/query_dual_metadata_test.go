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

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockQueryResultIterator is an in-memory QueryResultIterator backed by a slice of raw JSON
// byte slices, used to test dualMetadataStorePrincipalDedupIterator without a real database.
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

// marshalRow builds a minimal principal-row JSON byte slice with id and name fields.
func marshalRow(id, name string) []byte {
	row, err := base.JSONMarshal(map[string]string{"id": id, "name": name})
	if err != nil {
		panic(fmt.Sprintf("marshalRow: %v", err))
	}
	return row
}

// TestMergedDedupIteratorPrimaryOnly verifies that all primary rows are emitted when there are
// no fallback rows.
func TestMergedDedupIteratorPrimaryOnly(t *testing.T) {
	ctx := base.TestCtx(t)
	primary := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice"),
		marshalRow("_sync:user:bob", "bob"),
	})
	fallback := newMockIterator(nil)

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, 0)
	var rows []PrincipalRow
	for {
		var row PrincipalRow
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

// TestMergedDedupIteratorFallbackOnly verifies that all fallback rows are emitted when there
// are no primary rows.
func TestMergedDedupIteratorFallbackOnly(t *testing.T) {
	ctx := base.TestCtx(t)
	primary := newMockIterator(nil)
	fallback := newMockIterator([][]byte{
		marshalRow("_sync:user:carol", "carol"),
		marshalRow("_sync:role:editor", "editor"),
	})

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, 0)
	var rows []PrincipalRow
	for {
		var row PrincipalRow
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

// TestMergedDedupIteratorEmpty verifies that empty iterators are handled cleanly.
func TestMergedDedupIteratorEmpty(t *testing.T) {
	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](newMockIterator(nil), newMockIterator(nil), 0)
	var row PrincipalRow
	assert.False(t, iter.Next(context.Background(), &row))
	require.NoError(t, iter.Close())
}

// TestMergedDedupIteratorDeduplicatesPrimaryPreferred verifies that when the same ID
// appears in both primary and fallback, only the primary version is emitted. Results are
// in sorted ID order.
func TestMergedDedupIteratorDeduplicatesPrimaryPreferred(t *testing.T) {
	ctx := base.TestCtx(t)
	// Both iterators sorted by ID.
	primary := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice-primary"),
		marshalRow("_sync:user:bob", "bob-primary"),
	})
	fallback := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice-fallback"),
		marshalRow("_sync:user:bob", "bob-fallback"),
		marshalRow("_sync:user:carol", "carol-fallback"),
	})

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, 0)
	var rows []PrincipalRow
	for {
		var row PrincipalRow
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

// TestMergedDedupIteratorAllDuplicates verifies that when every fallback doc is a duplicate of
// a primary doc, only the primary docs are returned.
func TestMergedDedupIteratorAllDuplicates(t *testing.T) {
	ctx := base.TestCtx(t)
	primary := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice-primary"),
		marshalRow("_sync:user:bob", "bob-primary"),
	})
	fallback := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice"),
		marshalRow("_sync:user:bob", "bob"),
	})
	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, 0)
	var result []PrincipalRow
	for {
		var row PrincipalRow
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

// TestMergedDedupIteratorMergeSortOrder verifies that interleaved IDs from both stores are
// emitted in globally sorted order.
func TestMergedDedupIteratorMergeSortOrder(t *testing.T) {
	ctx := base.TestCtx(t)
	primary := newMockIterator([][]byte{
		marshalRow("A", "a"),
		marshalRow("C", "c"),
		marshalRow("E", "e"),
	})
	fallback := newMockIterator([][]byte{
		marshalRow("B", "b"),
		marshalRow("D", "d"),
		marshalRow("F", "f"),
	})

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, 0)
	var ids []string
	for {
		var row PrincipalRow
		if !iter.Next(ctx, &row) {
			break
		}
		ids = append(ids, row.Id)
	}
	require.NoError(t, iter.Close())
	assert.Equal(t, []string{"A", "B", "C", "D", "E", "F"}, ids)
}

// TestMergedDedupIteratorNextBytes verifies that NextBytes returns raw JSON and that
// deduplication still works through the raw-bytes path.
// TestMergedDedupIteratorClosesProperly verifies that Close propagates to both underlying
// iterators, even after partial iteration.
func TestMergedDedupIteratorClosesProperly(t *testing.T) {
	ctx := base.TestCtx(t)
	mockPrimary := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice"),
	})
	mockFallback := newMockIterator([][]byte{
		marshalRow("_sync:user:bob", "bob"),
	})

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](mockPrimary, mockFallback, 0)
	var row PrincipalRow
	require.True(t, iter.Next(ctx, &row))
	require.NoError(t, iter.Close())
	assert.True(t, mockPrimary.closed, "primary iterator should be closed")
	assert.True(t, mockFallback.closed, "fallback iterator should be closed")
}

// TestMergedDedupIteratorOne verifies the One helper returns the first row and closes both
// iterators.
func TestMergedDedupIteratorOne(t *testing.T) {
	ctx := base.TestCtx(t)
	mockPrimary := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice"),
	})
	mockFallback := newMockIterator(nil)

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](mockPrimary, mockFallback, 0)
	var row PrincipalRow
	require.NoError(t, iter.One(ctx, &row))
	assert.Equal(t, "_sync:user:alice", row.Id)
	assert.True(t, mockPrimary.closed)
	assert.True(t, mockFallback.closed)
}

// TestMergedDedupIteratorSafeBoundary verifies the safe-boundary mechanism: when a source
// exhausts at exactly limit rows, the iterator stops emitting rows from the other source
// whose IDs exceed the exhausted source's last ID. This prevents pagination cursor jumping.
func TestMergedDedupIteratorSafeBoundary(t *testing.T) {
	ctx := base.TestCtx(t)
	// Simulate LIMIT 2: primary returns [A, E] (2 rows = limit), fallback returns [B, C] (2 rows = limit).
	// Without boundary, merged output would be [A, B, C, E] and cursor would jump to E, skipping D.
	// With boundary: fallback exhausts at 2==limit → boundary=C. E > C → stop.
	// Output: [A, B, C]. Next page starts at C and picks up D.
	primary := newMockIterator([][]byte{
		marshalRow("A", "a"),
		marshalRow("E", "e"),
	})
	fallback := newMockIterator([][]byte{
		marshalRow("B", "b"),
		marshalRow("C", "c"),
	})

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, 2)
	var ids []string
	for {
		var row PrincipalRow
		if !iter.Next(ctx, &row) {
			break
		}
		ids = append(ids, row.Id)
	}
	require.NoError(t, iter.Close())
	assert.Equal(t, []string{"A", "B", "C"}, ids, "E should be suppressed by safe boundary at C")
}

// TestMergedDedupIteratorSafeBoundaryPrimaryExhausts verifies the boundary when primary
// exhausts first.
func TestMergedDedupIteratorSafeBoundaryPrimaryExhausts(t *testing.T) {
	ctx := base.TestCtx(t)
	// LIMIT 2: primary [A, B] hit limit → boundary = B. fallback [C, D] → C > B → stop
	primary := newMockIterator([][]byte{
		marshalRow("A", "a"),
		marshalRow("B", "b"),
	})
	fallback := newMockIterator([][]byte{
		marshalRow("C", "c"),
		marshalRow("D", "d"),
	})

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, 2)
	var ids []string
	for {
		var row PrincipalRow
		if !iter.Next(ctx, &row) {
			break
		}
		ids = append(ids, row.Id)
	}
	require.NoError(t, iter.Close())
	assert.Equal(t, []string{"A", "B"}, ids, "C and D should be suppressed by safe boundary at B")
}

// TestMergedDedupIteratorNoBoundaryWhenBelowLimit verifies that when a source returns fewer
// than limit rows (truly exhausted), no boundary is set and all remaining rows from the other
// source are emitted.
func TestMergedDedupIteratorNoBoundaryWhenBelowLimit(t *testing.T) {
	ctx := base.TestCtx(t)
	// LIMIT 5, primary returns 2 rows (< 5, truly exhausted), fallback returns 3.
	// No boundary should be set — all results should be emitted.
	primary := newMockIterator([][]byte{
		marshalRow("A", "a"),
		marshalRow("C", "c"),
	})
	fallback := newMockIterator([][]byte{
		marshalRow("B", "b"),
		marshalRow("D", "d"),
		marshalRow("E", "e"),
	})

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, 5)
	var ids []string
	for {
		var row PrincipalRow
		if !iter.Next(ctx, &row) {
			break
		}
		ids = append(ids, row.Id)
	}
	require.NoError(t, iter.Close())
	assert.Equal(t, []string{"A", "B", "C", "D", "E"}, ids, "all rows should be emitted when below limit")
}

// TestMergedDedupIteratorBoundaryWithDuplicates verifies the safe boundary works correctly
// when there are duplicate IDs across stores.
func TestMergedDedupIteratorBoundaryWithDuplicates(t *testing.T) {
	ctx := base.TestCtx(t)
	// LIMIT 3: primary [A, B, C] (hit limit), fallback [A, D, E] (hit limit).
	// Merge: A(primary, dup fallback discarded), B, C. Primary exhausts at 3==limit → boundary=C.
	// Fallback next peek: D > C → stop.
	// Output: [A, B, C].
	primary := newMockIterator([][]byte{
		marshalRow("A", "a-primary"),
		marshalRow("B", "b-primary"),
		marshalRow("C", "c-primary"),
	})
	fallback := newMockIterator([][]byte{
		marshalRow("A", "a-fallback"),
		marshalRow("D", "d-fallback"),
		marshalRow("E", "e-fallback"),
	})

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, 3)
	var rows []PrincipalRow
	for {
		var row PrincipalRow
		if !iter.Next(ctx, &row) {
			break
		}
		rows = append(rows, row)
	}
	require.NoError(t, iter.Close())
	require.Len(t, rows, 3)
	assert.Equal(t, "A", rows[0].Id)
	assert.Equal(t, "a-primary", rows[0].Name, "primary preferred for duplicate")
	assert.Equal(t, "B", rows[1].Id)
	assert.Equal(t, "C", rows[2].Id)
}

// TestMergedDedupIteratorUnlimited verifies that with limit=0, all rows from both stores are
// emitted without any boundary constraint.
func TestMergedDedupIteratorUnlimited(t *testing.T) {
	ctx := base.TestCtx(t)
	primary := newMockIterator([][]byte{
		marshalRow("A", "a"),
		marshalRow("E", "e"),
	})
	fallback := newMockIterator([][]byte{
		marshalRow("B", "b"),
		marshalRow("C", "c"),
	})

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, 0)
	var ids []string
	for {
		var row PrincipalRow
		if !iter.Next(ctx, &row) {
			break
		}
		ids = append(ids, row.Id)
	}
	require.NoError(t, iter.Close())
	assert.Equal(t, []string{"A", "B", "C", "E"}, ids, "all rows emitted when unlimited")
}

// TestMergedDeduplicateWithDuplicate verifies duplicates in differing orders from each store still produce the right result
func TestMergedDeduplicateWithDuplicate(t *testing.T) {
	ctx := base.TestCtx(t)
	primary := newMockIterator([][]byte{
		marshalRow("A", "a"),
		marshalRow("C", "c"),
	})
	fallback := newMockIterator([][]byte{
		marshalRow("C", "c"),
		marshalRow("D", "d"),
	})

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, 0)
	var ids []string
	for {
		var row PrincipalRow
		if !iter.Next(ctx, &row) {
			break
		}
		ids = append(ids, row.Id)
	}
	require.NoError(t, iter.Close())
	assert.Equal(t, []string{"A", "C", "D"}, ids)
}

// TestMergedDedupIteratorPaginationSimulation simulates the pagination loop used by GetUsers
// to verify that no IDs are missed across multiple pages when the stores have interleaved IDs.
func TestMergedDedupIteratorPaginationSimulation(t *testing.T) {
	// Full dataset:
	//   primary: [A, D, G, J]
	//   fallback: [B, C, E, F, H, I]
	// Expected global sorted unique: [A, B, C, D, E, F, G, H, I, J]
	// Pagination limit: 2 per page

	type storeData struct {
		primary  []string
		fallback []string
	}
	data := storeData{
		primary:  []string{"A", "D", "G", "J"},
		fallback: []string{"B", "C", "E", "F", "H", "I"},
	}

	const limit = 2
	var allCollected []string
	startKey := ""
	maxPages := 20 // safety limit

	for page := range maxPages {
		// Simulate the per-store query: filter rows >= startKey, then take first `limit`.
		filterAndLimit := func(ids []string) [][]byte {
			var rows [][]byte
			for _, id := range ids {
				if id >= startKey {
					rows = append(rows, marshalRow(id, id))
					if len(rows) >= limit {
						break
					}
				}
			}
			return rows
		}
		primary := newMockIterator(filterAndLimit(data.primary))
		fallback := newMockIterator(filterAndLimit(data.fallback))
		iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, limit)

		ctx := base.TestCtx(t)
		resultCount := 0
		for {
			var row PrincipalRow
			if !iter.Next(ctx, &row) {
				break
			}
			// Skip the overlapping first row (same as GetUsers pagination logic).
			if resultCount == 0 && startKey != "" && row.Id == startKey {
				resultCount++
				continue
			}
			startKey = row.Id
			resultCount++
			allCollected = append(allCollected, row.Id)
		}
		_ = iter.Close()

		if resultCount < limit {
			break
		}
		require.Less(t, page, maxPages-1, "pagination should terminate")
	}

	assert.Equal(t, []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}, allCollected,
		"all IDs should be collected across paginated queries")
}

// TestMergedDedupIteratorBothExhaustAtLimit verifies behaviour when both sources exhaust at
// exactly limit rows simultaneously. The first source to exhaust during the merge sets the
// boundary; the second source's remaining rows beyond the boundary are suppressed.
func TestMergedDedupIteratorBothExhaustAtLimit(t *testing.T) {
	ctx := base.TestCtx(t)
	// LIMIT 2. primary: [A, C], fallback: [B, D].
	// Merge: A(primary), B(fallback), C(primary). After C emitted, peekPrimary → nil.
	// primaryConsumed=2==limit → boundary=C. peekFallback has D already peeked.
	// D > C → stop.
	// Output: [A, B, C].
	primary := newMockIterator([][]byte{
		marshalRow("A", "a"),
		marshalRow("C", "c"),
	})
	fallback := newMockIterator([][]byte{
		marshalRow("B", "b"),
		marshalRow("D", "d"),
	})

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, 2)
	var ids []string
	for {
		var row PrincipalRow
		if !iter.Next(ctx, &row) {
			break
		}
		ids = append(ids, row.Id)
	}
	require.NoError(t, iter.Close())
	assert.Equal(t, []string{"A", "B", "C"}, ids,
		"first-to-exhaust sets boundary; D beyond boundary C should be suppressed")
}

// TestMergedDedupIteratorBoundaryIsExclusive verifies that a row whose ID equals the safe
// boundary is still emitted (the check is row.id > boundary, not >=).
func TestMergedDedupIteratorBoundaryIsExclusive(t *testing.T) {
	ctx := base.TestCtx(t)
	// LIMIT 2. primary: [B, D] (exhausts at limit → boundary=D).
	// fallback: [A, D, E].
	// Merge: A(fallback), B(primary), D==D → emit D(primary), discard D(fallback).
	// Primary exhausts at 2==limit → boundary=D.
	// Fallback peeks E. E > D → stop.
	// Output: [A, B, D]. D itself is emitted even though it equals boundary.
	primary := newMockIterator([][]byte{
		marshalRow("B", "b"),
		marshalRow("D", "d-primary"),
	})
	fallback := newMockIterator([][]byte{
		marshalRow("A", "a"),
		marshalRow("D", "d-fallback"),
		marshalRow("E", "e"),
	})

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, 2)
	var rows []PrincipalRow
	for {
		var row PrincipalRow
		if !iter.Next(ctx, &row) {
			break
		}
		rows = append(rows, row)
	}
	require.NoError(t, iter.Close())
	require.Len(t, rows, 3)
	assert.Equal(t, "A", rows[0].Id)
	assert.Equal(t, "B", rows[1].Id)
	assert.Equal(t, "D", rows[2].Id)
	assert.Equal(t, "d-primary", rows[2].Name, "primary preferred for duplicate at boundary")
}

// TestMergedDedupIteratorPaginationSimulationWithDuplicates simulates the GetUsers pagination
// loop when both stores contain overlapping IDs. This verifies that duplicates are correctly
// handled across page boundaries and no IDs are missed or emitted twice.
func TestMergedDedupIteratorPaginationSimulationWithDuplicates(t *testing.T) {
	// Full dataset:
	//   primary:  [A, B, D, F, G]
	//   fallback: [B, C, D, E, G, H]
	// Global sorted unique: [A, B, C, D, E, F, G, H]
	// Pagination limit: 3 per page

	type storeData struct {
		primary  []string
		fallback []string
	}
	data := storeData{
		primary:  []string{"A", "B", "D", "F", "G"},
		fallback: []string{"B", "C", "D", "E", "G", "H"},
	}

	const limit = 3
	var allCollected []string
	startKey := ""
	maxPages := 20

	for page := range maxPages {
		filterAndLimit := func(ids []string) [][]byte {
			var rows [][]byte
			for _, id := range ids {
				if id >= startKey {
					rows = append(rows, marshalRow(id, id))
					if len(rows) >= limit {
						break
					}
				}
			}
			return rows
		}
		primary := newMockIterator(filterAndLimit(data.primary))
		fallback := newMockIterator(filterAndLimit(data.fallback))
		iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, limit)

		ctx := base.TestCtx(t)
		resultCount := 0
		for {
			var row PrincipalRow
			if !iter.Next(ctx, &row) {
				break
			}
			if resultCount == 0 && startKey != "" && row.Id == startKey {
				resultCount++
				continue
			}
			startKey = row.Id
			resultCount++
			allCollected = append(allCollected, row.Id)
		}
		_ = iter.Close()

		if resultCount < limit {
			break
		}
		require.Less(t, page, maxPages-1, "pagination should terminate")
	}

	assert.Equal(t, []string{"A", "B", "C", "D", "E", "F", "G", "H"}, allCollected,
		"all unique IDs should be collected across paginated queries with duplicates")
}

// TestMergedDedupIteratorPaginationSimulationHeavySkew simulates pagination when one store
// has many more rows than the other, verifying that the boundary mechanism handles asymmetric
// distributions without skipping IDs.
func TestMergedDedupIteratorPaginationSimulationHeavySkew(t *testing.T) {
	// primary: [A, Z]  (only 2 rows, big gap)
	// fallback: [B, C, D, E, F, G, H, I, J]  (9 rows, dense)
	// Global sorted unique: [A, B, C, D, E, F, G, H, I, J, Z]
	// Pagination limit: 3

	type storeData struct {
		primary  []string
		fallback []string
	}
	data := storeData{
		primary:  []string{"A", "Z"},
		fallback: []string{"B", "C", "D", "E", "F", "G", "H", "I", "J"},
	}

	const limit = 3
	var allCollected []string
	startKey := ""
	maxPages := 20

	for page := range maxPages {
		filterAndLimit := func(ids []string) [][]byte {
			var rows [][]byte
			for _, id := range ids {
				if id >= startKey {
					rows = append(rows, marshalRow(id, id))
					if len(rows) >= limit {
						break
					}
				}
			}
			return rows
		}
		primary := newMockIterator(filterAndLimit(data.primary))
		fallback := newMockIterator(filterAndLimit(data.fallback))
		iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, limit)

		ctx := base.TestCtx(t)
		resultCount := 0
		for {
			var row PrincipalRow
			if !iter.Next(ctx, &row) {
				break
			}
			if resultCount == 0 && startKey != "" && row.Id == startKey {
				resultCount++
				continue
			}
			startKey = row.Id
			resultCount++
			allCollected = append(allCollected, row.Id)
		}
		_ = iter.Close()

		if resultCount < limit {
			break
		}
		require.Less(t, page, maxPages-1, "pagination should terminate")
	}

	assert.Equal(t, []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "Z"}, allCollected,
		"all IDs should be collected with heavily skewed store sizes")
}

// TestMergedDedupIteratorCloseErrorPropagation verifies that Close returns the primary
// iterator's error when both iterators return errors, and the fallback iterator's error when
// only the fallback fails.
func TestMergedDedupIteratorCloseErrorPropagation(t *testing.T) {
	t.Run("both_errors_returns_primary", func(t *testing.T) {
		mockPrimary := newMockIterator(nil)
		mockPrimary.closeErr = fmt.Errorf("primary close error")
		mockFallback := newMockIterator(nil)
		mockFallback.closeErr = fmt.Errorf("fallback close error")

		iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](mockPrimary, mockFallback, 0)
		err := iter.Close()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "primary close error",
			"primary error should take precedence")
		assert.True(t, mockPrimary.closed)
		assert.True(t, mockFallback.closed, "fallback should still be closed even when primary errors")
	})

	t.Run("only_fallback_error", func(t *testing.T) {
		mockPrimary := newMockIterator(nil)
		mockFallback := newMockIterator(nil)
		mockFallback.closeErr = fmt.Errorf("fallback close error")

		iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](mockPrimary, mockFallback, 0)
		err := iter.Close()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "fallback close error")
	})

	t.Run("no_errors", func(t *testing.T) {
		mockPrimary := newMockIterator(nil)
		mockFallback := newMockIterator(nil)

		iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](mockPrimary, mockFallback, 0)
		require.NoError(t, iter.Close())
	})
}

// TestMergedDedupIteratorOneEmpty verifies that One returns sgbucket.ErrNoRows when both
// iterators are empty, and that both iterators are closed.
func TestMergedDedupIteratorOneEmpty(t *testing.T) {
	ctx := base.TestCtx(t)
	mockPrimary := newMockIterator(nil)
	mockFallback := newMockIterator(nil)

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](mockPrimary, mockFallback, 0)
	var row PrincipalRow
	err := iter.One(ctx, &row)
	require.ErrorIs(t, err, sgbucket.ErrNoRows)
	assert.True(t, mockPrimary.closed, "primary should be closed after One")
	assert.True(t, mockFallback.closed, "fallback should be closed after One")
}

// TestMergedDedupIteratorOneFallbackRow verifies that One returns the first row when it comes
// from the fallback store (not just primary).
func TestMergedDedupIteratorOneFallbackRow(t *testing.T) {
	ctx := base.TestCtx(t)
	mockPrimary := newMockIterator(nil)
	mockFallback := newMockIterator([][]byte{
		marshalRow("_sync:user:alice", "alice"),
	})

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](mockPrimary, mockFallback, 0)
	var row PrincipalRow
	require.NoError(t, iter.One(ctx, &row))
	assert.Equal(t, "_sync:user:alice", row.Id)
	assert.True(t, mockPrimary.closed)
	assert.True(t, mockFallback.closed)
}

// TestMergedDedupIteratorBoundaryWithDiscardedDuplicates verifies that discarded fallback
// duplicates still count toward fallbackConsumed, correctly triggering the safe boundary when
// fallback has consumed exactly limit rows (including discards).
func TestMergedDedupIteratorBoundaryWithDiscardedDuplicates(t *testing.T) {
	ctx := base.TestCtx(t)
	// LIMIT 3. primary: [A, B, C] (3 rows = limit). fallback: [A, B, D] (3 rows = limit).
	// Merge: A(primary, discard A-fallback), B(primary, discard B-fallback), C(primary).
	// Primary exhausts at 3==limit → boundary=C.
	// Fallback peek: D (already consumed A,B as discards → fallbackConsumed=2).
	// D > C → stop.
	// Output: [A, B, C].
	// Key: fallback consumed 2 rows that were discarded + has D peeked but never consumed.
	// The boundary is set by primary exhausting, not by fallback's consumed count.
	primary := newMockIterator([][]byte{
		marshalRow("A", "a-primary"),
		marshalRow("B", "b-primary"),
		marshalRow("C", "c-primary"),
	})
	fallback := newMockIterator([][]byte{
		marshalRow("A", "a-fallback"),
		marshalRow("B", "b-fallback"),
		marshalRow("D", "d-fallback"),
	})

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, 3)
	var rows []PrincipalRow
	for {
		var row PrincipalRow
		if !iter.Next(ctx, &row) {
			break
		}
		rows = append(rows, row)
	}
	require.NoError(t, iter.Close())
	require.Len(t, rows, 3)
	assert.Equal(t, "A", rows[0].Id)
	assert.Equal(t, "a-primary", rows[0].Name, "primary preferred")
	assert.Equal(t, "B", rows[1].Id)
	assert.Equal(t, "b-primary", rows[1].Name, "primary preferred")
	assert.Equal(t, "C", rows[2].Id)
	// D from fallback should be suppressed because D > boundary C.
}

// TestMergedDedupIteratorNextBytesWithBoundary verifies that the safe boundary works correctly
// when consuming via the NextBytes path (raw JSON bytes).
func TestMergedDedupIteratorNextBytesWithBoundary(t *testing.T) {
	// LIMIT 2. primary: [A, E], fallback: [B, C].
	// Boundary set by fallback at C. E > C → suppressed.
	ctx := base.TestCtx(t)
	primary := newMockIterator([][]byte{
		marshalRow("A", "a"),
		marshalRow("E", "e"),
	})
	fallback := newMockIterator([][]byte{
		marshalRow("B", "b"),
		marshalRow("C", "c"),
	})

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, 2)
	var rows []PrincipalRow
	for {
		var row PrincipalRow
		if !iter.Next(ctx, &row) {
			break
		}
		rows = append(rows, row)
	}
	require.NoError(t, iter.Close())
	require.Len(t, rows, 3, "E should be suppressed by boundary at C")
	assert.Equal(t, "A", rows[0].Id)
	assert.Equal(t, "B", rows[1].Id)
	assert.Equal(t, "C", rows[2].Id)
}

func TestMergeDeudupIteratorBoundryIsDuplicate(t *testing.T) {
	ctx := base.TestCtx(t)
	// LIMIT 2. primary: [A, C], fallback: [B, C].
	// Boundary set by primary at C. C from fallback is duplicate and should be discarded, not emitted.
	primary := newMockIterator([][]byte{
		marshalRow("A", "a"),
		marshalRow("C", "c-primary"),
	})
	fallback := newMockIterator([][]byte{
		marshalRow("B", "b"),
		marshalRow("C", "c-fallback"),
	})

	iter := newDualMetadataStorePrincipalDedupIterator[PrincipalRow](primary, fallback, 2)
	var rows []PrincipalRow
	for {
		var row PrincipalRow
		if !iter.Next(ctx, &row) {
			break
		}
		rows = append(rows, row)
	}
	require.NoError(t, iter.Close())
	require.Len(t, rows, 3)
	assert.Equal(t, "A", rows[0].Id)
	assert.Equal(t, "B", rows[1].Id)
	assert.Equal(t, "C", rows[2].Id)
	assert.Equal(t, "c-primary", rows[2].Name, "primary version of C should be preferred and emitted at boundary; fallback C should be discarded")
}
