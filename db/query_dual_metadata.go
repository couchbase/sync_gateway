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
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

// dualMetadataN1QLQuery executes the same N1QL statement independently against both the
// primary and fallback datastores of a *base.MetadataStore, then returns a
// mergedDedupIterator that merge-sorts the two result streams by document ID, deduplicating
// and preferring the primary-store version when both contain the same document.
//
// The statement must still contain base.KeyspaceQueryToken; each store's Query method
// replaces this token with its own escaped keyspace before execution.
//
// The statement should NOT include a LIMIT clause — callers pass the desired limit separately.
// When limit > 0, each per-store query appends LIMIT <limit>. The mergedDedupIterator uses
// the limit to detect when a source hit its LIMIT (consumed exactly limit rows) vs. was truly
// exhausted (consumed fewer). When a source hits its LIMIT, the iterator establishes a "safe
// boundary" at that source's last emitted ID and stops returning rows from the other source
// beyond that boundary. This prevents the pagination cursor from jumping over IDs that the
// limited source may still have, without doubling the per-store LIMIT.
func dualMetadataN1QLQuery(ctx context.Context, ms *base.MetadataStore, queryName string,
	statement string, params map[string]any, consistency base.ConsistencyMode, adhoc bool,
	dbStats *base.DbStats, slowQueryWarningThreshold time.Duration, limit int) (sgbucket.QueryResultIterator, error) {

	storeStatement := statement
	if limit > 0 {
		storeStatement = fmt.Sprintf("%s LIMIT %d", statement, limit)
	}

	primaryIter, err := N1QLQueryWithStats(ctx, ms.Primary(), queryName, storeStatement, params, consistency, adhoc, dbStats, slowQueryWarningThreshold)
	if err != nil {
		return nil, fmt.Errorf("dual metadata primary N1QL query: %w", err)
	}

	fallbackIter, err := N1QLQueryWithStats(ctx, ms.Fallback(), queryName, storeStatement, params, consistency, adhoc, dbStats, slowQueryWarningThreshold)
	if err != nil {
		_ = primaryIter.Close()
		return nil, fmt.Errorf("dual metadata fallback N1QL query: %w", err)
	}

	return newMergedDedupIterator(primaryIter, fallbackIter, limit), nil
}

// peekedRow holds a row that has been read from a source iterator but not yet emitted.
type peekedRow struct {
	raw []byte
	id  string
}

// mergedDedupIterator performs a sorted merge of two sgbucket.QueryResultIterators (primary and
// fallback), emitting rows in ascending document-ID order. When both iterators contain a row
// with the same ID, only the primary version is emitted.
//
// Both source iterators must return rows sorted by META().id ascending (the standard ordering
// for principal queries). The merge maintains this ordering in the output.
//
// When limit > 0, the iterator tracks how many rows each source produced. If a source returns
// exactly limit rows and then exhausts, it may have been truncated by the N1QL LIMIT. In that
// case a "safe boundary" is set to that source's last ID, and no further rows from the other
// source with IDs beyond the boundary are emitted. This prevents the pagination cursor from
// advancing past IDs the limited source might still have, ensuring subsequent pages do not
// skip results. When a source returns fewer than limit rows, it is truly exhausted and no
// boundary is imposed.
type mergedDedupIterator struct {
	primary      sgbucket.QueryResultIterator
	fallback     sgbucket.QueryResultIterator
	primaryPeek  *peekedRow
	fallbackPeek *peekedRow

	// Per-source consumed counts and last IDs for safe-boundary detection.
	primaryConsumed  int
	fallbackConsumed int
	lastPrimaryID    string
	lastFallbackID   string
	limit            int // per-store N1QL LIMIT; 0 = unlimited

	// Safe boundary: when a source exhausts at exactly limit rows, boundary is set to its
	// last consumed ID. Rows from the other source with ID > boundary are suppressed.
	boundary    string
	hasBoundary bool

	primaryDone  bool
	fallbackDone bool
}

// Compile-time assertion that *mergedDedupIterator implements sgbucket.QueryResultIterator.
var _ sgbucket.QueryResultIterator = (*mergedDedupIterator)(nil)

func newMergedDedupIterator(primary, fallback sgbucket.QueryResultIterator, limit int) *mergedDedupIterator {
	return &mergedDedupIterator{
		primary:  primary,
		fallback: fallback,
		limit:    limit,
	}
}

// peekPrimary ensures primaryPeek is populated. Returns false if the primary iterator is
// exhausted.
func (m *mergedDedupIterator) peekPrimary() bool {
	if m.primaryPeek != nil {
		return true
	}
	if m.primaryDone {
		return false
	}
	raw := m.primary.NextBytes()
	if raw == nil {
		m.primaryDone = true
		// If consumed exactly limit rows, primary may have been truncated.
		if m.limit > 0 && m.primaryConsumed >= m.limit && !m.hasBoundary {
			m.boundary = m.lastPrimaryID
			m.hasBoundary = true
		}
		return false
	}
	m.primaryConsumed++
	m.primaryPeek = &peekedRow{raw: raw, id: extractRowID(raw)}
	return true
}

// peekFallback ensures fallbackPeek is populated. Returns false if the fallback iterator is
// exhausted.
func (m *mergedDedupIterator) peekFallback() bool {
	if m.fallbackPeek != nil {
		return true
	}
	if m.fallbackDone {
		return false
	}
	raw := m.fallback.NextBytes()
	if raw == nil {
		m.fallbackDone = true
		// If consumed exactly limit rows, fallback may have been truncated.
		if m.limit > 0 && m.fallbackConsumed >= m.limit && !m.hasBoundary {
			m.boundary = m.lastFallbackID
			m.hasBoundary = true
		}
		return false
	}
	m.fallbackConsumed++
	m.fallbackPeek = &peekedRow{raw: raw, id: extractRowID(raw)}
	return true
}

// NextBytes returns the raw JSON bytes of the next row from the sorted merge, or nil when
// both iterators are exhausted or the safe boundary has been reached.
func (m *mergedDedupIterator) NextBytes() []byte {
	hasPrimary := m.peekPrimary()
	hasFallback := m.peekFallback()

	if !hasPrimary && !hasFallback {
		return nil
	}

	// Determine which row to emit based on merge-sort comparison.
	var row *peekedRow
	var fromPrimary bool

	switch {
	case hasPrimary && !hasFallback:
		row = m.primaryPeek
		fromPrimary = true
	case !hasPrimary && hasFallback:
		row = m.fallbackPeek
		fromPrimary = false
	case m.primaryPeek.id < m.fallbackPeek.id:
		row = m.primaryPeek
		fromPrimary = true
	case m.primaryPeek.id > m.fallbackPeek.id:
		row = m.fallbackPeek
		fromPrimary = false
	default:
		// Equal IDs — prefer primary, discard fallback.
		row = m.primaryPeek
		fromPrimary = true
		m.lastFallbackID = m.fallbackPeek.id
		m.fallbackPeek = nil // discard duplicate
	}

	// Check safe boundary: do not emit rows beyond the boundary.
	if m.hasBoundary && row.id > m.boundary {
		return nil
	}

	// Consume the selected row and track the last ID for boundary detection.
	if fromPrimary {
		m.lastPrimaryID = row.id
		m.primaryPeek = nil
	} else {
		m.lastFallbackID = row.id
		m.fallbackPeek = nil
	}

	return row.raw
}

// Next unmarshals the next merged row into valuePtr. Returns false when exhausted.
func (m *mergedDedupIterator) Next(ctx context.Context, valuePtr any) bool {
	raw := m.NextBytes()
	if raw == nil {
		return false
	}
	if err := base.JSONUnmarshal(raw, valuePtr); err != nil {
		base.WarnfCtx(ctx, "mergedDedupIterator: failed to unmarshal row: %v", err)
		return false
	}
	return true
}

// One unmarshals the first merged row into valuePtr and closes both iterators.
// Returns sgbucket.ErrNoRows when no rows are available.
func (m *mergedDedupIterator) One(ctx context.Context, valuePtr any) error {
	defer func() {
		_ = m.Close()
	}()
	if !m.Next(ctx, valuePtr) {
		return sgbucket.ErrNoRows
	}
	return nil
}

// Close closes both the primary and fallback iterators.
func (m *mergedDedupIterator) Close() error {
	primaryErr := m.primary.Close()
	fallbackErr := m.fallback.Close()
	if primaryErr != nil {
		return primaryErr
	}
	return fallbackErr
}

// idOnlyRow is a minimal struct used to extract META().id from a raw N1QL result row.
// All principal query SELECT clauses serialise META(<alias>).id as the top-level "id" field.
type idOnlyRow struct {
	ID string `json:"id"`
}

// extractRowID extracts the META().id value from a raw N1QL result row JSON byte slice.
// Returns an empty string if the bytes cannot be parsed or the "id" field is absent.
func extractRowID(raw []byte) string {
	var row idOnlyRow
	if err := base.JSONUnmarshal(raw, &row); err != nil {
		return ""
	}
	return row.ID
}
