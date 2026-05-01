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

// identifiableRow is implemented by query row types that carry a document ID, enabling the
// generic dualMetadataStorePrincipalDedupIterator to extract IDs for merge-sort comparison
// without a separate parsing step.
type identifiableRow interface {
	rowID() string
}

// dualMetadataN1QLQuery executes the same N1QL statement independently against both the
// primary and fallback datastores of a *base.MetadataStore, then returns a
// dualMetadataStorePrincipalDedupIterator that merge-sorts the two result streams by document ID, deduplicating
// and preferring the primary-store version when both contain the same document.
//
// The type parameter T determines the row struct used for unmarshaling; it must implement
// identifiableRow so the iterator can extract document IDs for merge-sort comparison.
//
// The statement must still contain base.KeyspaceQueryToken; each store's Query method
// replaces this token with its own escaped keyspace before execution.
//
// The statement should NOT include a LIMIT clause — callers pass the desired limit separately.
// When limit > 0, each per-store query appends LIMIT <limit>. The dualMetadataStorePrincipalDedupIterator uses
// the limit to detect when a source hit its LIMIT (consumed exactly limit rows) vs. was truly
// exhausted (consumed fewer). When a source hits its LIMIT, the iterator establishes a "safe
// boundary" at that source's last emitted ID and stops returning rows from the other source
// beyond that boundary. This prevents the pagination cursor from jumping over IDs that the
// limited source may still have, without doubling the per-store LIMIT.
func dualMetadataN1QLQuery[T identifiableRow](ctx context.Context, ms *base.MetadataStore, queryName string,
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

	return newDualMetadataStorePrincipalDedupIterator[T](primaryIter, fallbackIter, limit), nil
}

// dualMetadataStorePrincipalDedupIterator performs a sorted merge of two sgbucket.QueryResultIterators (primary and
// fallback), emitting rows in ascending document-ID order. When both iterators contain a row
// with the same ID, only the primary version is emitted.
//
// The type parameter T must implement identifiableRow so that the iterator can extract the
// document ID from each row for merge-sort comparison. Each source row is unmarshaled exactly
// once at peek time; the merged result is copied directly into the caller's pointer in Next.
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
type dualMetadataStorePrincipalDedupIterator[T identifiableRow] struct {
	primary      sgbucket.QueryResultIterator
	fallback     sgbucket.QueryResultIterator
	primaryPeek  *T
	fallbackPeek *T

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

func newDualMetadataStorePrincipalDedupIterator[T identifiableRow](primary, fallback sgbucket.QueryResultIterator, limit int) *dualMetadataStorePrincipalDedupIterator[T] {
	return &dualMetadataStorePrincipalDedupIterator[T]{
		primary:  primary,
		fallback: fallback,
		limit:    limit,
	}
}

// peekPrimary ensures primaryPeek is populated by reading and unmarshaling the next row from
// the primary source iterator. Returns false if the primary iterator is exhausted.
func (m *dualMetadataStorePrincipalDedupIterator[T]) peekPrimary(ctx context.Context) bool {
	if m.primaryPeek != nil {
		return true
	}
	if m.primaryDone {
		return false
	}
	var row T
	if !m.primary.Next(ctx, &row) {
		m.primaryDone = true
		// Only an exhaustion after consuming exactly limit rows indicates the source may
		// have been truncated by the per-store LIMIT and therefore needs a safe boundary.
		if m.limit > 0 && m.primaryConsumed == m.limit && !m.hasBoundary {
			m.boundary = m.lastPrimaryID
			m.hasBoundary = true
		}
		return false
	}
	m.primaryConsumed++
	m.primaryPeek = &row
	return true
}

// peekFallback ensures fallbackPeek is populated by reading and unmarshaling the next row from
// the fallback source iterator. Returns false if the fallback iterator is exhausted.
func (m *dualMetadataStorePrincipalDedupIterator[T]) peekFallback(ctx context.Context) bool {
	if m.fallbackPeek != nil {
		return true
	}
	if m.fallbackDone {
		return false
	}
	var row T
	if !m.fallback.Next(ctx, &row) {
		m.fallbackDone = true
		// Only an exhaustion after consuming exactly limit rows indicates the source may
		// have been truncated by the per-store LIMIT and therefore needs a safe boundary.
		if m.limit > 0 && m.fallbackConsumed == m.limit && !m.hasBoundary {
			m.boundary = m.lastFallbackID
			m.hasBoundary = true
		}
		return false
	}
	m.fallbackConsumed++
	m.fallbackPeek = &row
	return true
}

// NextBytes is a no-op stub that satisfies the sgbucket.QueryResultIterator interface. Callers
// should use Next instead, which performs the merge-sort and returns typed rows.
func (m *dualMetadataStorePrincipalDedupIterator[T]) NextBytes() []byte {
	return nil
}

// Next populates valuePtr with the next merged row. Returns false when both iterators are
// exhausted or the safe boundary has been reached. valuePtr must be a *T.
func (m *dualMetadataStorePrincipalDedupIterator[T]) Next(ctx context.Context, valuePtr any) bool {
	hasPrimary := m.peekPrimary(ctx)
	hasFallback := m.peekFallback(ctx)

	if !hasPrimary && !hasFallback {
		return false
	}

	// Extract IDs from the peeked rows for merge-sort comparison.
	var primaryID, fallbackID string
	if hasPrimary {
		primaryID = (*m.primaryPeek).rowID()
	}
	if hasFallback {
		fallbackID = (*m.fallbackPeek).rowID()
	}

	// Determine which row to emit based on merge-sort comparison.
	var selected *T

	switch {
	case hasPrimary && !hasFallback:
		selected = m.primaryPeek
		m.lastPrimaryID = primaryID
		m.primaryPeek = nil
	case !hasPrimary && hasFallback:
		selected = m.fallbackPeek
		m.lastFallbackID = fallbackID
		m.fallbackPeek = nil
	case primaryID < fallbackID:
		selected = m.primaryPeek
		m.lastPrimaryID = primaryID
		m.primaryPeek = nil
	case primaryID > fallbackID:
		selected = m.fallbackPeek
		m.lastFallbackID = fallbackID
		m.fallbackPeek = nil
	default:
		// Equal IDs — prefer primary, discard fallback.
		selected = m.primaryPeek
		m.lastPrimaryID = primaryID
		m.primaryPeek = nil
		m.lastFallbackID = fallbackID
		m.fallbackPeek = nil
	}

	selectedID := (*selected).rowID()

	// Suppress rows from the other source that fall beyond the safe boundary. The boundary is
	// the last ID emitted by the source that hit its LIMIT, so any row with id == boundary was
	// already emitted (or deduped away via the default case above). Only rows strictly after
	// the boundary are unsafe, hence > rather than >=.
	if m.hasBoundary && selectedID > m.boundary {
		return false
	}

	// valuePtr arrives as any because Next must satisfy the sgbucket.QueryResultIterator
	// interface. At runtime it must be a *T — the same concrete type the iterator was
	// instantiated with. The assertion enforces this contract: a mismatch means the caller
	// is passing a pointer to a different struct, which would silently produce zero values
	// without this guard.
	if ptr, ok := valuePtr.(*T); ok {
		*ptr = *selected
	} else {
		base.WarnfCtx(ctx, "dualMetadataStorePrincipalDedupIterator: type mismatch: expected %T, got %T", (*T)(nil), valuePtr)
		return false
	}
	return true
}

// One unmarshals the first merged row into valuePtr and closes both iterators.
// Returns sgbucket.ErrNoRows when no rows are available.
func (m *dualMetadataStorePrincipalDedupIterator[T]) One(ctx context.Context, valuePtr any) error {
	defer func() {
		_ = m.Close()
	}()
	if !m.Next(ctx, valuePtr) {
		return sgbucket.ErrNoRows
	}
	return nil
}

// Close closes both the primary and fallback iterators.
func (m *dualMetadataStorePrincipalDedupIterator[T]) Close() error {
	primaryErr := m.primary.Close()
	fallbackErr := m.fallback.Close()
	if primaryErr != nil {
		return primaryErr
	}
	return fallbackErr
}
