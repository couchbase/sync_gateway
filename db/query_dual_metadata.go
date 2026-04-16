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

// dualMetadataN1QLQuery runs the given N1QL statement against both the primary and fallback
// datastores of a *base.MetadataStore and returns a mergeDedupIterator over the two result
// streams. The primary datastore results are emitted first; fallback results are only emitted
// for documents whose META().id was not already seen in the primary stream.
//
// When a query limit is present in the statement it applies independently to each store, so
// the merged stream may return up to 2× the requested limit of unique rows. Callers that
// need strict limits should truncate after the desired number of rows.
func dualMetadataN1QLQuery(ctx context.Context, ms *base.MetadataStore, queryName string,
	statement string, params map[string]any, consistency base.ConsistencyMode, adhoc bool,
	dbStats *base.DbStats, slowQueryWarningThreshold time.Duration) (sgbucket.QueryResultIterator, error) {

	primaryIter, err := N1QLQueryWithStats(ctx, ms.Primary(), queryName, statement, params, consistency, adhoc, dbStats, slowQueryWarningThreshold)
	if err != nil {
		return nil, fmt.Errorf("dual metadata N1QL query on primary store: %w", err)
	}

	fallbackIter, err := N1QLQueryWithStats(ctx, ms.Fallback(), queryName, statement, params, consistency, adhoc, dbStats, slowQueryWarningThreshold)
	if err != nil {
		_ = primaryIter.Close()
		return nil, fmt.Errorf("dual metadata N1QL query on fallback store: %w", err)
	}

	return newMergeDedupIterator(primaryIter, fallbackIter), nil
}

// mergeDedupIterator wraps two sgbucket.QueryResultIterators and emits each unique META().id
// exactly once. The primary iterator is exhausted first; results from the fallback iterator
// are only emitted when their META().id was not already seen in the primary stream. This
// ensures the primary datastore version is always preferred when the same document exists in
// both stores.
type mergeDedupIterator struct {
	primary     sgbucket.QueryResultIterator
	fallback    sgbucket.QueryResultIterator
	seenIDs     map[string]struct{}
	primaryDone bool
}

// Compile-time assertion that *mergeDedupIterator implements sgbucket.QueryResultIterator.
var _ sgbucket.QueryResultIterator = (*mergeDedupIterator)(nil)

func newMergeDedupIterator(primary, fallback sgbucket.QueryResultIterator) *mergeDedupIterator {
	return &mergeDedupIterator{
		primary:  primary,
		fallback: fallback,
		seenIDs:  make(map[string]struct{}),
	}
}

// NextBytes returns the raw JSON bytes of the next non-duplicate result row, or nil when the
// merged stream is exhausted. Primary rows are emitted first; fallback rows whose META().id
// was already seen from primary are silently skipped.
func (m *mergeDedupIterator) NextBytes() []byte {
	// Drain the primary iterator first.
	if !m.primaryDone {
		if raw := m.primary.NextBytes(); raw != nil {
			id := extractRowID(raw)
			m.seenIDs[id] = struct{}{}
			return raw
		}
		m.primaryDone = true
	}

	// Drain the fallback iterator, skipping any document already seen in primary.
	for {
		raw := m.fallback.NextBytes()
		if raw == nil {
			return nil
		}
		id := extractRowID(raw)
		if _, seen := m.seenIDs[id]; seen {
			continue
		}
		m.seenIDs[id] = struct{}{}
		return raw
	}
}

// Next unmarshals the next non-duplicate result row into valuePtr. Returns false when the
// merged stream is exhausted.
func (m *mergeDedupIterator) Next(_ context.Context, valuePtr any) bool {
	raw := m.NextBytes()
	if raw == nil {
		return false
	}
	if err := base.JSONUnmarshal(raw, valuePtr); err != nil {
		base.WarnfCtx(context.Background(), "mergeDedupIterator: failed to unmarshal row: %v", err)
		return false
	}
	return true
}

// One unmarshals the first non-duplicate result row into valuePtr and closes the iterator.
// Returns sgbucket.ErrNoRows when the merged stream is empty.
func (m *mergeDedupIterator) One(ctx context.Context, valuePtr any) error {
	defer func() {
		_ = m.Close()
	}()

	if !m.Next(ctx, valuePtr) {
		return sgbucket.ErrNoRows
	}
	return nil
}

// Close closes both underlying iterators. The first non-nil error encountered is returned.
func (m *mergeDedupIterator) Close() error {
	primaryErr := m.primary.Close()
	fallbackErr := m.fallback.Close()
	if primaryErr != nil {
		return primaryErr
	}
	return fallbackErr
}

// idOnlyRow is a minimal struct used to extract META().id from a raw N1QL result row.
// All principal query SELECT clauses include META(<alias>).id which serialises as the
// top-level "id" JSON field.
type idOnlyRow struct {
	ID string `json:"id"`
}

// extractRowID extracts the META().id value from a raw N1QL result row JSON byte slice.
// Returns an empty string if the bytes cannot be parsed or the field is absent.
// SHOULD WE REMOVE THIS AND JUST USE NEXT TO ASSIGN ID TO MAP??
func extractRowID(raw []byte) string {
	var row idOnlyRow
	if err := base.JSONUnmarshal(raw, &row); err != nil {
		return ""
	}
	return row.ID
}
