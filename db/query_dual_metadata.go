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
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

// dualMetadataSortOrderKey is the N1QL column alias injected as a numeric literal into each
// arm of the UNION ALL query to enforce primary-before-fallback ordering. Primary arms receive
// value 0; fallback arms receive value 1. The outer ORDER BY uses this column as its first
// sort key, ensuring all primary rows are returned before any fallback rows regardless of
// N1QL's internal execution order.
const dualMetadataSortOrderKey = "sort_order"

// dualMetadataN1QLQuery executes a single UNION ALL N1QL statement across both the primary
// and fallback datastores of a *base.MetadataStore and returns a unionDedupIterator over the
// combined result stream.
//
// The original statement (which may still contain base.KeyspaceQueryToken) is expanded by
// buildDualMetadataUnionStatement into two fully-qualified sub-queries joined with UNION ALL.
// A sort_order literal (0=primary, 1=fallback) is injected into each arm, and a single outer
// ORDER BY sort_order[, <original order>] guarantees that all primary rows precede all
// fallback rows in the result stream. unionDedupIterator relies on this ordering: "first seen
// wins" always selects the primary-store version when the same META().id exists in both stores.
//
// The combined query is executed against the fallback (default) store. Because both keyspaces
// are referenced with fully-qualified names, the fallback store's query_context does not
// restrict access to the primary keyspace.
//
// When a query limit is present in the statement it applies independently to each sub-query,
// so the merged stream may return up to 2x the requested limit of unique rows.
func dualMetadataN1QLQuery(ctx context.Context, ms *base.MetadataStore, queryName string,
	statement string, params map[string]any, consistency base.ConsistencyMode, adhoc bool,
	dbStats *base.DbStats, slowQueryWarningThreshold time.Duration) (sgbucket.QueryResultIterator, error) {

	primaryN1QL, ok := base.AsN1QLStore(ms.Primary())
	if !ok {
		return nil, fmt.Errorf("primary datastore (%T) does not support N1QL", ms.Primary())
	}
	fallbackN1QL, ok := base.AsN1QLStore(ms.Fallback())
	if !ok {
		return nil, fmt.Errorf("fallback datastore (%T) does not support N1QL", ms.Fallback())
	}

	unionStatement := buildDualMetadataUnionStatement(statement, primaryN1QL.EscapedKeyspace(), fallbackN1QL.EscapedKeyspace())

	// Execute against the fallback (default) store. Both keyspaces are fully-qualified in the
	// statement, so the fallback store's query_context does not restrict which keyspaces are
	// accessible.
	iter, err := N1QLQueryWithStats(ctx, ms.Fallback(), queryName, unionStatement, params, consistency, adhoc, dbStats, slowQueryWarningThreshold)
	if err != nil {
		return nil, fmt.Errorf("dual metadata UNION ALL N1QL query: %w", err)
	}
	return newUnionDedupIterator(iter), nil
}

// buildDualMetadataUnionStatement expands a single-keyspace N1QL statement into a UNION ALL
// across the primary and fallback keyspaces. A sort_order literal is injected into each arm's
// SELECT list (0 for primary, 1 for fallback), and base.KeyspaceQueryToken is replaced with
// the actual fully-qualified keyspace name.
//
// Any ORDER BY clause present in the original statement is stripped from the individual arms
// and re-applied as a single outer ORDER BY on the full UNION ALL result, with sort_order
// prepended so that primary rows always sort before fallback rows. Any LIMIT clause is
// preserved in each arm so it continues to apply independently per sub-query.
//
// The outer ORDER BY translates META(<alias>).id references to just `id`, which is the
// projected column name at the UNION ALL result scope.
//
// Example output (simplified, no inner ORDER BY, no LIMIT):
//
//	(SELECT name, META(alias).id, 0 AS sort_order
//	   FROM `bucket`.`_system`.`_mobile` AS alias WHERE ...)
//	UNION ALL
//	(SELECT name, META(alias).id, 1 AS sort_order
//	   FROM `bucket`.`_default`.`_default` AS alias WHERE ...)
//	ORDER BY sort_order, id
func buildDualMetadataUnionStatement(statement, primaryEscapedKeyspace, fallbackEscapedKeyspace string) string {
	// Strip the inner ORDER BY so the outer ORDER BY governs the full merged result.
	// Any LIMIT clause is re-attached to each arm so it continues to bound each sub-query.
	coreStmt, innerOrderBy, limitClause := splitStatement(statement)

	// " FROM $_keyspace" appears exactly once in each principal query statement. We prepend the
	// sort_order literal to the SELECT column list by inserting before the FROM token, then
	// replace the token with the actual escaped keyspace name.
	inject := func(escapedKeyspace string, sortOrder int) string {
		fromToken := " FROM " + base.KeyspaceQueryToken
		withMeta := strings.Replace(
			coreStmt,
			fromToken,
			fmt.Sprintf(", %d AS %s%s", sortOrder, dualMetadataSortOrderKey, fromToken),
			1,
		)
		arm := strings.ReplaceAll(withMeta, base.KeyspaceQueryToken, escapedKeyspace)
		return arm + limitClause
	}

	union := "(" + inject(primaryEscapedKeyspace, 0) + ") UNION ALL (" + inject(fallbackEscapedKeyspace, 1) + ")"

	// Build the outer ORDER BY: sort_order (0=primary, 1=fallback) takes precedence so all
	// primary rows precede all fallback rows in the merged stream.  The original inner ORDER BY
	// columns are appended so document ordering within each store is preserved.
	// META(<alias>).id is translated to just `id` because at the outer UNION ALL scope the
	// document key is projected by its field name.
	outerOrderBy := dualMetadataSortOrderKey
	if innerOrderBy != "" {
		adapted := strings.ReplaceAll(innerOrderBy, fmt.Sprintf("META(%s).id", base.KeyspaceQueryAlias), "id")
		outerOrderBy += ", " + adapted
	}

	return union + " ORDER BY " + outerOrderBy
}

// splitStatement splits a N1QL statement into three parts following the standard clause order:
// core (everything before ORDER BY), orderByColumns (the ORDER BY expression list without the
// "ORDER BY" keyword), and limitClause (the LIMIT clause including the "LIMIT" keyword).
//
// All keyword matching is case-insensitive. If ORDER BY or LIMIT are absent the corresponding
// return values are empty strings.
func splitStatement(stmt string) (core, orderByColumns, limitClause string) {
	upper := strings.ToUpper(stmt)

	orderByIdx := strings.LastIndex(upper, " ORDER BY ")
	if orderByIdx < 0 {
		// No ORDER BY; check for a bare LIMIT (unusual but possible).
		limitIdx := strings.LastIndex(upper, " LIMIT ")
		if limitIdx < 0 {
			return stmt, "", ""
		}
		return stmt[:limitIdx], "", stmt[limitIdx:]
	}

	core = stmt[:orderByIdx]
	remainder := stmt[orderByIdx+len(" ORDER BY "):]

	upperRemainder := strings.ToUpper(remainder)
	limitIdx := strings.Index(upperRemainder, " LIMIT ")
	if limitIdx < 0 {
		return core, remainder, ""
	}
	return core, remainder[:limitIdx], remainder[limitIdx:]
}

// unionDedupIterator wraps a single sgbucket.QueryResultIterator (typically the result of a
// UNION ALL query) and emits each unique META().id exactly once.
//
// Deduplication relies on the sort_order column injected by buildDualMetadataUnionStatement:
// the outer ORDER BY sort_order guarantees that all primary-keyspace rows (sort_order=0)
// are returned before any fallback-keyspace rows (sort_order=1). As a result, "first seen
// wins" always selects the primary-store version when the same document exists in both
// keystores. The sort_order column injected by buildDualMetadataUnionStatement
// is present in each emitted row but are harmlessly ignored by callers that unmarshal into
// typed structs.
type unionDedupIterator struct {
	iter    sgbucket.QueryResultIterator
	seenIDs map[string]struct{}
}

// Compile-time assertion that *unionDedupIterator implements sgbucket.QueryResultIterator.
var _ sgbucket.QueryResultIterator = (*unionDedupIterator)(nil)

func newUnionDedupIterator(iter sgbucket.QueryResultIterator) *unionDedupIterator {
	return &unionDedupIterator{
		iter:    iter,
		seenIDs: make(map[string]struct{}),
	}
}

// NextBytes returns the raw JSON bytes of the next non-duplicate row, or nil when exhausted.
// Rows whose META().id has already been emitted are silently skipped.
func (u *unionDedupIterator) NextBytes() []byte {
	for {
		raw := u.iter.NextBytes()
		if raw == nil {
			return nil
		}
		id := extractRowID(raw)
		if _, exists := u.seenIDs[id]; exists {
			continue
		}
		u.seenIDs[id] = struct{}{}
		return raw
	}
}

// Next unmarshals the next non-duplicate row into valuePtr. Returns false when exhausted.
func (u *unionDedupIterator) Next(ctx context.Context, valuePtr any) bool {
	raw := u.NextBytes()
	if raw == nil {
		return false
	}
	if err := base.JSONUnmarshal(raw, valuePtr); err != nil {
		base.WarnfCtx(ctx, "unionDedupIterator: failed to unmarshal row: %v", err)
		return false
	}
	return true
}

// One unmarshals the first non-duplicate row into valuePtr and closes the iterator.
// Returns sgbucket.ErrNoRows when the stream is empty.
func (u *unionDedupIterator) One(ctx context.Context, valuePtr any) error {
	defer func() { _ = u.Close() }()
	if !u.Next(ctx, valuePtr) {
		return sgbucket.ErrNoRows
	}
	return nil
}

// Close closes the underlying iterator.
func (u *unionDedupIterator) Close() error {
	return u.iter.Close()
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
