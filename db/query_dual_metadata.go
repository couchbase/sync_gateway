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
	"strconv"
	"strings"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

// dualMetadataSortOrderKey is the N1QL column alias injected as a numeric literal into each
// arm of the UNION ALL query. Primary arms receive value 0; fallback arms receive value 1.
// The outer ORDER BY places sort_order after id, so that for equal document IDs the
// primary-store row (0) is returned before the fallback-store row (1). This guarantees the
// unionDedupIterator's "first seen wins" strategy always selects the primary-store version.
const dualMetadataSortOrderKey = "sort_order"

// dualMetadataN1QLQuery executes a single UNION ALL N1QL statement across both the primary
// and fallback datastores of a *base.MetadataStore and returns a unionDedupIterator over the
// combined result stream.
//
// The original statement (which may still contain base.KeyspaceQueryToken) is expanded by
// buildDualMetadataUnionStatement into two fully-qualified sub-queries joined with UNION ALL.
// A sort_order literal (0=primary, 1=fallback) is injected into each arm, and a single outer
// ORDER BY id[, <original order>], sort_order ensures document IDs are monotonically
// non-decreasing (required for startKey-based pagination) while for equal IDs the primary-store
// row sorts first (required for correct deduplication by unionDedupIterator).
//
// The combined query is executed against the fallback (default) store. Because both keyspaces
// are referenced with fully-qualified names, the fallback store's query_context does not
// restrict access to the primary keyspace.
//
// Any LIMIT clause from the original statement is moved from the individual arms to the outer
// UNION ALL and doubled to account for worst-case duplicate overlap. This prevents per-arm
// limits from permanently hiding results when the two stores have interleaved ID ranges.
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
// and re-applied as a single outer ORDER BY on the full UNION ALL result. The outer ORDER BY
// uses the document id as the primary sort key (for correct startKey-based pagination) and
// appends sort_order so that for equal IDs the primary-store row (0) precedes the
// fallback-store row (1), enabling unionDedupIterator's "first seen wins" deduplication.
//
// Any LIMIT clause is doubled on both the individual arms and the outer UNION ALL to account
// for worst-case duplicate overlap between the two stores (each document can appear at most
// twice — once per store). The per-arm doubled limit bounds how many rows each sub-query
// reads, while the outer doubled limit caps the merged stream. With at most 2 copies per ID,
// 2*N raw rows always yield at least N unique results after deduplication, ensuring the
// pagination termination condition in GetUsers works correctly.
//
// The outer ORDER BY translates META(<alias>).id references to just `id`, which is the
// projected column name at the UNION ALL result scope.
//
// Example output (simplified, original LIMIT 3):
//
//	(SELECT name, META(alias).id, 0 AS sort_order
//	   FROM `bucket`.`_system`.`_mobile` AS alias WHERE ... LIMIT 6)
//	UNION ALL
//	(SELECT name, META(alias).id, 1 AS sort_order
//	   FROM `bucket`.`_default`.`_default` AS alias WHERE ... LIMIT 6)
//	ORDER BY id, sort_order LIMIT 6
func buildDualMetadataUnionStatement(statement, primaryEscapedKeyspace, fallbackEscapedKeyspace string) string {
	// Strip the inner ORDER BY so the outer ORDER BY governs the full merged result.
	// The LIMIT clause is extracted, doubled, and applied to both each arm (bounding per-store
	// read cost) and the outer UNION ALL (capping the merged stream).
	coreStmt, innerOrderBy, limitClause := splitStatement(statement)

	doubledLimit := doubleLimitClause(limitClause)

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
		return arm + doubledLimit
	}

	union := "(" + inject(primaryEscapedKeyspace, 0) + ") UNION ALL (" + inject(fallbackEscapedKeyspace, 1) + ")"

	// Build the outer ORDER BY: id first (for correct startKey-based pagination), then
	// sort_order so that for equal document IDs the primary-store row (0) precedes the
	// fallback-store row (1). Any original inner ORDER BY columns are used as the id portion;
	// META(<alias>).id is translated to just `id` at the outer UNION ALL scope.
	outerOrderBy := "id"
	if innerOrderBy != "" {
		outerOrderBy = strings.ReplaceAll(innerOrderBy, fmt.Sprintf("META(%s).id", base.KeyspaceQueryAlias), "id")
	}
	outerOrderBy += ", " + dualMetadataSortOrderKey

	return union + " ORDER BY " + outerOrderBy + doubledLimit
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

// doubleLimitClause parses a LIMIT clause string (e.g. " LIMIT 10") and returns a new clause
// with the limit value doubled (e.g. " LIMIT 20"). If the clause is empty or cannot be parsed
// the original clause is returned unchanged.
func doubleLimitClause(limitClause string) string {
	if limitClause == "" {
		return ""
	}
	trimmed := strings.TrimSpace(limitClause)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "LIMIT ") {
		return limitClause
	}
	numStr := strings.TrimSpace(trimmed[len("LIMIT "):])
	n, err := strconv.Atoi(numStr)
	if err != nil {
		return limitClause
	}
	return fmt.Sprintf(" LIMIT %d", n*2)
}

// unionDedupIterator wraps a single sgbucket.QueryResultIterator (typically the result of a
// UNION ALL query) and emits each unique META().id exactly once.
//
// Deduplication relies on the sort_order column injected by buildDualMetadataUnionStatement:
// the outer ORDER BY id, sort_order guarantees that for equal document IDs the primary-keyspace
// row (sort_order=0) is returned before the fallback-keyspace row (sort_order=1). As a result,
// "first seen wins" always selects the primary-store version when the same document exists in
// both keystores. The sort_order column is present in each emitted row but is harmlessly
// ignored by callers that unmarshal into typed structs.
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
