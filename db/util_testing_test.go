//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestQueryHandlerForTestFilters covers the test double's own filtering, so it stays faithful to
// DatabaseCollection.QueryChannels. It has to live in package db because
// getChangesInChannelFromQuery is unexported - ChannelQueryHandler's only method.
//
// The bounds contract being pinned here: startSeq and endSeq are both inclusive, endSeq of 0
// means unbounded, and both are applied before the limit.
func TestQueryHandlerForTestFilters(t *testing.T) {
	const chanA, chanB = "chanA", "chanB"

	// seq 1-5 in chanA, seq 6 in chanB only.
	seed := func() *QueryHandlerForTest {
		qh := &QueryHandlerForTest{}
		entries := make(LogEntries, 0, 6)
		for seq := 1; seq <= 5; seq++ {
			entries = append(entries, MakeTestLogEntryForChannels(seq, []string{chanA}))
		}
		entries = append(entries, MakeTestLogEntryForChannels(6, []string{chanB}))
		qh.SeedEntries(entries)
		return qh
	}

	testCases := []struct {
		name       string
		channel    string
		startSeq   uint64
		endSeq     uint64
		limit      int
		activeOnly bool
		expected   []uint64
	}{
		{
			name:     "full range, endSeq unbounded",
			channel:  chanA,
			startSeq: 1,
			endSeq:   0,
			expected: []uint64{1, 2, 3, 4, 5},
		},
		{
			name:     "startSeq is inclusive",
			channel:  chanA,
			startSeq: 3,
			endSeq:   0,
			expected: []uint64{3, 4, 5},
		},
		{
			name:     "endSeq is inclusive",
			channel:  chanA,
			startSeq: 1,
			endSeq:   3,
			expected: []uint64{1, 2, 3},
		},
		{
			name:     "both bounds inclusive, single sequence",
			channel:  chanA,
			startSeq: 3,
			endSeq:   3,
			expected: []uint64{3},
		},
		{
			name:     "range excludes everything",
			channel:  chanA,
			startSeq: 10,
			endSeq:   20,
			expected: []uint64{},
		},
		{
			name:     "other channel is filtered out",
			channel:  chanB,
			startSeq: 1,
			endSeq:   0,
			expected: []uint64{6},
		},
		{
			name:     "unknown channel returns nothing",
			channel:  "nosuchchannel",
			startSeq: 1,
			endSeq:   0,
			expected: []uint64{},
		},
		{
			name:     "limit truncates the result",
			channel:  chanA,
			startSeq: 1,
			endSeq:   0,
			limit:    2,
			expected: []uint64{1, 2},
		},
		{
			// The case the bounds fix exists for: entries below startSeq must not consume the
			// limit. Filtering after the limit would return 1 and 2 here.
			name:     "bounds are applied before the limit",
			channel:  chanA,
			startSeq: 3,
			endSeq:   0,
			limit:    2,
			expected: []uint64{3, 4},
		},
		{
			name:     "limit larger than the range",
			channel:  chanA,
			startSeq: 4,
			endSeq:   0,
			limit:    10,
			expected: []uint64{4, 5},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			qh := seed()
			entries, err := qh.getChangesInChannelFromQuery(base.TestCtx(t), testCase.channel,
				testCase.startSeq, testCase.endSeq, testCase.limit, testCase.activeOnly)
			require.NoError(t, err)
			require.Len(t, entries, len(testCase.expected))
			for i, expectedSeq := range testCase.expected {
				assert.Equal(t, expectedSeq, entries[i].Sequence,
					fmt.Sprintf("unexpected sequence at index %d", i))
			}
		})
	}
}

// TestQueryHandlerForTestActiveOnly covers the activeOnly filter, which the cache passes through
// from ChangesOptions and which no existing test exercises.
func TestQueryHandlerForTestActiveOnly(t *testing.T) {
	const channel = "chanA"

	active := MakeTestLogEntryForChannels(1, []string{channel})
	removed := MakeTestLogEntryForChannels(2, []string{channel})
	removed.SetRemoved()
	deleted := MakeTestLogEntryForChannels(3, []string{channel})
	deleted.SetDeleted()
	alsoActive := MakeTestLogEntryForChannels(4, []string{channel})

	qh := &QueryHandlerForTest{}
	qh.SeedEntries(LogEntries{active, removed, deleted, alsoActive})

	entries, err := qh.getChangesInChannelFromQuery(base.TestCtx(t), channel, 1, 0, 0, false)
	require.NoError(t, err)
	require.Len(t, entries, 4, "activeOnly=false returns removals and deletes")

	entries, err = qh.getChangesInChannelFromQuery(base.TestCtx(t), channel, 1, 0, 0, true)
	require.NoError(t, err)
	require.Len(t, entries, 2, "activeOnly=true drops the removal and the delete")
	assert.Equal(t, uint64(1), entries[0].Sequence)
	assert.Equal(t, uint64(4), entries[1].Sequence)

	// activeOnly combines with the bounds rather than replacing them.
	entries, err = qh.getChangesInChannelFromQuery(base.TestCtx(t), channel, 2, 0, 0, true)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint64(4), entries[0].Sequence)
}

// TestQueryHandlerForTestQueryCount pins the query counter, which the cache tests assert against
// to prove a channel was initialised with exactly one query.
func TestQueryHandlerForTestQueryCount(t *testing.T) {
	qh := &QueryHandlerForTest{}
	qh.SeedEntries(LogEntries{MakeTestLogEntryForChannels(1, []string{"chanA"})})
	assert.Equal(t, 0, qh.QueryCount())

	for i := 1; i <= 3; i++ {
		_, err := qh.getChangesInChannelFromQuery(base.TestCtx(t), "chanA", 1, 0, 0, false)
		require.NoError(t, err)
		assert.Equal(t, i, qh.QueryCount())
	}

	// A query that matches nothing still counts - the cache pays for it either way.
	_, err := qh.getChangesInChannelFromQuery(base.TestCtx(t), "nosuchchannel", 1, 0, 0, false)
	require.NoError(t, err)
	assert.Equal(t, 4, qh.QueryCount())
}

// TestQueryHandlerForTestAsFactory covers the two factory shapes: a shared handler, and a fresh
// empty handler per collection.
func TestQueryHandlerForTestAsFactory(t *testing.T) {
	shared := &QueryHandlerForTest{}
	shared.SeedEntries(LogEntries{MakeTestLogEntryForChannels(1, []string{"chanA"})})

	first, err := shared.AsFactory(base.DefaultCollectionID)
	require.NoError(t, err)
	second, err := shared.AsFactory(base.DefaultCollectionID + 1)
	require.NoError(t, err)
	assert.Same(t, first, second, "AsFactory shares one handler across collections")

	fresh, err := QueryHandlerFactoryForTest(base.DefaultCollectionID)
	require.NoError(t, err)
	require.NotNil(t, fresh)
	entries, err := fresh.getChangesInChannelFromQuery(base.TestCtx(t), "chanA", 1, 0, 0, false)
	require.NoError(t, err)
	assert.Len(t, entries, 0, "QueryHandlerFactoryForTest returns an unseeded handler")

	var _ ChannelQueryHandler = shared
}
