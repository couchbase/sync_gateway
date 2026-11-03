// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/couchbaselabs/rosmar"
)

func TestEncodeSyncXattrValueMatchesSGBucket(t *testing.T) {
	for _, syncJSON := range []string{``, `{}`, `{"sequence":1}`, string(make([]byte, 70000))} {
		want := sgbucket.EncodeValueWithXattrs(nil, sgbucket.Xattr{Name: base.SyncXattrName, Value: []byte(syncJSON)})
		require.Equal(t, want, encodeSyncXattrValue([]byte(syncJSON)), "len(syncJSON)=%d", len(syncJSON))
	}
}

func TestAppendCasMatchesCasToString(t *testing.T) {
	for _, cas := range []uint64{0, 1, 255, 0x1234567890abcdef, casPlaceholder, math.MaxUint64} {
		require.Equal(t, base.CasToString(cas), string(appendCas(nil, cas)), "cas=%d", cas)
	}
}

func TestCutAtPlaceholders(t *testing.T) {
	x := placeholder{text: []byte("XX"), slot: slotSeq0}
	y := placeholder{text: []byte("YYY"), slot: slotCas}
	tests := []struct {
		data         string
		wantSegments []string
		wantSlots    []slot
	}{
		{data: "", wantSegments: []string{""}},
		{data: "no placeholders", wantSegments: []string{"no placeholders"}},
		{data: "XX", wantSegments: []string{"", ""}, wantSlots: []slot{slotSeq0}},
		{data: "aXXbYYYcXX", wantSegments: []string{"a", "b", "c", ""}, wantSlots: []slot{slotSeq0, slotCas, slotSeq0}},
		{data: "YYYXX", wantSegments: []string{"", "", ""}, wantSlots: []slot{slotCas, slotSeq0}},
	}
	for _, tc := range tests {
		segments, slots := cutAtPlaceholders([]byte(tc.data), []placeholder{x, y})
		gotSegments := make([]string, 0, len(segments))
		for _, s := range segments {
			gotSegments = append(gotSegments, string(s))
		}
		require.Equal(t, tc.wantSegments, gotSegments, "data %q", tc.data)
		require.Equal(t, tc.wantSlots, slots, "data %q", tc.data)
	}
}

// TestSyncTemplateMatchesFileComment keeps the worked example at the top of syncTemplate.go true.
func TestSyncTemplateMatchesFileComment(t *testing.T) {
	dcp := &dcpDataGen{numChannelsPerDoc: 2, numTotalChannels: 3}
	templates, err := newSyncTemplates(dcp)
	require.NoError(t, err)
	tmpl := templates.byNumSeqs[1][0]
	require.Equal(t, []slot{slotSeq0, slotSeq0, slotCas, slotSeq0, slotSeq0, slotTimeSaved}, tmpl.slots)
	require.Len(t, tmpl.segments, len(tmpl.slots)+1)

	// The cycle the comment describes: test-0, then test-2, then test-1, then back to test-0.
	require.Equal(t, 2, templates.normaliseChanCount(templates.byNumSeqs[1][0].nextChanCount))
	require.Equal(t, 1, templates.normaliseChanCount(templates.byNumSeqs[1][2].nextChanCount))
	require.Equal(t, 0, templates.normaliseChanCount(templates.byNumSeqs[1][1].nextChanCount))

	w := pendingWrite{numSeqs: 1, cas: 0x1122334455667788}
	w.seqs[0] = 42
	var filled db.SyncData
	require.NoError(t, base.JSONUnmarshal(tmpl.appendSync(nil, &w, time.Date(2026, 11, 3, 10, 0, 0, 5, time.UTC)), &filled))
	require.Equal(t, uint64(42), filled.Sequence)
	require.Equal(t, "0x8877665544332211", filled.Cas)
	require.Equal(t, []db.ChannelSetEntry{{Name: "test-0", Start: 42}, {Name: "test-1", Start: 42}}, filled.ChannelSet)
}

// templateTestConfigs cover a channel cycle shorter than, equal to and longer than one document's
// channels, no channels at all, and -rapidUpdateDocs.
var templateTestConfigs = []struct {
	chansPerDoc, totalChans int
	rapidUpdate             bool
}{
	{chansPerDoc: 10, totalChans: 20000},
	{chansPerDoc: 10, totalChans: 20000, rapidUpdate: true},
	{chansPerDoc: 1, totalChans: 1},
	{chansPerDoc: 0, totalChans: 0},
	{chansPerDoc: 0, totalChans: 0, rapidUpdate: true},
	{chansPerDoc: 10, totalChans: 10},
	{chansPerDoc: 3, totalChans: 7, rapidUpdate: true},
	{chansPerDoc: 7, totalChans: 1000, rapidUpdate: true},
}

// TestTemplateMatchesMarshal checks the template generator produces the same document as the marshal
// generator for every channel pattern a run reaches, across sequence lengths, CAS values and time
// formats. Bytes are not compared directly because EE's JSON encoder does not sort map keys, so even two
// marshals of the same _sync can differ in channel order; both values are decoded instead, as a SyncData
// (what Sync Gateway reads) and as a generic map (so a field SyncData ignores cannot differ unnoticed).
func TestTemplateMatchesMarshal(t *testing.T) {
	seqs := []uint64{1, 9, 10, 99_999, 1 << 32, 1<<63 + 7, math.MaxUint64}
	cases := []uint64{0, 1, 0x1234567890abcdef, math.MaxUint64}
	pst := time.FixedZone("PST", -8*60*60)
	times := []time.Time{
		time.Now(),
		time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC),      // whole second: no fraction written
		time.Date(2026, 11, 3, 10, 0, 0, 120_000_000, pst), // trailing zeros trimmed, non-UTC offset
		time.Date(2026, 11, 3, 10, 0, 0, 999_999_999, time.UTC),
		{}, // zero time, which FeedLatency treats as "unknown"
	}
	for _, tc := range templateTestConfigs {
		t.Run(fmt.Sprintf("chansPerDoc=%d,totalChans=%d,rapidUpdate=%t", tc.chansPerDoc, tc.totalChans, tc.rapidUpdate), func(t *testing.T) {
			dcp := &dcpDataGen{numChannelsPerDoc: tc.chansPerDoc, numTotalChannels: tc.totalChans, simRapidUpdate: tc.rapidUpdate}
			templates, err := newSyncTemplates(dcp)
			require.NoError(t, err)

			// Two full trips round the channel cycle, so wrap-around is exercised from both generators'
			// own cursors rather than from one shared one.
			writes := 2 * templateCycleLength(tc.chansPerDoc, tc.totalChans)
			var scratch []byte
			templateChan, marshalChan := 0, 0
			for i := range max(writes, 2*len(seqs)) {
				w := pendingWrite{numSeqs: 1, cas: cases[i%len(cases)]}
				if tc.rapidUpdate && i%2 == 0 {
					w.numSeqs = recentSeqsPerRapidUpdate
				}
				for j := range w.numSeqs {
					w.seqs[j] = seqs[(i+j)%len(seqs)]
				}
				timeSaved := times[i%len(times)]

				var templateValue []byte
				templateValue, templateChan, scratch, err = templates.value(scratch, &w, templateChan, timeSaved)
				require.NoError(t, err)
				var marshalValue []byte
				marshalValue, marshalChan, err = dcp.marshalValue(&w, marshalChan, timeSaved)
				require.NoError(t, err)
				require.Equal(t, marshalChan, templateChan, "write %d: channel cursor diverged", i)

				templateBody, templateSync := decodeSyncValue(t, templateValue)
				marshalBody, marshalSync := decodeSyncValue(t, marshalValue)
				require.Empty(t, templateBody, "write %d: the caching feed is xattr-only", i)
				require.Empty(t, marshalBody, "write %d: the caching feed is xattr-only", i)

				var templateSyncData, marshalSyncData db.SyncData
				require.NoError(t, base.JSONUnmarshal(templateSync, &templateSyncData), "write %d: %s", i, templateSync)
				require.NoError(t, base.JSONUnmarshal(marshalSync, &marshalSyncData), "write %d: %s", i, marshalSync)
				require.Equal(t, marshalSyncData, templateSyncData, "write %d", i)

				// history is left out of the generic comparison: a RevTree marshals its revisions in map
				// iteration order, so its arrays differ between any two marshals. The SyncData comparison
				// above has already compared it as a tree.
				var templateMap, marshalMap map[string]any
				require.NoError(t, base.JSONUnmarshal(templateSync, &templateMap))
				require.NoError(t, base.JSONUnmarshal(marshalSync, &marshalMap))
				_, hasHistory := templateMap["history"]
				require.True(t, hasHistory, "write %d: no history in %s", i, templateSync)
				delete(templateMap, "history")
				delete(marshalMap, "history")
				require.Equal(t, marshalMap, templateMap, "write %d", i)

				// The fields the caching feed acts on, checked against the write rather than the other generator.
				require.Equal(t, w.seqs[w.numSeqs-1], templateSyncData.Sequence, "write %d", i)
				require.Equal(t, w.recentSeqs(), templateSyncData.RecentSequences, "write %d", i)
				require.Equal(t, w.cas, templateSyncData.GetSyncCas(), "write %d", i)
				require.True(t, timeSaved.Equal(templateSyncData.TimeSaved), "write %d: time_saved %v, want %v", i, templateSyncData.TimeSaved, timeSaved)
				require.Len(t, templateSyncData.Channels, tc.chansPerDoc, "write %d", i)
			}
		})
	}
}

// TestTemplateValueAllocatesOnlyTheValue guards the point of the template generator: once a writer's
// scratch buffer has grown, a write allocates only the value handed to the feed.
func TestTemplateValueAllocatesOnlyTheValue(t *testing.T) {
	for _, rapidUpdate := range []bool{false, true} {
		dcp := &dcpDataGen{numChannelsPerDoc: 10, numTotalChannels: 20000, simRapidUpdate: rapidUpdate}
		templates, err := newSyncTemplates(dcp)
		require.NoError(t, err)
		w := pendingWrite{numSeqs: 1, cas: 0x1234567890abcdef}
		if rapidUpdate {
			w.numSeqs = recentSeqsPerRapidUpdate
		}
		for j := range w.numSeqs {
			w.seqs[j] = 1_000_000 + uint64(j)
		}
		timeSaved := time.Now()
		var scratch []byte
		chanCount := 0
		_, _, scratch, err = templates.value(scratch, &w, chanCount, timeSaved)
		require.NoError(t, err)
		allocs := testing.AllocsPerRun(100, func() {
			_, chanCount, scratch, _ = templates.value(scratch, &w, chanCount, timeSaved)
		})
		require.Equal(t, 1.0, allocs, "rapidUpdate=%t", rapidUpdate)
	}
}

// TestGeneratedDocsReachChannelCaches is the end-to-end check: documents from each generator go through
// a real DCP client and worker into a running change cache, and every channel cache must then hold
// exactly the documents assigned to it, at their current sequences.
func TestGeneratedDocsReachChannelCaches(t *testing.T) {
	for _, generator := range []string{generatorTemplate, generatorMarshal} {
		for _, rapidUpdate := range []bool{false, true} {
			t.Run(fmt.Sprintf("generator=%s,rapidUpdate=%t", generator, rapidUpdate), func(t *testing.T) {
				requireGeneratedDocsCached(t, generator, rapidUpdate)
			})
		}
	}
}

func requireGeneratedDocsCached(t *testing.T, generator string, rapidUpdate bool) {
	const (
		chansPerDoc = 3
		totalChans  = 7 // wraps mid-document, and 3 and 7 are coprime so every start is reached
		numVBuckets = 4
		numWrites   = 200 // keeps each channel well under the default 500-entry cache length
	)
	ctx := t.Context()

	bucket, err := rosmar.OpenBucket(rosmar.InMemoryURL, "cachePerfTool"+generator+strconv.FormatBool(rapidUpdate), rosmar.CreateOrOpen)
	require.NoError(t, err)
	t.Cleanup(func() { bucket.Close(ctx) })
	cacheOpts := db.DefaultCacheOptions()
	dbContext, err := db.NewDatabaseContext(ctx, "db", bucket, false, db.DatabaseContextOptions{
		Scopes: map[string]db.ScopeOptions{
			base.DefaultScope: {Collections: map[string]db.CollectionOptions{base.DefaultCollection: {}}},
		},
		CacheOptions: &cacheOpts,
	})
	require.NoError(t, err)
	t.Cleanup(func() { dbContext.Close(ctx) })
	dbContext.StartChangeCache(t, ctx)
	for i := range totalChans {
		require.NoError(t, dbContext.InitChannel(ctx, t, "test-"+strconv.Itoa(i)))
	}

	syncSeq := newSyncSeq()
	_ = syncSeq.nextBatch(1) // sequences start at 1, as in main
	dcp := &dcpDataGen{
		seqAlloc:          newSequenceAllocator(10, syncSeq),
		numChannelsPerDoc: chansPerDoc,
		numTotalChannels:  totalChans,
		simRapidUpdate:    rapidUpdate,
		numVBuckets:       numVBuckets,
	}
	if generator == generatorTemplate {
		dcp.templates, err = newSyncTemplates(dcp)
		require.NoError(t, err)
	}
	// One DCP worker, so documents reach the cache in the order they are sent. StartChangeCache never
	// sets an initial sequence, so the cache starts from whichever sequence it sees first; with more
	// workers a later sequence can get there first and the earlier ones are then treated as old.
	client, err := createDCPClient(t, ctx, &base.GocbV2Bucket{}, dbContext.GetMutationListener(t).ProcessFeedEvent,
		dbContext.DbStats.Database().CacheFeedMapStats.Map, 1, numVBuckets)
	require.NoError(t, err)
	// The workers stop only when the client closes, not on ctx cancellation or dbContext.Close.
	t.Cleanup(func() { _ = client.Close() })
	dcp.client = client

	// Written from one goroutine so sequences arrive in order and nothing is held pending; the
	// expected cache contents are worked out independently of the generators.
	type cachedDoc struct {
		docID string
		seq   uint64
	}
	want := make(map[string][]cachedDoc, totalChans)
	dw := &docWriter{dcp: dcp}
	vbSeqs := make([]uint64, numVBuckets)
	var highSeq uint64
	for i := range numWrites {
		vb := i % numVBuckets
		w := dcp.allocateWrite(vbSeqs[vb])
		vbSeqs[vb]++
		firstChan := dw.chanCount % totalChans
		require.NoError(t, dw.send(uint16(vb), vbSeqs[vb], &w))

		currentSeq := w.seqs[w.numSeqs-1]
		highSeq = max(highSeq, currentSeq)
		keySeq := uint64(0)
		if w.numSeqs == 1 {
			keySeq = currentSeq
		}
		docID := fmt.Sprintf("key-%d-%d-%d", vb, vbSeqs[vb], keySeq)
		for c := range chansPerDoc {
			chanName := "test-" + strconv.Itoa((firstChan+c)%totalChans)
			want[chanName] = append(want[chanName], cachedDoc{docID: docID, seq: currentSeq})
		}
	}

	dbContext.ChangeCacheForTest(t).WaitForSequenceForTest(t, highSeq)

	for i := range totalChans {
		chanName := "test-" + strconv.Itoa(i)
		entries, err := dbContext.GetCachedChangesSince(t, ctx, channels.NewID(chanName, base.DefaultCollectionID), 0)
		require.NoError(t, err)
		got := make([]cachedDoc, 0, len(entries))
		for _, e := range entries {
			got = append(got, cachedDoc{docID: e.DocID, seq: e.Sequence})
		}
		require.Equal(t, want[chanName], got, "channel %s", chanName)
	}

	stats := dbContext.DbStats.Database()
	require.Equal(t, int64(numWrites), stats.DCPCachingCount.Value(), "every document is cached exactly once")
	require.Equal(t, int64(numWrites), stats.DCPReceivedCount.Value())
	// time_saved is filled per write after the cache started, so the feed measures a receive latency.
	require.Positive(t, stats.DCPReceivedTime.Value())
	require.Zero(t, dbContext.DbStats.Cache().NumSkippedSeqs.Value(), "in-order writes must not skip sequences")
}

// templateCycleLength is how many distinct channel starts a writer goes through: the number of
// templates newSyncTemplates builds for one sequence count.
func templateCycleLength(chansPerDoc, totalChans int) int {
	if chansPerDoc == 0 || totalChans == 0 {
		return 1
	}
	a, b := totalChans, chansPerDoc
	for b != 0 {
		a, b = b, a%b
	}
	return totalChans / a
}

func decodeSyncValue(t *testing.T, value []byte) (body, syncJSON []byte) {
	t.Helper()
	body, xattrs, err := sgbucket.DecodeValueWithXattrs([]string{base.SyncXattrName}, value)
	require.NoError(t, err)
	return body, xattrs[base.SyncXattrName]
}

func TestTemplateCountMatchesCycle(t *testing.T) {
	for _, tc := range templateTestConfigs {
		dcp := &dcpDataGen{numChannelsPerDoc: tc.chansPerDoc, numTotalChannels: tc.totalChans, simRapidUpdate: tc.rapidUpdate}
		templates, err := newSyncTemplates(dcp)
		require.NoError(t, err)
		numSeqsUsed := []int{1}
		if tc.rapidUpdate {
			numSeqsUsed = append(numSeqsUsed, recentSeqsPerRapidUpdate)
		}
		for numSeqs, byStart := range templates.byNumSeqs {
			if !slices.Contains(numSeqsUsed, numSeqs) {
				require.Nil(t, byStart, "numSeqs=%d", numSeqs)
				continue
			}
			built := 0
			for _, tmpl := range byStart {
				if tmpl != nil {
					built++
				}
			}
			require.Equal(t, templateCycleLength(tc.chansPerDoc, tc.totalChans), built, "%+v numSeqs=%d", tc, numSeqs)
		}
	}
}

func BenchmarkWriteValue(b *testing.B) {
	for _, generator := range []string{generatorTemplate, generatorMarshal} {
		b.Run(generator, func(b *testing.B) {
			dcp := &dcpDataGen{numChannelsPerDoc: 10, numTotalChannels: 20000}
			if generator == generatorTemplate {
				var err error
				dcp.templates, err = newSyncTemplates(dcp)
				require.NoError(b, err)
			}
			w := pendingWrite{numSeqs: 1, cas: 0x1234567890abcdef}
			var scratch []byte
			chanCount := 0
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				w.seqs[0] = uint64(i + 1)
				if dcp.templates != nil {
					_, chanCount, scratch, _ = dcp.templates.value(scratch, &w, chanCount, time.Now())
				} else {
					_, chanCount, _ = dcp.marshalValue(&w, chanCount, time.Now())
				}
			}
		})
	}
}
