//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	trace2 "go.opentelemetry.io/otel/trace"
)

// endedSpanNames returns the names of all completed spans, deduplicated.
func endedSpanNames(recorder *tracetest.SpanRecorder) []string {
	var names []string
	for _, s := range recorder.Ended() {
		if !slices.Contains(names, s.Name()) {
			names = append(names, s.Name())
		}
	}
	slices.Sort(names)
	return names
}

// findSpan returns the first completed span with the given name.
func findSpan(t *testing.T, recorder *tracetest.SpanRecorder, name string) trace.ReadOnlySpan {
	t.Helper()
	for _, s := range recorder.Ended() {
		if s.Name() == name {
			return s
		}
	}
	require.Fail(t, fmt.Sprintf("no span named %q found, have: %v", name, endedSpanNames(recorder)))
	return nil
}

// TestTraceDatabaseStartup covers deliverable A: bringing a database online produces a single
// sgw.db.online trace with the startup phases as descendants.
func TestTraceDatabaseStartup(t *testing.T) {
	recorder := base.InstallTestTracerProvider(t)

	rt := NewRestTester(t, nil)
	defer rt.Close()
	rt.WaitForDBOnline()

	names := endedSpanNames(recorder)
	for _, expected := range []string{
		"sgw.db.online",
		"sgw.bucket.connect",
		"sgw.db.new_context",
		"sgw.db.start_online_processes",
		"sgw.change_cache.init",
		"sgw.mutation_listener.start",
		"sgw.change_cache.start",
	} {
		assert.Contains(t, names, expected)
	}

	root := findSpan(t, recorder, "sgw.db.online")
	assert.False(t, root.Parent().IsValid(), "sgw.db.online should be the trace root")

	// every startup phase belongs to the same trace
	onlineProcesses := findSpan(t, recorder, "sgw.db.start_online_processes")
	assert.Equal(t, root.SpanContext().TraceID(), onlineProcesses.SpanContext().TraceID())
	changeCacheInit := findSpan(t, recorder, "sgw.change_cache.init")
	assert.Equal(t, onlineProcesses.SpanContext().SpanID(), changeCacheInit.Parent().SpanID())
}

// TestTraceBlipRevMessage covers deliverable B: a pushed rev produces one trace rooted at the BLIP
// message, with the sync function as a descendant.
func TestTraceBlipRevMessage(t *testing.T) {
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &RestTesterConfig{
			GuestEnabled: true,
			SyncFn:       `function(doc) { channel(doc.channels); }`,
		})
		defer rt.Close()

		// install the recorder only once the database is up, so we capture rev traffic alone
		recorder := base.InstallTestTracerProvider(t)

		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer btc.Close()
		btcRunner.StartPush(btc.id)

		version := btcRunner.AddRev(btc.id, "doc1", nil, []byte(`{"channels":["a"]}`))
		rt.WaitForVersion("doc1", version)

		names := endedSpanNames(recorder)
		assert.Contains(t, names, "blip.rev")
		assert.Contains(t, names, "sgw.sync_fn")

		rev := findSpan(t, recorder, "blip.rev")
		assert.False(t, rev.Parent().IsValid(), "blip.rev should be the trace root")

		syncFn := findSpan(t, recorder, "sgw.sync_fn")
		assert.Equal(t, rev.SpanContext().TraceID(), syncFn.SpanContext().TraceID(),
			"sync function should be part of the rev's trace")
	})
}

// TestSpanVolumePerRev records how many spans a pushed rev produces. Span volume, not CPU, is what
// decides whether always-on replication tracing is affordable: spans per rev x revs/sec/node is the
// rate a collector has to absorb.
//
// Two figures are reported, because they answer different questions:
//   - spans per rev trace: spans belonging to a blip.rev trace. The cost of tracing one rev.
//   - total spans per rev: everything Sync Gateway emitted in the same window, including
//     background KV traffic from the caching feed, sequence allocation and checkpointing. What the
//     collector actually receives.
func TestSpanVolumePerRev(t *testing.T) {
	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		GuestEnabled: true,
		syncFn:       `function(doc) { channel(doc.channels); }`,
	})
	defer bt.Close()

	// Warm up outside the recorder so one-off connection setup isn't counted. gocb connection churn
	// dominates at low rev counts and badly inflates the per-rev figure.
	for i := range 20 {
		bt.SendRev(fmt.Sprintf("warmup-%d", i), "1-abc", []byte(`{"channels":["a"]}`), blip.Properties{})
	}

	recorder := base.InstallTestTracerProvider(t)

	const revCount = 100
	for i := range revCount {
		bt.SendRev(fmt.Sprintf("doc-%d", i), "1-abc", []byte(`{"channels":["a"]}`), blip.Properties{})
	}

	// identify the traces rooted at a blip.rev span
	revTraceIDs := make(map[trace2.TraceID]struct{})
	for _, s := range recorder.Ended() {
		if s.Name() == "blip.rev" && !s.Parent().IsValid() {
			revTraceIDs[s.SpanContext().TraceID()] = struct{}{}
		}
	}
	require.Len(t, revTraceIDs, revCount, "expected exactly one blip.rev root span per rev")

	inRevTraces, background := 0, 0
	perName := map[string]int{}
	for _, s := range recorder.Ended() {
		if _, ok := revTraceIDs[s.SpanContext().TraceID()]; ok {
			inRevTraces++
			perName[s.Name()]++
		} else {
			background++
		}
	}
	total := len(recorder.Ended())

	t.Logf("spans per rev trace:  %.1f  (%d spans across %d rev traces)",
		float64(inRevTraces)/revCount, inRevTraces, revCount)
	t.Logf("total spans per rev:  %.1f  (%d spans, of which %d were background, not part of a rev trace)",
		float64(total)/revCount, total, background)
	t.Logf("breakdown within rev traces:")
	for _, name := range slices.Sorted(maps.Keys(perName)) {
		t.Logf("  %-32s %5d (%.2f/rev)", name, perName[name], float64(perName[name])/revCount)
	}

	assert.Equal(t, revCount, perName["blip.rev"])
	assert.Equal(t, revCount, perName["sgw.sync_fn"], "each rev runs the sync function once")
}
