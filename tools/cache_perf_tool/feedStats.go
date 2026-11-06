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
	"io"
	"sync"
)

// feedCounters is what the simulated changes feeds saw. The cache-side stats say how fast entries were
// cached, not whether feeds were woken for them, so a change to when notify fires is only visible here:
// wakes minus wakesWithData is the number of wasted wakes, and newestAgeNs is how stale the newest entry
// was by the time a feed read it.
type feedCounters struct {
	wakes         int64 // wakes that reported changes
	wakesWithData int64 // of those, wakes where at least one watched channel had a new entry
	entriesRead   int64
	nonEmptyReads int64 // per-channel reads that returned at least one entry
	newestAgeNs   int64 // summed over nonEmptyReads: age of the newest entry returned, since DCP receipt
}

// feedTotalsAccumulator sums the counters of a group of feeds. Each feed goroutine counts into its own
// feedCounters and merges once on exit: 20,000 feeds adding to shared atomics on every read would add
// contention to the run being measured. main waits for all goroutines to exit before printing, so the
// totals are complete unless that wait times out, which main logs.
type feedTotalsAccumulator struct {
	lock   sync.Mutex
	totals feedCounters
}

var (
	feedTotals     feedTotalsAccumulator // the per-channel feeds (-numChangesFeeds)
	starFeedTotals feedTotalsAccumulator // the "*" feeds (-starChannelFeeds)
)

func (a *feedTotalsAccumulator) merge(f *feedCounters) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.totals.wakes += f.wakes
	a.totals.wakesWithData += f.wakesWithData
	a.totals.entriesRead += f.entriesRead
	a.totals.nonEmptyReads += f.nonEmptyReads
	a.totals.newestAgeNs += f.newestAgeNs
}

// print writes the totals as labelled lines starting with prefix.
func (a *feedTotalsAccumulator) print(w io.Writer, prefix string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	var meanNewestAgeMs float64
	if a.totals.nonEmptyReads > 0 {
		meanNewestAgeMs = float64(a.totals.newestAgeNs) / 1e6 / float64(a.totals.nonEmptyReads)
	}
	_, _ = fmt.Fprintf(w, "%s_wakes,%d\n", prefix, a.totals.wakes)
	_, _ = fmt.Fprintf(w, "%s_wakes_with_data,%d\n", prefix, a.totals.wakesWithData)
	_, _ = fmt.Fprintf(w, "%s_entries_read,%d\n", prefix, a.totals.entriesRead)
	_, _ = fmt.Fprintf(w, "%s_newest_entry_age_ms_per_channel_read,%f\n", prefix, meanNewestAgeMs)
}

// printFeedTotals writes the feed counters as labelled lines, after the end-of-run CSV so positional
// parsing of that CSV is unaffected. They are printed in every mode, as 0 when no feeds ran, so a parser
// always finds them.
func printFeedTotals(w io.Writer) {
	feedTotals.print(w, "feed")
	starFeedTotals.print(w, "star_feed")
}
