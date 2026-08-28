// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main // main.go

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/rosmar"
	"github.com/felixge/fgprof"
)

var numGoroutines atomic.Int32

const (
	processEntry = "processEntry"
	dcp          = "dcp"
)

func main() {
	mode := flag.String("mode", processEntry, "Mode for the tool to run in, either dcp or processEntry.")
	nodes := flag.Int("sgwNodes", 1, "Number of sgw nodes to abstract. NOTE only relevant for processEntry mode.")
	batchSize := flag.Int("batchSize", 10, "Batch size for the sequence allocator.")
	timeToRun := flag.Duration("duration", 5*time.Minute, "Duration to run the test for in minutes. Examples:  3m for 3 minutes, 30s for 30 seconds etc")
	delays := flag.String("writeDelay", "0", "Delay between writes in milliseconds. Must be entered in format <delayMS>,<delayMS>,<delayMS>.")
	profileInterval := flag.Duration("profileInterval", 0*time.Second, "Interval for profiling to be triggered on, example 10s would be every 10 seconds.")
	numChannelsPerDoc := flag.Int("numChannels", 1, "Number of channels to create per document.")
	totalNumberOfChans := flag.Int("totalNumberOfChans", 1, "Total number of channels to create in the system.")
	numOfChangesFeeds := flag.Int("numChangesFeeds", 0, "Number of changes feeds to create. Used only for DCP mode.")
	channelsPerClient := flag.Int("channelsPerClient", 0, "Number of channels per client to wait on. Used only for DCP mode.")
	rapidUpdateDocs := flag.Bool("rapidUpdateDocs", false, "Have documents rapidly updated (use of recent sequences). Used only for DCP mode.")
	numDCPWorkers := flag.Int("numDCPWorkers", 8, "Number of DCP workers to create. Default is 8. Used only for DCP mode.")
	numVBuckets := flag.Int("numVBuckets", 1024, "Number of vBuckets the DCP client is sized for (worker routing and metadata). Used only for DCP mode. NOTE this does not change the generator, which always starts 1024 vBucket writer goroutines - lowering it does not lower the offered load. Default is 1024.")
	flag.Parse()

	if *nodes < 1 {
		log.Fatalf("Invalid number of nodes: %d", *nodes)
	}
	if *numOfChangesFeeds < 0 {
		log.Fatalf("Invalid number of changes feeds: %d", *numOfChangesFeeds)
	}
	if *batchSize < 1 || *batchSize > 10 {
		log.Fatalf("Invalid batch size: %d", *batchSize)
	}
	if *timeToRun < 1 {
		log.Fatalf("Invalid duration: %d", *timeToRun)
	}
	if *numChannelsPerDoc < 0 {
		log.Fatalf("Invalid number of channels: %d", *numChannelsPerDoc)
	}
	if profileInterval.Seconds() != 0 && *profileInterval >= *timeToRun {
		log.Fatalf("Invalid profile interval: %d, must be less than the duration of test: %d", *profileInterval, *timeToRun)
	}
	// 0 is allowed for both channel flags: it gives a no-channel-cache baseline (documents cached with
	// no channel population to write into), which isolates the rest of the write path. The check that
	// matters is that the system has at least as many channels as each document is assigned to,
	// otherwise the generator wraps and silently assigns fewer channels per document than requested.
	if *totalNumberOfChans < 0 || (*numChannelsPerDoc > 0 && *totalNumberOfChans < *numChannelsPerDoc) {
		log.Fatalf("Invalid total number of channels: %d (must be >= 0, and >= numChannels=%d)", *totalNumberOfChans, *numChannelsPerDoc)
	}
	if *channelsPerClient < 0 {
		log.Fatalf("Invalid number of channels per client: %d", *channelsPerClient)
	}
	if *numDCPWorkers < 1 {
		log.Fatalf("Invalid number of DCP workers: %d", *numDCPWorkers)
	}

	delayList, err := extractDelays(*delays, *mode)
	if err != nil {
		return
	}
	// need to have a delay for each node defined so we have variable write throughput
	if len(delayList) != *nodes && *mode == processEntry {
		log.Printf("invalid number of delays, number of input delays should match number of nodes: "+
			"Delays=%d and number of nodes=%d", len(delayList), *nodes)
		return
	}

	parentCtx := context.Background()
	ctx, cancelFunc := context.WithCancelCause(parentCtx)

	// Need a bucket type for creating the database context
	var walrusBucket *rosmar.Bucket
	bucketName := "cacheTest" + "rosmar_"
	url := rosmar.InMemoryURL
	walrusBucket, err = rosmar.OpenBucket(url, bucketName, rosmar.CreateOrOpen)
	if err != nil {
		log.Fatalf("Error opening walrus bucket: %v", err)
	}
	defer walrusBucket.Close(parentCtx)

	if profileInterval.Seconds() > 1 {
		// start CPU profiling here
		cpuProfBuf := bytes.Buffer{}
		err = pprof.StartCPUProfile(&cpuProfBuf)
		if err != nil {
			log.Printf("Error starting CPU profile: %v", err)
			return
		}
		defer func() {
			pprof.StopCPUProfile()
			cpuProfileBuffer, err := io.ReadAll(&cpuProfBuf)
			if err != nil {
				log.Printf("error reading cpuProfBuf: %v", err)
				return
			}
			err = os.WriteFile("cpu.prof", cpuProfileBuffer, os.ModePerm)
			if err != nil {
				log.Printf("error writing cpu profile to file: %v", err)
				return
			}
		}()
		fileName := fmt.Sprintf("fprof-%s.prof", time.Now().Format(time.RFC3339))
		fProfBuf := bytes.Buffer{}
		stopFn := fgprof.Start(&fProfBuf, fgprof.FormatPprof)
		defer func() {
			err := stopFn()
			if err != nil {
				log.Printf("Error stopping fprof profile: %v", err)
				return
			}
			err = os.WriteFile(fileName, fProfBuf.Bytes(), os.ModePerm)
			if err != nil {
				log.Printf("Error writing fprof profile to file: %v", err)
				return
			}
		}()
		go heapProfiling(ctx, *profileInterval)
		go mutexProfiling(ctx, *profileInterval)
		go blockProfiling(ctx, *profileInterval)
		go goroutineProfiling(ctx, *profileInterval)
	}

	// new syncSeqMock to be used for the sequence allocator
	seqAllocator := newSyncSeq()
	_ = seqAllocator.nextBatch(1) // init atomic on syncSeqMock to 1

	var t *testing.T
	cacheOpts := db.DefaultCacheOptions()
	dbContext, err := db.NewDatabaseContext(ctx, "db", walrusBucket, false, db.DatabaseContextOptions{
		Scopes: map[string]db.ScopeOptions{
			base.DefaultScope: {
				Collections: map[string]db.CollectionOptions{
					base.DefaultCollection: {},
				},
			},
		},
		CacheOptions: &cacheOpts,
	})
	if err != nil {
		log.Printf("Error creating database context: %v", err)
		return
	}
	defer dbContext.Close(ctx)

	// stats goroutine
	go csvStats(ctx, dbContext)

	// init change cache and unlock mutex for the test
	dbContext.StartChangeCache(t, parentCtx)

	// init channels
	for i := 0; i < *totalNumberOfChans; i++ {
		chanName := "test-" + strconv.Itoa(i)
		err = dbContext.InitChannel(ctx, t, chanName)
		if err != nil {
			log.Printf("Error initializing channel %s: %v", chanName, err)
			return
		}
	}

	// build change waiters (spoofing running changes feeds)
	if *numOfChangesFeeds > 0 && *mode == dcp {
		go startChanges(ctx, t, dbContext, *channelsPerClient, *numOfChangesFeeds, *totalNumberOfChans)
	}

	// mode selection logic
	if *mode == dcp {
		bucket := &base.GocbV2Bucket{}
		// setup dcp generator object and create fake dcp client
		seqAlloc := newSequenceAllocator(*batchSize, seqAllocator)
		dcpGen := &dcpDataGen{seqAlloc: seqAlloc, delays: delayList, dbCtx: dbContext, numChannelsPerDoc: *numChannelsPerDoc,
			numTotalChannels: *totalNumberOfChans, simRapidUpdate: *rapidUpdateDocs}
		mutationListener := dbContext.GetMutationListener(t)
		cacheFeedStatsMap := dbContext.DbStats.Database().CacheFeedMapStats
		client, err := createDCPClient(t, ctx, bucket, mutationListener.ProcessFeedEvent, cacheFeedStatsMap.Map, *numDCPWorkers, uint16(*numVBuckets))
		if err != nil {
			log.Printf("Error creating DCP client: %v", err)
			return
		}
		dcpGen.client = client

		// create vBucket mutations. This blocks until every vBucket writer is running - the writers are
		// staggered by 100ms each, so it takes ~100s for 1024 vBuckets, and only then does the -duration
		// timer below start.
		dcpGen.vBucketCreation(ctx)
		runThroughput.markGeneratorReady()
	} else if *mode == processEntry {
		p := &processEntryGen{t: t, dbCtx: dbContext, delays: delayList, seqAlloc: seqAllocator, numNodes: *nodes,
			batchSize: *batchSize, numChansPerDoc: *numChannelsPerDoc, totalChans: *totalNumberOfChans}
		// create new sgw node abstraction and spawn write goroutines
		p.spawnDocCreationGoroutine(ctx)
		runThroughput.markGeneratorReady()
	} else {
		log.Printf("Invalid mode: %s", *mode)
		return
	}
	defer printEndofTestStatsFile(ctx, dbContext)

	// duration of test logic
	time.Sleep(*timeToRun)
	cancelFunc(errors.New("test duration complete"))

	workerFunc := func() (shouldRetry bool, err error, val any) {
		return numGoroutines.Load() != int32(0), nil, val
	}
	err, _ = base.RetryLoop(parentCtx, "wait for writing goroutines to stop", workerFunc, base.CreateSleeperFunc(500, 100))
	if err != nil {
		log.Printf("Error waiting for stat value (%d) to reach 0: %v", numGoroutines.Load(), err)
	}

}

func startChanges(ctx context.Context, t *testing.T, dbContext *db.DatabaseContext, clientChans int, numClients int, totalSystemChannels int) {
	mutationListener := dbContext.GetMutationListener(t)
	chanIDList := make([]channels.ID, 0, clientChans)
	chanCount := 0
	var chanID channels.ID
	for range numClients {
		for range clientChans { // create clientChans number of channels for each change waiter
			if chanCount == totalSystemChannels {
				chanCount = 0 // reset channel count so we don't go over system channels count
			}
			chanID = channels.NewID("test-"+strconv.Itoa(chanCount), base.DefaultCollectionID)
			chanIDList = append(chanIDList, chanID)
			chanCount++
		}
		chans, err := channels.SetOf(chanIDList...)
		if err != nil {
			log.Printf("Error creating channel set: %v", err)
			return
		}
		chanIDList = make([]channels.ID, 0, clientChans) // overwrite the list for next client
		waiter := mutationListener.NewWaiterWithChannels(chans, nil, true)

		go func(ctx context.Context, wait *db.ChangeWaiter, chanMap channels.Set) {
			numGoroutines.Add(1)
			defer numGoroutines.Add(-1)
			// Per-feed cursor: remember the last sequence each watched channel has been read up to, so each
			// wake reads only the delta (like a real changes feed passing `since`), not the whole channel log.
			sinceByChan := make(map[channels.ID]uint64, len(chanMap))
			for {
				if ctx.Err() != nil {
					return
				}
				num := wait.Wait(ctx)
				if num == db.WaiterClosed {
					return
				} else if num == db.WaiterHasChanges {
					// get cached changes for map, simulating changes feeds actually using the channel cache
					for id := range chanMap {
						changes, err := dbContext.GetCachedChangesSince(t, ctx, id, sinceByChan[id])
						if err != nil {
							log.Printf("Error getting cached changes: %v", err)
							return
						}
						if n := len(changes); n > 0 {
							sinceByChan[id] = changes[n-1].Sequence
						}
					}
				}
			}
		}(ctx, waiter, chans)
	}
}

// steadyWindowSecs is the trailing window the steady-state throughput figure is taken over. 300s is
// long enough to average out the notify cadence and short enough to exclude the ~100s vBucket ramp,
// during which throughput is still climbing and would drag the whole-run average down.
const steadyWindowSecs = 300

// throughputSample is one reading of the cumulative documents-cached counter, taken at the same
// moment as (and carrying the same values as) a row of the per-second stderr CSV.
type throughputSample struct {
	unixSec int64
	count   int64
}

// runThroughput accumulates the per-second series csvStats already reads, so the end-of-run summary
// can state documents cached per second directly instead of every consumer re-deriving it from the
// CSV. One sample per second, so the memory cost is negligible even for a multi-hour run.
var runThroughput throughputSeries

type throughputSeries struct {
	lock sync.Mutex
	// samples is the per-second series, oldest first.
	samples []throughputSample
	// readyUnixSec is when the write generator reached full offered load; samples before it are
	// excluded from the steady-state window. In DCP mode the 1024 vBucket writers are started 100ms
	// apart, so the run spends its first ~100s ramping - and that ramp happens BEFORE the -duration
	// timer starts, which is exactly the trap this guards: without it, a run given a -duration shorter
	// than the steady window would report a steady figure that silently averages in the ramp.
	readyUnixSec int64
}

// markGeneratorReady records that every writer is now running, so the steady-state window can start
// from here rather than from the first sample.
func (s *throughputSeries) markGeneratorReady() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.readyUnixSec = time.Now().Unix()
}

func (s *throughputSeries) record(unixSec int64, count int64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.samples = append(s.samples, throughputSample{unixSec: unixSec, count: count})
}

// rates returns documents cached per second over the whole sampled run, and over the steady-state
// window: the last windowSecs, never reaching back past the point the generator came up to full
// load. window is the length of that steady window as actually measured - shorter than windowSecs
// when the run did not last that long after ramp, and 0 when the run was too short to give a steady
// window at all, in which case steady is 0 too rather than a ramp-contaminated number.
func (s *throughputSeries) rates(windowSecs int64) (overall, steady float64, window int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	n := len(s.samples)
	if n < 2 {
		return 0, 0, 0
	}
	first, last := s.samples[0], s.samples[n-1]
	if last.unixSec > first.unixSec {
		overall = float64(last.count-first.count) / float64(last.unixSec-first.unixSec)
	}

	cutoff := last.unixSec - windowSecs
	if s.readyUnixSec > cutoff {
		cutoff = s.readyUnixSec
	}
	// Walk back to the earliest sample still inside the window.
	var start throughputSample
	found := false
	for i := n - 1; i >= 0 && s.samples[i].unixSec >= cutoff; i-- {
		start = s.samples[i]
		found = true
	}
	if found && last.unixSec > start.unixSec {
		window = last.unixSec - start.unixSec
		steady = float64(last.count-start.count) / float64(window)
	}
	return overall, steady, window
}

// avgCachingTimeMs is the running mean time to cache one sequence, in ms. Guarded so a run that has
// not cached anything yet reports 0 rather than NaN, which would otherwise land in the CSV.
func avgCachingTimeMs(timeNano, count int64) float64 {
	if count <= 0 {
		return 0
	}
	return (float64(timeNano) / float64(count)) / 1e6
}

func printEndofTestStatsFile(ctx context.Context, dbContext *db.DatabaseContext) {
	// Print the csv file to stdout
	dbContext.UpdateCalculatedStats(ctx)
	dbStats := dbContext.DbStats
	// calculate here avg time to cache seq in ms
	count := dbStats.Database().DCPCachingCount.Value()
	timeNano := dbStats.Database().DCPCachingTime.Value()
	avgTimeMs := avgCachingTimeMs(timeNano, count)
	timeMS := timeNano / 1e6

	_, _ = fmt.Fprintf(os.Stdout, "timestamp,")
	_, _ = fmt.Fprintf(os.Stdout, "high_seq_feed,")
	_, _ = fmt.Fprintf(os.Stdout, "pending_seq_len,")
	_, _ = fmt.Fprintf(os.Stdout, "high_seq_stable,")
	_, _ = fmt.Fprintf(os.Stdout, "current_skipped_seq_count,")
	_, _ = fmt.Fprintf(os.Stdout, "num_skipped_seqs,")
	_, _ = fmt.Fprintf(os.Stdout, "skipped_sequence_skip_list_nodes,")
	_, _ = fmt.Fprintf(os.Stdout, "dcp_caching_count,")
	_, _ = fmt.Fprintf(os.Stdout, "dcp_caching_time,")
	_, _ = fmt.Fprintf(os.Stdout, "avg_time_per_seq_ms")
	_, _ = fmt.Fprintf(os.Stdout, "\n")

	// print end of run stats
	_, _ = fmt.Fprintf(os.Stdout, "%d,", time.Now().Unix())
	_, _ = fmt.Fprintf(os.Stdout, "%d,", dbStats.Database().HighSeqFeed.Value())
	_, _ = fmt.Fprintf(os.Stdout, "%d,", dbStats.Cache().PendingSeqLen.Value())
	_, _ = fmt.Fprintf(os.Stdout, "%d,", dbStats.Cache().HighSeqStable.Value())
	_, _ = fmt.Fprintf(os.Stdout, "%d,", dbStats.Cache().NumCurrentSeqsSkipped.Value())
	_, _ = fmt.Fprintf(os.Stdout, "%d,", dbStats.Cache().NumSkippedSeqs.Value())
	_, _ = fmt.Fprintf(os.Stdout, "%d,", dbStats.Cache().SkippedSequenceSkiplistNodes.Value())
	_, _ = fmt.Fprintf(os.Stdout, "%d,", count)
	_, _ = fmt.Fprintf(os.Stdout, "%d,", timeMS)
	_, _ = fmt.Fprintf(os.Stdout, "%f", avgTimeMs)
	_, _ = fmt.Fprintf(os.Stdout, "\n")

	// Documents cached per second, on their own labelled lines so this can be parsed straight out of
	// the summary rather than re-derived from the per-second CSV. Both rates are computed from that
	// same series, so they agree with it exactly.
	//
	//   - _overall covers every sample of the run, so it INCLUDES the ~100s vBucket ramp during which
	//     throughput is still climbing. Use it only for whole-run accounting.
	//   - _steady covers the last steadyWindowSecs of post-ramp run and is the figure to compare
	//     between runs.
	//   - _steady_window_secs is the window that was actually measured. It is less than
	//     steadyWindowSecs when the run did not last that long after ramp; if it is 0 the run was too
	//     short to give a steady window at all and _steady is 0 rather than a ramp-blended number.
	overallRate, steadyRate, steadyWindow := runThroughput.rates(steadyWindowSecs)
	_, _ = fmt.Fprintf(os.Stdout, "docs_cached_per_sec_overall,%f\n", overallRate)
	_, _ = fmt.Fprintf(os.Stdout, "docs_cached_per_sec_steady,%f\n", steadyRate)
	_, _ = fmt.Fprintf(os.Stdout, "docs_cached_per_sec_steady_window_secs,%d\n", steadyWindow)

	// B5 amplification: DCPReceivedCount = one per DCP event (DocChanged); DCPCachingCount = one per
	// cached sequence (_addToCache). Their ratio is the RecentSequences/UnusedSequences fan-out, i.e.
	// how many processEntry calls each DCP event costs. Labelled lines again, so the positional
	// per-second CSV recipe is undisturbed.
	received := dbStats.Database().DCPReceivedCount.Value()
	var seqsPerEvent float64
	if received > 0 {
		seqsPerEvent = float64(count) / float64(received)
	}
	_, _ = fmt.Fprintf(os.Stdout, "dcp_received_count,%d\n", received)
	_, _ = fmt.Fprintf(os.Stdout, "seqs_cached_per_event,%f\n", seqsPerEvent)
}

func csvStats(ctx context.Context, dbContext *db.DatabaseContext) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	numGoroutines.Add(1)
	defer numGoroutines.Add(-1)
	_, _ = fmt.Fprintf(os.Stderr, "timestamp,")
	_, _ = fmt.Fprintf(os.Stderr, "high_seq_feed,")
	_, _ = fmt.Fprintf(os.Stderr, "pending_seq_len,")
	_, _ = fmt.Fprintf(os.Stderr, "high_seq_stable,")
	_, _ = fmt.Fprintf(os.Stderr, "current_skipped_seq_count,")
	_, _ = fmt.Fprintf(os.Stderr, "num_skipped_seqs,")
	_, _ = fmt.Fprintf(os.Stderr, "skipped_sequence_skip_list_nodes,")
	_, _ = fmt.Fprintf(os.Stderr, "dcp_caching_count,")
	_, _ = fmt.Fprintf(os.Stderr, "dcp_caching_time,")
	_, _ = fmt.Fprintf(os.Stderr, "avg_time_per_seq_ms")
	_, _ = fmt.Fprintf(os.Stderr, "\n")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dbContext.UpdateCalculatedStats(ctx)
			dbStats := dbContext.DbStats
			// calculate here avg time to cache seq in ms
			count := dbStats.Database().DCPCachingCount.Value()
			timeNano := dbStats.Database().DCPCachingTime.Value()
			avgTimeMs := avgCachingTimeMs(timeNano, count)
			timeMS := timeNano / 1e6
			now := time.Now().Unix()
			// Keep the series for the end-of-run docs-cached-per-second lines, using the same timestamp
			// and counter value printed below so the summary and the CSV cannot disagree.
			runThroughput.record(now, count)
			_, _ = fmt.Fprintf(os.Stderr, "%d,", now)
			_, _ = fmt.Fprintf(os.Stderr, "%d,", dbStats.Database().HighSeqFeed.Value())
			_, _ = fmt.Fprintf(os.Stderr, "%d,", dbStats.Cache().PendingSeqLen.Value())
			_, _ = fmt.Fprintf(os.Stderr, "%d,", dbStats.Cache().HighSeqStable.Value())
			_, _ = fmt.Fprintf(os.Stderr, "%d,", dbStats.Cache().NumCurrentSeqsSkipped.Value())
			_, _ = fmt.Fprintf(os.Stderr, "%d,", dbStats.Cache().NumSkippedSeqs.Value())
			_, _ = fmt.Fprintf(os.Stderr, "%d,", dbStats.Cache().SkippedSequenceSkiplistNodes.Value())
			_, _ = fmt.Fprintf(os.Stderr, "%d,", count)
			_, _ = fmt.Fprintf(os.Stderr, "%d,", timeMS)
			_, _ = fmt.Fprintf(os.Stderr, "%f", avgTimeMs)
			_, _ = fmt.Fprintf(os.Stderr, "\n")
		}
	}
}

func extractDelays(delayStr string, mode string) ([]time.Duration, error) {
	var delays []time.Duration
	if delayStr == "" {
		return delays, nil
	}
	delayList := strings.SplitSeq(delayStr, ",")
	for delay := range delayList {
		delayInt, err := strconv.Atoi(delay)
		if err != nil {
			log.Printf("Error parsing delay: %v", err)
			return nil, err
		}
		if delayInt < 0 {
			log.Printf("Invalid delay: %d, you can have a minimum delay of 0ms", delayInt)
			return nil, fmt.Errorf("invalid delay")
		}
		if mode == processEntry {
			if delayInt > 150 {
				log.Printf("Invalid delay: %d, you can have a max delay of 150ms and minimum delay of 0ms", delayInt)
				return nil, fmt.Errorf("invalid delay")
			}
		}
		delays = append(delays, time.Duration(delayInt)*time.Millisecond)
	}
	return delays, nil
}

func heapProfiling(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	numGoroutines.Add(1)
	defer numGoroutines.Add(-1)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fileName := fmt.Sprintf("heap-%s.prof", time.Now().Format(time.RFC3339))
			heapProfBuf := bytes.Buffer{}
			err := pprof.WriteHeapProfile(&heapProfBuf)
			if err != nil {
				log.Printf("Error writing heap profile: %v", err)
				return
			}

			err = os.WriteFile(fileName, heapProfBuf.Bytes(), os.ModePerm)
			if err != nil {
				log.Printf("Error writing heap profile to file: %v", err)
				return
			}
		}
	}
}

func mutexProfiling(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	numGoroutines.Add(1)
	defer numGoroutines.Add(-1)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fileName := fmt.Sprintf("mutex-%s.prof", time.Now().Format(time.RFC3339))
			mutexProfBuf := bytes.Buffer{}
			runtime.SetMutexProfileFraction(1)
			time.Sleep(interval)
			err := pprof.Lookup("mutex").WriteTo(&mutexProfBuf, 0)
			if err != nil {
				log.Printf("Error writing mutex profile: %v", err)
				return
			}
			runtime.SetMutexProfileFraction(0)
			err = os.WriteFile(fileName, mutexProfBuf.Bytes(), os.ModePerm)
			if err != nil {
				log.Printf("Error writing mutex profile to file: %v", err)
				return
			}
		}
	}
}

func blockProfiling(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	numGoroutines.Add(1)
	defer numGoroutines.Add(-1)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fileName := fmt.Sprintf("block-%s.prof", time.Now().Format(time.RFC3339))
			blockProfBuf := bytes.Buffer{}
			runtime.SetBlockProfileRate(1)
			time.Sleep(interval)
			err := pprof.Lookup("block").WriteTo(&blockProfBuf, 0)
			if err != nil {
				log.Printf("Error writing block profile: %v", err)
				return
			}
			runtime.SetBlockProfileRate(0)

			err = os.WriteFile(fileName, blockProfBuf.Bytes(), os.ModePerm)
			if err != nil {
				log.Printf("Error writing block profile to file: %v", err)
				return
			}
		}
	}
}

func goroutineProfiling(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	numGoroutines.Add(1)
	defer numGoroutines.Add(-1)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fileName := fmt.Sprintf("goroutine-%s.prof", time.Now().Format(time.RFC3339))
			goroutineProfBuf := bytes.Buffer{}
			err := pprof.Lookup("goroutine").WriteTo(&goroutineProfBuf, 1)
			if err != nil {
				log.Printf("Error writing goroutine profile: %v", err)
				return
			}

			err = os.WriteFile(fileName, goroutineProfBuf.Bytes(), os.ModePerm)
			if err != nil {
				log.Printf("Error writing goroutine profile to file: %v", err)
				return
			}
		}
	}
}
