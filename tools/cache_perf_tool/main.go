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
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/rosmar"
	"github.com/felixge/fgprof"
)

var numGoroutines atomic.Int32

func main() {
	mode := flag.String("mode", "processEntry", "Mode for the tool to run in, either dcp or processEntry.")
	nodes := flag.Int("sgwNodes", 1, "Number of sgw nodes to abstract.")
	batchSize := flag.Int("batchSize", 10, "Batch size for the sequence allocator.")
	timeToRun := flag.Duration("duration", 5*time.Minute, "Duration to run the test for in minutes. Examples:  3m for 3 minutes, 30s for 30 seconds etc")
	delays := flag.String("writeDelay", "0", "Delay between writes in milliseconds. Must be entered in format <delayMS>,<delayMS>,<delayMS>.")
	profileInterval := flag.Duration("profileInterval", 30*time.Second, "Interval for profiling to be triggered on, example 10s would be every 10 seconds.")
	numChannels := flag.Int("numChannels", 1, "Number of channels to create per document.")
	flag.Parse()

	if *nodes < 1 {
		log.Fatalf("Invalid number of nodes: %d", *nodes)
	}
	if *batchSize < 1 || *batchSize > 10 {
		log.Fatalf("Invalid batch size: %d", *batchSize)
	}
	if *timeToRun < 1 {
		log.Fatalf("Invalid duration: %d", *timeToRun)
	}
	if *numChannels < 1 {
		log.Fatalf("Invalid number of channels: %d", *numChannels)
	}
	var delayList []time.Duration
	delayList, err := extractDelays(*delays)
	if err != nil {
		return
	}
	// need to have a delay for each node defined so we have variable write throughput
	if len(delayList) != *nodes {
		log.Printf("invalid number of delays, number of input delays should match number of nodes: "+
			"Delays=%d and number of nodes=%d", len(delayList), *nodes)
		return
	}

	parentCtx := context.Background()
	ctx, cancelFunc := context.WithCancel(parentCtx)

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
	dbContext, err := db.NewDatabaseContext(ctx, "db", walrusBucket, false, db.DatabaseContextOptions{
		Scopes: map[string]db.ScopeOptions{
			base.DefaultScope: {
				Collections: map[string]db.CollectionOptions{
					base.DefaultCollection: {},
				},
			},
		},
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

	// mode selection logic
	if *mode == "dcp" {
		// todo: add dcp mode code
	} else if *mode == "processEntry" {
		p := &processEntryGen{t: t, dbCtx: dbContext, delays: delayList, seqAlloc: seqAllocator, numNodes: *nodes,
			batchSize: *batchSize, numChans: *numChannels}
		// create new sgw node abstraction and spawn write goroutines
		p.spawnDocCreationGoroutine(ctx)
	} else {
		log.Printf("Invalid mode: %s", *mode)
		return
	}
	defer printJavaPropertiesFile(ctx, dbContext)

	// duration of test logic
	ticker := time.NewTicker(*timeToRun)
	defer ticker.Stop()

outerloop:
	for {
		select {
		case <-ticker.C:
			cancelFunc()
			break outerloop
		}
	}

	workerFunc := func() (shouldRetry bool, err error, val interface{}) {
		return numGoroutines.Load() != int32(0), nil, val
	}
	err, _ = base.RetryLoop(parentCtx, "wait for writing goroutines to stop", workerFunc, base.CreateSleeperFunc(500, 100))
	if err != nil {
		log.Printf("Error waiting for stat value (%d) to reach 0: %v", numGoroutines.Load(), err)
	}

}

func printJavaPropertiesFile(ctx context.Context, dbContext *db.DatabaseContext) {
	// Print the Java properties file to stdout
	dbContext.UpdateCalculatedStats(ctx)
	dbStats := dbContext.DbStats
	// calculate here avg time to cache seq in ms
	count := dbStats.Database().DCPCachingCount.Value()
	timeNano := dbStats.Database().DCPCachingTime.Value()
	avgTimeNano := float64(timeNano) / float64(count)
	avgTimeMs := avgTimeNano / 1e6
	timeMS := timeNano / 1e6
	_, _ = fmt.Fprintf(os.Stdout, "high_seq_feed=%d\n", dbStats.Database().HighSeqFeed.Value())
	_, _ = fmt.Fprintf(os.Stdout, "pending_seq_len=%d\n", dbStats.Cache().PendingSeqLen.Value())
	_, _ = fmt.Fprintf(os.Stdout, "high_seq_stable=%d\n", dbStats.Cache().HighSeqStable.Value())
	_, _ = fmt.Fprintf(os.Stdout, "current_skipped_seq_count=%d\n", dbStats.Cache().NumCurrentSeqsSkipped.Value())
	_, _ = fmt.Fprintf(os.Stdout, "num_skipped_seqs=%d\n", dbStats.Cache().NumSkippedSeqs.Value())
	_, _ = fmt.Fprintf(os.Stdout, "skipped_seq_len=%d\n", dbStats.Cache().SkippedSeqLen.Value())
	_, _ = fmt.Fprintf(os.Stdout, "skipped_seq_cap=%d\n", dbStats.Cache().SkippedSeqCap.Value())
	_, _ = fmt.Fprintf(os.Stdout, "dcp_caching_count=%d\n", count)
	_, _ = fmt.Fprintf(os.Stdout, "dcp_caching_time=%d\n", timeMS)
	_, _ = fmt.Fprintf(os.Stdout, "avg_time_per_seq_ms=%f\n", avgTimeMs)
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
	_, _ = fmt.Fprintf(os.Stderr, "skipped_seq_len,")
	_, _ = fmt.Fprintf(os.Stderr, "skipped_seq_cap,")
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
			avgTimeNano := float64(timeNano) / float64(count)
			avgTimeMs := avgTimeNano / 1e6
			timeMS := timeNano / 1e6
			_, _ = fmt.Fprintf(os.Stderr, "%d,", time.Now().Unix())
			_, _ = fmt.Fprintf(os.Stderr, "%d,", dbStats.Database().HighSeqFeed.Value())
			_, _ = fmt.Fprintf(os.Stderr, "%d,", dbStats.Cache().PendingSeqLen.Value())
			_, _ = fmt.Fprintf(os.Stderr, "%d,", dbStats.Cache().HighSeqStable.Value())
			_, _ = fmt.Fprintf(os.Stderr, "%d,", dbStats.Cache().NumCurrentSeqsSkipped.Value())
			_, _ = fmt.Fprintf(os.Stderr, "%d,", dbStats.Cache().NumSkippedSeqs.Value())
			_, _ = fmt.Fprintf(os.Stderr, "%d,", dbStats.Cache().SkippedSeqLen.Value())
			_, _ = fmt.Fprintf(os.Stderr, "%d,", dbStats.Cache().SkippedSeqCap.Value())
			_, _ = fmt.Fprintf(os.Stderr, "%d,", count)
			_, _ = fmt.Fprintf(os.Stderr, "%d,", timeMS)
			_, _ = fmt.Fprintf(os.Stderr, "%f", avgTimeMs)
			_, _ = fmt.Fprintf(os.Stderr, "\n")
		}
	}
}

func extractDelays(delayStr string) ([]time.Duration, error) {
	var delays []time.Duration
	if delayStr == "" {
		return delays, nil
	}
	delayList := strings.Split(delayStr, ",")
	for _, delay := range delayList {
		delayInt, err := strconv.Atoi(delay)
		if err != nil {
			log.Printf("Error parsing delay: %v", err)
			return nil, err
		}
		if delayInt > 150 || delayInt < 0 {
			log.Printf("Invalid delay: %d, you can have a max delay of 150ms and minimum delay of 0ms", delayInt)
			return nil, err
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
