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
	"runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/rosmar"
)

var numGoroutines atomic.Int32

func main() {
	mode := flag.String("mode", "processEntry", "Mode for the tool to run in, either dcp or processEntry.")
	nodes := flag.Int("sgwNodes", 1, "Number of sgw nodes to abstract.")
	bachSize := flag.Int("batchSize", 10, "Batch size for the sequence allocator.")
	timeToRun := flag.Int("duration", 5, "Duration to run the test for in minutes.")
	dalays := flag.String("writeDelay", "150", "Delay between writes in milliseconds. Must be entered in format <delayMS>,<delayMS>,<delayMS>.")
	profile := flag.Bool("profile", false, "Enable profiling.")
	flag.Parse()

	if *nodes < 1 {
		log.Fatalf("Invalid number of nodes: %d", *nodes)
	}
	if *bachSize < 1 {
		log.Fatalf("Invalid batch size: %d", *bachSize)
	}
	if *timeToRun < 1 {
		log.Fatalf("Invalid duration: %d", *timeToRun)
	}

	parentCtx := context.Background()
	ctx, cancelFunc := context.WithCancel(parentCtx)

	// Need a bucket type for creating the database context
	var walrusBucket *rosmar.Bucket
	bucketName := "cahceTest" + "rosmar_"
	url := rosmar.InMemoryURL
	walrusBucket, err := rosmar.OpenBucket(url, bucketName, rosmar.CreateOrOpen)
	if err != nil {
		log.Fatalf("Error opening walrus bucket: %v", err)
	}
	defer walrusBucket.Close(parentCtx)

	if *profile {
		// start CPU profiling here
		cpuProfBuf := bytes.Buffer{}
		err = pprof.StartCPUProfile(&cpuProfBuf)
		if err != nil {
			log.Fatalf("Error starting CPU profile: %v", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			cpuProfileBuffer, err := io.ReadAll(&cpuProfBuf)
			if err != nil {
				log.Fatalf("error reading cpuProfBuf: %v", err)
			}
			err = os.WriteFile("cpu.prof", cpuProfileBuffer, os.ModePerm)
			if err != nil {
				log.Fatalf("error writing cpu profile to file: %v", err)
			}
		}()
		go heapProfiling(ctx)
		go mutexProfiling(ctx)
		go blockProfiling(ctx)
		go goroutineProfiling(ctx)
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
		log.Fatalf("Error creating database context: %v", err)
	}
	defer dbContext.Close(ctx)

	// init change cache and unlock mutex for the test
	dbContext.StartChangeCache(t, parentCtx)

	// mode selection logic
	if *mode == "dcp" {
		// todo: add dcp mode code
	} else if *mode == "processEntry" {
		var delayList []time.Duration
		if *nodes > 1 { // if we have one node then we don't have a delay (node will write as fast as possible)
			delayList = extractDelays(*dalays)
			// need to have a delay for each node defined so we have variable write throughput
			if len(delayList) != *nodes-1 {
				log.Fatalf("invalid number of delays: %d", len(delayList))
			}
		}
		p := &processEntryGen{t: t, dbCtx: dbContext, delays: delayList, seqAlloc: seqAllocator, numNodes: *nodes, batchSize: *bachSize}
		// create new sgw node abstraction and spawn write goroutines
		p.spawnDocCreationGoroutine(ctx)
	} else {
		log.Fatalf("Invalid mode: %s", *mode)
	}

	defer func() {
		fmt.Println("-------------------------------------")
		fmt.Println("end of test stats")
		dbContext.UpdateCalculatedStats(ctx)
		fmt.Println("high seq of cache", dbContext.DbStats.Database().HighSeqFeed.Value())
		fmt.Println("pending seq length", dbContext.DbStats.Cache().PendingSeqLen.Value())
		fmt.Println("high seq stable", dbContext.DbStats.Cache().HighSeqStable.Value())
		fmt.Println("num current skipped", dbContext.DbStats.Cache().NumCurrentSeqsSkipped.Value())
		fmt.Println("cumulative skipped", dbContext.DbStats.Cache().NumSkippedSeqs.Value())
		fmt.Println("skipped length", dbContext.DbStats.Cache().SkippedSeqLen.Value())
		fmt.Println("skipped capacity", dbContext.DbStats.Cache().SkippedSeqCap.Value())
		fmt.Println("dcp cache count", dbContext.DbStats.Database().DCPCachingCount.Value())
		fmt.Println("dcp cache time", dbContext.DbStats.Database().DCPCachingTime.Value())
	}()

	// duration of test logic
	ticker := time.NewTicker(time.Duration(*timeToRun) * time.Minute)
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
	err, _ = base.RetryLoop(parentCtx, "wait for writing goroutines to stop", workerFunc, base.CreateSleeperFunc(200, 100))
	if err != nil {
		log.Printf("Error waiting for stat value (%d) to reach 0: %v", numGoroutines.Load(), err)
	}

}

func extractDelays(delayStr string) []time.Duration {
	var delays []time.Duration
	if delayStr == "" {
		return delays
	}
	delayList := strings.Split(delayStr, ",")
	for _, delay := range delayList {
		delayInt, err := strconv.Atoi(delay)
		if err != nil {
			log.Fatalf("Error parsing delay: %v", err)
		}
		delays = append(delays, time.Duration(delayInt)*time.Millisecond)
	}
	return delays
}

func heapProfiling(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
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
				log.Fatalf("Error writing heap profile: %v", err)
			}

			err = os.WriteFile(fileName, heapProfBuf.Bytes(), os.ModePerm)
			if err != nil {
				log.Fatalf("Error writing heap profile to file: %v", err)
			}
		}
	}
}

func mutexProfiling(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
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
			err := pprof.Lookup("mutex").WriteTo(&mutexProfBuf, 0)
			if err != nil {
				log.Fatalf("Error writing mutex profile: %v", err)
			}
			err = os.WriteFile(fileName, mutexProfBuf.Bytes(), os.ModePerm)
			if err != nil {
				log.Fatalf("Error writing mutex profile to file: %v", err)
			}
		}
	}
}

func blockProfiling(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
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
			err := pprof.Lookup("block").WriteTo(&blockProfBuf, 0)
			if err != nil {
				log.Fatalf("Error writing block profile: %v", err)
			}

			err = os.WriteFile(fileName, blockProfBuf.Bytes(), os.ModePerm)
			if err != nil {
				log.Fatalf("Error writing block profile to file: %v", err)
			}
		}
	}
}

func goroutineProfiling(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
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
				log.Fatalf("Error writing goroutine profile: %v", err)
			}

			err = os.WriteFile(fileName, goroutineProfBuf.Bytes(), os.ModePerm)
			if err != nil {
				log.Fatalf("Error writing goroutine profile to file: %v", err)
			}
		}
	}
}
