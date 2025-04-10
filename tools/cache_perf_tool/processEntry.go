// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"context"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
)

type processEntryGen struct {
	seqAlloc  *syncSeqMock
	delays    []time.Duration
	dbCtx     *db.DatabaseContext
	t         *testing.T
	numNodes  int
	batchSize int
	numChans  int
}

func (p *processEntryGen) spawnDocCreationGoroutine(ctx context.Context) {
	for i := 0; i < p.numNodes; i++ {
		// create new sgw node abstraction
		sgNode := &sgwNode{nodeID: i, seqAlloc: newSequenceAllocator(p.batchSize, p.seqAlloc)}
		// delay list should be same length as num sgw nodes
		go p.nodeWrites(ctx, sgNode, p.delays[i])
	}
}

func (p *processEntryGen) nodeWrites(ctx context.Context, node *sgwNode, delay time.Duration) {
	docCount := uint64(0)
	numGoroutines.Add(1)
	defer numGoroutines.Add(-1)
	log.Printf("node %d has delay of %v ms", node.nodeID, delay.Milliseconds())
	// create map of configured channels
	chanMap := make(channels.ChannelMap)
	for i := 0; i < p.numChans; i++ {
		chanMap["test-"+strconv.Itoa(i)] = nil
	}
	if delay.Nanoseconds() == 0 {
		// mutate as fast as possible
		for {
			timeStamp := time.Now()
			select {
			case <-ctx.Done():
				return
			default:
				sgwSeqno := node.seqAlloc.nextSeq()
				docCount++
				logEntry := &db.LogEntry{
					Sequence:     sgwSeqno,
					DocID:        "key-" + strconv.FormatUint(docCount, 10) + "-" + strconv.FormatUint(sgwSeqno, 10),
					RevID:        "1-abc",
					Flags:        0,
					TimeReceived: timeStamp,
					TimeSaved:    timeStamp,
					Channels:     chanMap,
					CollectionID: 0,
				}
				p.dbCtx.CallProcessEntry(p.t, ctx, logEntry)
			}
		}
	}

	ticker := time.NewTicker(delay)
	defer ticker.Stop()
	for {
		timeStamp := time.Now()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			docCount++
			sgwSeqno := node.seqAlloc.nextSeq()
			logEntry := &db.LogEntry{
				Sequence:     sgwSeqno,
				DocID:        "key-" + strconv.FormatUint(docCount, 10) + "-" + strconv.FormatUint(sgwSeqno, 10),
				RevID:        "1-abc",
				Flags:        0,
				TimeReceived: timeStamp,
				TimeSaved:    timeStamp,
				Channels:     chanMap,
				CollectionID: 0,
			}
			p.dbCtx.CallProcessEntry(p.t, ctx, logEntry)
		}
	}
}
