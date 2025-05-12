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
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
)

type processEntryGen struct {
	seqAlloc       *syncSeqMock
	delays         []time.Duration
	dbCtx          *db.DatabaseContext
	t              *testing.T
	numNodes       int
	batchSize      int
	numChansPerDoc int
	totalChans     int
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
	// create map of configured channels, initialised channels are names test-x where x is integer between 0 and total
	// number of system channels. Thus to assign to channels we can increment through channels initialised
	chanCountZeroWait := 0
	chanCountWait := 0
	var chanMap channels.ChannelMap
	if delay.Nanoseconds() == 0 {
		// mutate as fast as possible
		for {
			chanMap = make(channels.ChannelMap, p.numChansPerDoc)
			for range p.numChansPerDoc {
				if chanCountZeroWait == p.totalChans {
					// when count gets to total number configured channels we rest index to 0
					chanCountZeroWait = 0
				}
				chanName := "test-" + strconv.Itoa(chanCountZeroWait)
				chanMap[chanName] = nil
				chanCountZeroWait++
			}
			select {
			case <-ctx.Done():
				return
			default:
				timeStamp := time.Now()
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
		chanMap = make(channels.ChannelMap, p.numChansPerDoc)
		for range p.numChansPerDoc {
			if chanCountWait == p.totalChans {
				// when count gets to total number configured channels we rest index to 0
				chanCountWait = 0
			}
			chanName := "test-" + strconv.Itoa(chanCountWait)
			chanMap[chanName] = nil
			chanCountWait++
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			timeStamp := time.Now()
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
