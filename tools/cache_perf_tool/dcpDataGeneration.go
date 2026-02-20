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
	"expvar"
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/rosmar"
)

var hlc = rosmar.NewHybridLogicalClock(0)

type dcpDataGen struct {
	seqAlloc          *sequenceAllocator
	delays            []time.Duration
	dbCtx             *db.DatabaseContext
	client            *base.GoCBDCPClient
	numChannelsPerDoc int
	numTotalChannels  int
	simRapidUpdate    bool
}

func (dcp *dcpDataGen) vBucketCreation(ctx context.Context) {
	delayIndex := 0
	// vBucket creation logic
	for i := range 1024 {
		time.Sleep(100 * time.Millisecond) // we need a slight delay each iteration otherwise many vBuckets end up writing at the same times
		if i == 520 {
			go dcp.syncSeqVBucketCreation(ctx, uint16(i), 2*time.Second) // sync seq hot vBucket so high delay
		} else {
			// iterate through provided delays and assign to vBucket, when we get to end of delay list reset index and
			// start from start again this will ensure some consistency between runs of the same parameters
			if delayIndex == len(dcp.delays) {
				delayIndex = 0 // reset index so we don't go out of bounds
			}
			go dcp.vBucketGoroutine(ctx, uint16(i), dcp.delays[delayIndex])
			delayIndex++
		}
	}
}

func (dcp *dcpDataGen) vBucketGoroutine(ctx context.Context, vbNo uint16, delay time.Duration) {
	numGoroutines.Add(1)
	defer numGoroutines.Add(-1)
	vbSeq := uint64(0)
	chanCount := 0
	var err error
	var newArr []byte
	var seqList []uint64
	if delay.Nanoseconds() == 0 {
		// mutate as fast as possible
		for {
			var sgwSeqno uint64
			if dcp.simRapidUpdate && vbSeq%2 == 0 { // simulate rapid update on subset of vBuckets if enabled
				seqList = dcp.seqAlloc.nextNSequences(5)
			} else {
				sgwSeqno = dcp.seqAlloc.nextSeq()
			}
			casVal := uint64(hlc.Now())
			select {
			case <-ctx.Done():
				return
			default:
				vbSeq++
				dcpMutation := gocbcore.DcpMutation{
					VbID:     vbNo,
					SeqNo:    vbSeq,
					StreamID: vbNo,
					Flags:    0,
					RevNo:    1,
					Expiry:   0,
					Cas:      casVal,
					Datatype: 5,
					Key:      []byte("key-" + strconv.FormatUint(vbSeq, 10) + "-" + strconv.FormatUint(sgwSeqno, 10)),
				}

				if sgwSeqno == 0 {
					newArr, chanCount, err = dcp.mutateWithDedupe(seqList, chanCount, casVal)
				} else {
					newArr, chanCount, err = dcp.mutateSyncData(sgwSeqno, chanCount, casVal)
				}
				if err != nil {
					log.Printf("Error setting sequence: %v", err)
					return
				}
				dcpMutation.Value = newArr
				dcp.client.Mutation(dcpMutation)
			}
		}
	}

	ticker := time.NewTicker(delay)
	defer ticker.Stop()
	// we have goroutine with a write delay associated with it
	for {
		var sgwSeqno uint64
		// allocate seq before wait on ticker
		if dcp.simRapidUpdate && vbSeq%2 == 0 { // simulate rapid update on subset of vBuckets if enabled
			seqList = dcp.seqAlloc.nextNSequences(5)
		} else {
			sgwSeqno = dcp.seqAlloc.nextSeq()
		}
		casVal := uint64(hlc.Now())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			vbSeq++
			dcpMutation := gocbcore.DcpMutation{
				VbID:     vbNo,
				SeqNo:    vbSeq,
				StreamID: vbNo,
				Flags:    0,
				RevNo:    1,
				Expiry:   0,
				Cas:      casVal,
				Datatype: 5,
				Key:      []byte("key-" + strconv.FormatUint(vbSeq, 10) + "-" + strconv.FormatUint(sgwSeqno, 10)),
			}

			if sgwSeqno == 0 {
				newArr, chanCount, err = dcp.mutateWithDedupe(seqList, chanCount, casVal)
			} else {
				newArr, chanCount, err = dcp.mutateSyncData(sgwSeqno, chanCount, casVal)
			}
			if err != nil {
				log.Printf("Error setting sequence: %v", err)
				return
			}
			dcpMutation.Value = newArr
			dcp.client.Mutation(dcpMutation)
		}
	}
}

func (dcp *dcpDataGen) syncSeqVBucketCreation(ctx context.Context, vbNo uint16, delay time.Duration) {
	numGoroutines.Add(1)
	defer numGoroutines.Add(-1)
	ticker := time.NewTicker(delay)
	defer ticker.Stop()
	vbSeq := uint64(0)
	chanCount := 0
	var err error
	var newArr []byte
	go func() {
		numGoroutines.Add(1)
		defer numGoroutines.Add(-1)
		for {
			select {
			case <-ctx.Done():
				return
			case _ = <-dcp.seqAlloc.syncSeqEvent:
				// channel has cap of 1 so sort of simulates dedupe on kv side
				vbSeq++
				dcpMutation := gocbcore.DcpMutation{
					VbID:     vbNo,
					SeqNo:    vbSeq,
					StreamID: vbNo,
					Flags:    0,
					RevNo:    1,
					Expiry:   0,
					Cas:      uint64(hlc.Now()),
					Datatype: 5,
					Key:      []byte("_sync:seq"),
					Value:    sgbucket.EncodeValueWithXattrs([]byte{50}),
				}
				dcp.client.Mutation(dcpMutation)
			}
		}
	}()

	for {
		// only allocate sgw seqno on actual write (not sync seq event) and BEFORE the delay as sgw will have allocated
		// it's seqno before any delay on vBuckets
		sgwSeqno := dcp.seqAlloc.nextSeq()
		casVal := uint64(hlc.Now())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			vbSeq++
			dcpMutation := gocbcore.DcpMutation{
				VbID:     vbNo,
				SeqNo:    vbSeq,
				StreamID: vbNo,
				Flags:    0,
				RevNo:    1,
				Expiry:   0,
				Cas:      casVal,
				Datatype: 5,
				Key:      []byte("key-" + strconv.FormatUint(vbSeq, 10) + "-" + strconv.FormatUint(sgwSeqno, 10)),
			}

			newArr, chanCount, err = dcp.mutateSyncData(sgwSeqno, chanCount, casVal)
			if err != nil {
				log.Printf("Error setting sequence: %v", err)
				return
			}
			dcpMutation.Value = newArr
			dcp.client.Mutation(dcpMutation)
		}
	}
}

func (dcp *dcpDataGen) mutateSyncData(sgwSeqno uint64, chanCount int, casValue uint64) ([]byte, int, error) {
	chanMap := make(channels.ChannelMap)
	chanSet := make([]db.ChannelSetEntry, 0, dcp.numChannelsPerDoc)
	chanSetMap := base.Set{}
	for i := 0; i < dcp.numChannelsPerDoc; i++ {
		if chanCount == dcp.numTotalChannels {
			chanCount = 0 // reset channel count so we don't go out of bounds
		}
		chanName := "test-" + strconv.Itoa(chanCount)
		chanMap[chanName] = nil
		chanSet = append(chanSet, db.ChannelSetEntry{
			Name:  chanName,
			Start: sgwSeqno,
		})
		chanSetMap[chanName] = struct{}{}
		chanCount++
	}
	revInf := db.RevInfo{
		ID:       "1-abc",
		Channels: chanSetMap,
	}
	revTree := db.RevTree{
		"1-abc": &revInf,
	}

	syncData := db.SyncData{
		Sequence: sgwSeqno,
		RevAndVersion: channels.RevAndVersion{
			RevTreeID: "1-abc",
		},
		History:         revTree,
		Channels:        chanMap,
		ChannelSet:      chanSet,
		TimeSaved:       time.Now(),
		Cas:             base.CasToString(casValue),
		RecentSequences: []uint64{sgwSeqno},
	}
	byteArrSync, err := base.JSONMarshal(syncData)
	if err != nil {
		log.Printf("Error marshalling sync data: %v", err)
		return nil, 0, err
	}

	inp := sgbucket.Xattr{
		Name:  "_sync",
		Value: byteArrSync,
	}
	encodedVal := sgbucket.EncodeValueWithXattrs([]byte(`{"some":"body"}`), inp)
	return encodedVal, chanCount, nil
}

func (dcp *dcpDataGen) mutateWithDedupe(seqs []uint64, chanCount int, casValue uint64) ([]byte, int, error) {
	chanMap := make(channels.ChannelMap)
	chanSet := make([]db.ChannelSetEntry, 0, dcp.numChannelsPerDoc)
	currSeq := seqs[len(seqs)-1] // grab current seq form end of seq list
	chanSetMap := base.Set{}
	for i := 0; i < dcp.numChannelsPerDoc; i++ {
		if chanCount == dcp.numTotalChannels {
			chanCount = 0 // reset channel count so we don't go out of bounds
		}
		chanName := "test-" + strconv.Itoa(chanCount)
		chanMap[chanName] = nil
		chanSet = append(chanSet, db.ChannelSetEntry{
			Name:  chanName,
			Start: currSeq,
		})
		chanSetMap[chanName] = struct{}{}
		chanCount++
	}

	revTree := make(db.RevTree)
	var revInf db.RevInfo
	for i := 1; i <= len(seqs); i++ {
		dummyRevID := fmt.Sprintf("%d-abc", i)
		revInf = db.RevInfo{
			ID:       dummyRevID,
			Channels: chanSetMap,
		}
		revTree[dummyRevID] = &revInf
	}
	currRev := fmt.Sprintf("%d-abc", len(seqs))

	syncData := db.SyncData{
		Sequence: currSeq,
		RevAndVersion: channels.RevAndVersion{
			RevTreeID: currRev,
		},
		History:         revTree,
		Channels:        chanMap,
		ChannelSet:      chanSet,
		TimeSaved:       time.Now(),
		Cas:             base.CasToString(casValue),
		RecentSequences: seqs,
	}
	byteArrSync, err := base.JSONMarshal(syncData)
	if err != nil {
		log.Printf("Error marshalling sync data: %v", err)
		return nil, 0, err
	}

	inp := sgbucket.Xattr{
		Name:  "_sync",
		Value: byteArrSync,
	}
	encodedVal := sgbucket.EncodeValueWithXattrs([]byte(`{"some":"body"}`), inp)
	return encodedVal, chanCount, nil
}

func createDCPClient(t *testing.T, ctx context.Context, bucket *base.GocbV2Bucket, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map, numWorkers, numVBuckets int) (*base.GoCBDCPClient, error) {
	options := base.DCPClientOptions{
		MetadataStoreType: base.DCPMetadataStoreInMemory,
		DbStats:           dbStats,
		CollectionIDs:     []uint32{0},
		AgentPriority:     gocbcore.DcpAgentPriorityMed,
		CheckpointPrefix:  "",
		NumWorkers:        numWorkers,
	}
	// fake client that we want to hook into
	client, err := base.NewDCPClientForTest(ctx, t, "test", callback, options, bucket, uint16(numVBuckets))
	if err != nil {
		return nil, err
	}
	// we want to start dcp workers but not client given we aren't streaming data from a bucket for this test
	client.StartWorkersForTest(t)
	return client, err
}
