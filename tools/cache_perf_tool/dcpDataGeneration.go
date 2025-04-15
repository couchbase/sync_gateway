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
	"encoding/json"
	"expvar"
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
	seqAlloc    *sequenceAllocator
	delays      []time.Duration
	dbCtx       *db.DatabaseContext
	client      *base.DCPClient
	numChannels int
}

func (dcp *dcpDataGen) vBucketCreation(ctx context.Context) {
	// setup the sync data for test run
	testSync := dcp.setupSyncDataForTestRun()
	delayIndex := 0
	// vBucket creation logic
	for i := 0; i < 1024; i++ {
		if i == 520 {
			go dcp.syncSeqVBucketCreation(ctx, uint16(i), 2*time.Second, testSync) // sync seq hot vBucket so high delay
		} else {
			// iterate through provided delays and assign to vBucket, when we get to end of delay list reset index and
			// start from start again this will ensure some consistency between runs of the same parameters
			if delayIndex == len(dcp.delays) {
				delayIndex = 0 // reset index so we don't go out of bounds
			}
			go dcp.vBucketGoroutine(ctx, uint16(i), dcp.delays[delayIndex], testSync)
			delayIndex++
		}
	}
}

func (dcp *dcpDataGen) vBucketGoroutine(ctx context.Context, vbNo uint16, delay time.Duration, syncData db.SyncData) {
	numGoroutines.Add(1)
	defer numGoroutines.Add(-1)
	vbSeq := uint64(0)
	if delay.Nanoseconds() == 0 {
		// mutate as fast as possible
		for {
			sgwSeqno := dcp.seqAlloc.nextSeq()
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
					Cas:      uint64(hlc.Now()),
					Datatype: 5,
					Key:      []byte("key-" + strconv.FormatUint(vbSeq, 10) + "-" + strconv.FormatUint(sgwSeqno, 10)),
				}

				newArr, err := mutateSyncData(sgwSeqno, syncData)
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
		sgwSeqno := dcp.seqAlloc.nextSeq() // allocate seq before wait on ticker
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
				Cas:      uint64(hlc.Now()),
				Datatype: 5,
				Key:      []byte("key-" + strconv.FormatUint(vbSeq, 10) + "-" + strconv.FormatUint(sgwSeqno, 10)),
			}

			newArr, err := mutateSyncData(sgwSeqno, syncData)
			if err != nil {
				log.Printf("Error setting sequence: %v", err)
				return
			}
			dcpMutation.Value = newArr
			dcp.client.Mutation(dcpMutation)
		}
	}
}

func (dcp *dcpDataGen) syncSeqVBucketCreation(ctx context.Context, vbNo uint16, delay time.Duration, syncData db.SyncData) {
	numGoroutines.Add(1)
	defer numGoroutines.Add(-1)
	ticker := time.NewTicker(delay)
	defer ticker.Stop()
	vbSeq := uint64(0)
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
				Cas:      uint64(hlc.Now()),
				Datatype: 5,
				Key:      []byte("key-" + strconv.FormatUint(vbSeq, 10) + "-" + strconv.FormatUint(sgwSeqno, 10)),
			}

			newArr, err := mutateSyncData(sgwSeqno, syncData)
			if err != nil {
				log.Printf("Error setting sequence: %v", err)
				return
			}
			dcpMutation.Value = newArr
			dcp.client.Mutation(dcpMutation)
		}
	}
}

func updateSyncData(seq uint64, syncData db.SyncData) ([]byte, error) {
	syncData.Sequence = seq
	syncData.RecentSequences = []uint64{seq}
	// update seq info on channel set map
	for _, v := range syncData.ChannelSet {
		v.Start = seq
	}

	byteArrSync, err := json.Marshal(syncData)
	if err != nil {
		log.Printf("Error marshalling sync data: %v", err)
		return nil, err
	}

	return byteArrSync, nil
}

func (dcp *dcpDataGen) setupSyncDataForTestRun() db.SyncData {
	chanMap := make(channels.ChannelMap)
	chanSet := make([]db.ChannelSetEntry, 0, dcp.numChannels)
	chanSetMap := base.Set{}
	for i := 0; i < dcp.numChannels; i++ {
		numChan := strconv.Itoa(i)
		chanMap["test-"+numChan] = nil
		chanSet = append(chanSet, db.ChannelSetEntry{
			Name: "test-" + numChan,
		})
		chanSetMap["test-"+numChan] = struct{}{}
	}
	revInf := db.RevInfo{
		ID:       "1-abc",
		Channels: chanSetMap,
	}
	revTree := db.RevTree{
		"1-abc": &revInf,
	}

	// return some sync data for the test, with channel info according to test parameters (sequence info added later)
	// This will make generation of syn data more efficient, instead of generating the whole sync data object we just
	// update sequence information on each mutation
	return db.SyncData{
		Sequence:    0,
		CurrentRev:  "1-abc",
		History:     revTree,
		Channels:    chanMap,
		ChannelSet:  chanSet,
		TimeSaved:   time.Now(),
		Cas:         "0x000008cc2ee83118",
		ClusterUUID: "6a1a82a8ea79aa8b82d3f5667892d9ce",
		Crc32c:      "0x615126c4",
	}
}

func mutateSyncData(sgwSeqno uint64, syncData db.SyncData) ([]byte, error) {
	newArr, err := updateSyncData(sgwSeqno, syncData)
	if err != nil {
		return nil, err
	}
	inp := sgbucket.Xattr{
		Name:  "_sync",
		Value: newArr,
	}
	encodedVal := sgbucket.EncodeValueWithXattrs([]byte(`{"some":"body"}`), inp)
	return encodedVal, nil
}

func createDCPClient(t *testing.T, ctx context.Context, bucket *base.GocbV2Bucket, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) (*base.DCPClient, error) {
	options := base.DCPClientOptions{
		MetadataStoreType: base.DCPMetadataStoreInMemory,
		GroupID:           "",
		DbStats:           dbStats,
		CollectionIDs:     []uint32{0},
		AgentPriority:     gocbcore.DcpAgentPriorityMed,
		CheckpointPrefix:  "",
	}

	client, err := base.NewDCPClientForTest(ctx, t, "test", callback, options, bucket, 1024)
	if err != nil {
		return nil, err
	}
	// we want to start dcp workers but not client given we aren't streaming data from a bucket for this test
	client.StartWorkersForTest(t)
	return client, err
}
