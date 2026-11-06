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
)

var hlc = sgbucket.NewHybridLogicalClock()

// syncSeqVBucket is the vBucket the generator treats as the hot _sync:seq vBucket. It is given a long
// write delay and its own goroutine, so it behaves like the single frequently-updated sequence
// document rather than a normal data vBucket. Clamped into range when running with fewer vBuckets -
// see vBucketCreation.
const syncSeqVBucket = 520

// mutationSink receives the generated DCP mutations: the DCP client in a run, a recorder in tests.
type mutationSink interface {
	Mutation(gocbcore.DcpMutation)
}

type dcpDataGen struct {
	seqAlloc          *sequenceAllocator
	delays            []time.Duration
	dbCtx             *db.DatabaseContext
	client            mutationSink
	numChannelsPerDoc int
	numTotalChannels  int
	simRapidUpdate    bool
	numVBuckets       int // number of vBucket writer goroutines to create; also the DCP client's vBucket count
	// writerGoroutines, when > 0, drives the data vBuckets from this many goroutines, each owning a
	// round-robin share, instead of one goroutine per vBucket. Only valid with every write delay at 0
	// (checked in main).
	//
	// In production the goroutines sending into the DCP worker queues are gocbcore's DCP dispatch
	// goroutines, one per KV node per feed (DCP agents use a single connection per node, see
	// GoCBPoolSizeDCP), each covering the vBuckets active on that node. With one writer per vBucket,
	// 1024 senders contend on the worker queues and on the shared sequence allocator, which caps the
	// generator below what a faster change cache can absorb.
	//
	// This is not a pure "number of KV nodes" setting: each writer here also allocates Sync Gateway
	// sequences, which production does on the write path across many concurrent writers. Fewer writers
	// therefore also means fewer sequences allocated but not yet delivered, so less arrival disorder, and
	// a different pending/skipped workload. Only compare runs at the same writer count.
	writerGoroutines int
	// templates, when set, builds each document from pre-marshalled _sync templates (-generator=template).
	// nil marshals every document from scratch (-generator=marshal).
	templates *syncTemplates
}

// vBucketCreation starts one writer goroutine per vBucket and returns once they are all running.
//
// Writers are staggered by 100ms, so this BLOCKS for numVBuckets*100ms before the caller's -duration
// timer starts: ~100s at the default 1024 vBuckets, proportionally less below that.
//
// numVBuckets is the offered write concurrency, so lowering it lowers the load the run applies. Runs
// with different vBucket counts are not comparable with each other.
func (dcp *dcpDataGen) vBucketCreation(ctx context.Context) {
	delayIndex := 0
	// The hot sync-seq vBucket has a fixed ID, so with fewer vBuckets than that ID it is clamped to the
	// last one rather than silently not being created at all. At numVBuckets == 1 it is dropped
	// entirely (-1 never matches below) so the single vBucket is a data writer - otherwise a 1-vBucket
	// run would generate almost nothing. At the default 1024 this leaves it exactly where it has
	// always been.
	syncSeqVb := -1
	if dcp.numVBuckets > 1 {
		syncSeqVb = min(syncSeqVBucket, dcp.numVBuckets-1)
	}
	if dcp.writerGoroutines > 0 {
		dcp.startWriterGroups(ctx, syncSeqVb)
		return
	}
	// vBucket creation logic
	for i := range dcp.numVBuckets {
		time.Sleep(100 * time.Millisecond) // we need a slight delay each iteration otherwise many vBuckets end up writing at the same times
		if i == syncSeqVb {
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

// startWriterGroups spreads the data vBuckets over writerGoroutines goroutines (see writerGroups). The
// hot sync-seq vBucket keeps its own goroutine, as in the one-per-vBucket shape. Like vBucketCreation
// it staggers goroutine starts by 100ms, which here is writerGoroutines*100ms rather than ~100s.
func (dcp *dcpDataGen) startWriterGroups(ctx context.Context, syncSeqVb int) {
	if syncSeqVb >= 0 {
		go dcp.syncSeqVBucketCreation(ctx, uint16(syncSeqVb), 2*time.Second)
	}
	for _, vbs := range writerGroups(dcp.numVBuckets, dcp.writerGoroutines, syncSeqVb) {
		time.Sleep(100 * time.Millisecond)
		go dcp.flatOutWriter(ctx, vbs)
	}
}

// writerGroups deals vBuckets 0..numVBuckets-1, other than syncSeqVb, round-robin into at most writers
// groups, so every data vBucket has exactly one writer and so one ordered stream of sequence numbers.
// With fewer data vBuckets than writers, only as many groups as there are vBuckets are returned.
func writerGroups(numVBuckets, writers, syncSeqVb int) [][]uint16 {
	groups := make([][]uint16, 0, writers)
	next := 0
	for i := range numVBuckets {
		if i == syncSeqVb {
			continue
		}
		if next == len(groups) {
			groups = append(groups, nil)
		}
		groups[next] = append(groups[next], uint16(i))
		next = (next + 1) % writers
	}
	return groups
}

// recentSeqsPerRapidUpdate is how many sequences a -rapidUpdateDocs write carries in RecentSequences.
const recentSeqsPerRapidUpdate = 5

// pendingWrite is a document write whose Sync Gateway sequence(s) and CAS have been allocated but which
// has not been sent yet. Allocation and send are separate steps because a delayed writer allocates
// before it waits, as Sync Gateway does: the gap between the two is what produces arrival disorder.
type pendingWrite struct {
	// seqs[:numSeqs] are the document's recent sequences, oldest first; the last is its current sequence.
	// One for a normal write, recentSeqsPerRapidUpdate for a -rapidUpdateDocs write. A fixed array so a
	// normal write allocates nothing here.
	seqs    [recentSeqsPerRapidUpdate]uint64
	numSeqs int
	cas     uint64
}

func (w *pendingWrite) recentSeqs() []uint64 {
	return w.seqs[:w.numSeqs]
}

// allocateWrite allocates the sequence(s) and CAS for the next write to a vBucket. vbSeq is the
// vBucket's DCP seqno before this write; with -rapidUpdateDocs every other write on a vBucket carries
// recent sequences.
func (dcp *dcpDataGen) allocateWrite(vbSeq uint64) pendingWrite {
	var w pendingWrite
	if dcp.simRapidUpdate && vbSeq%2 == 0 {
		w.numSeqs = copy(w.seqs[:], dcp.seqAlloc.nextNSequences(recentSeqsPerRapidUpdate))
	} else {
		w.seqs[0] = dcp.seqAlloc.nextSeq()
		w.numSeqs = 1
	}
	w.cas = hlc.Now(0)
	return w
}

// docWriter is one writer goroutine's document builder: its position in the channel list and, for the
// template generator, a scratch buffer for _sync that is reused across writes.
type docWriter struct {
	dcp       *dcpDataGen
	chanCount int // the next channel index to assign; advanced by every write
	scratch   []byte
}

// send builds the document for w and delivers it to the DCP client as vBucket vbNo's mutation vbSeq.
//
// The value is xattrs only, with no body, because the caching feed is opened xattr-only
// (FeedContentXattrOnly): Couchbase Server sends the xattrs and drops the body, but keeps reporting the
// document's underlying datatype, so Datatype stays JSON|XATTR.
//
// The key includes vbNo because (vbNo, vbSeq) is the only pair unique across vBuckets for every write:
// a recent-sequences write has no single sequence (its key ends in 0), and the channel cache dedupes
// entries by doc ID, so colliding keys would turn new documents into updates.
func (dw *docWriter) send(vbNo uint16, vbSeq uint64, w *pendingWrite) error {
	timeSaved := time.Now()
	var value []byte
	var err error
	if dw.dcp.templates != nil {
		value, dw.chanCount, dw.scratch, err = dw.dcp.templates.value(dw.scratch, w, dw.chanCount, timeSaved)
	} else {
		value, dw.chanCount, err = dw.dcp.marshalValue(w, dw.chanCount, timeSaved)
	}
	if err != nil {
		return err
	}
	var keySeq uint64
	if w.numSeqs == 1 {
		keySeq = w.seqs[0]
	}
	key := make([]byte, 0, 64)
	key = append(key, "key-"...)
	key = strconv.AppendUint(key, uint64(vbNo), 10)
	key = append(key, '-')
	key = strconv.AppendUint(key, vbSeq, 10)
	key = append(key, '-')
	key = strconv.AppendUint(key, keySeq, 10)
	dw.dcp.client.Mutation(gocbcore.DcpMutation{
		VbID:     vbNo,
		SeqNo:    vbSeq,
		StreamID: vbNo,
		RevNo:    1,
		Cas:      w.cas,
		Datatype: 5,
		Key:      key,
		Value:    value,
	})
	return nil
}

// flatOutWriter writes to vbs in turn as fast as it can, keeping a separate DCP seqno per vBucket so
// each vBucket's stream stays in order. A one-per-vBucket writer with no delay is the len(vbs) == 1 case.
func (dcp *dcpDataGen) flatOutWriter(ctx context.Context, vbs []uint16) {
	numGoroutines.Add(1)
	defer numGoroutines.Add(-1)
	vbSeqs := make([]uint64, len(vbs))
	dw := &docWriter{dcp: dcp}
	for i := 0; ctx.Err() == nil; i = (i + 1) % len(vbs) {
		w := dcp.allocateWrite(vbSeqs[i])
		vbSeqs[i]++
		if err := dw.send(vbs[i], vbSeqs[i], &w); err != nil {
			log.Printf("Error setting sequence: %v", err)
			return
		}
	}
}

func (dcp *dcpDataGen) vBucketGoroutine(ctx context.Context, vbNo uint16, delay time.Duration) {
	if delay == 0 {
		dcp.flatOutWriter(ctx, []uint16{vbNo})
		return
	}
	numGoroutines.Add(1)
	defer numGoroutines.Add(-1)
	vbSeq := uint64(0)
	dw := &docWriter{dcp: dcp}
	ticker := time.NewTicker(delay)
	defer ticker.Stop()
	for {
		// allocate before waiting on the ticker, as Sync Gateway allocates before the write lands
		w := dcp.allocateWrite(vbSeq)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			vbSeq++
			if err := dw.send(vbNo, vbSeq, &w); err != nil {
				log.Printf("Error setting sequence: %v", err)
				return
			}
		}
	}
}

// syncSeqVBucketCreation writes the hot vBucket that holds _sync:seq: a _sync:seq update for each
// syncSeqEvent and a one-sequence document write every delay.
func (dcp *dcpDataGen) syncSeqVBucketCreation(ctx context.Context, vbNo uint16, delay time.Duration) {
	numGoroutines.Add(1)
	defer numGoroutines.Add(-1)
	ticker := time.NewTicker(delay)
	defer ticker.Stop()
	// Both writes are sent from this goroutine so the vBucket's DCP sequence numbers are strictly
	// increasing, as KV guarantees per vBucket.
	vbSeq := uint64(0)
	dw := &docWriter{dcp: dcp}
	// The document's Sync Gateway sequence is allocated before the delay, as Sync Gateway allocates
	// before the write lands. It is reallocated only once that write is sent, so a _sync:seq update
	// arriving in between does not consume one.
	w := dcp.newSyncSeqVbWrite()
	for {
		select {
		case <-ctx.Done():
			return
		case <-dcp.seqAlloc.syncSeqEvent:
			// syncSeqEvent has a capacity of 1, which stands in for KV deduplicating _sync:seq updates
			vbSeq++
			dcp.client.Mutation(gocbcore.DcpMutation{
				VbID:     vbNo,
				SeqNo:    vbSeq,
				StreamID: vbNo,
				RevNo:    1,
				Cas:      hlc.Now(0),
				Datatype: 5,
				Key:      []byte("_sync:seq"),
				Value:    sgbucket.EncodeValueWithXattrs([]byte{50}),
			})
		case <-ticker.C:
			vbSeq++
			if err := dw.send(vbNo, vbSeq, &w); err != nil {
				log.Printf("Error setting sequence: %v", err)
				return
			}
			w = dcp.newSyncSeqVbWrite()
		}
	}
}

// newSyncSeqVbWrite allocates the next document write for the sync-seq vBucket: one sequence, never a
// rapid update.
func (dcp *dcpDataGen) newSyncSeqVbWrite() pendingWrite {
	w := pendingWrite{numSeqs: 1, cas: hlc.Now(0)}
	w.seqs[0] = dcp.seqAlloc.nextSeq()
	return w
}

// syncDataFor builds the _sync for a write carrying seqs (oldest first, the last being the document's
// current sequence), assigning the next numChannelsPerDoc channels from chanCount on, wrapping at
// numTotalChannels. It returns the channel cursor for the next write. Both generators build _sync with
// this, so the template generator's output cannot drift from the marshal generator's.
func (dcp *dcpDataGen) syncDataFor(seqs []uint64, chanCount int, cas uint64, timeSaved time.Time) (db.SyncData, int) {
	currSeq := seqs[len(seqs)-1]
	chanMap := make(channels.ChannelMap, dcp.numChannelsPerDoc)
	chanSet := make([]db.ChannelSetEntry, 0, dcp.numChannelsPerDoc)
	chanSetMap := make(base.Set, dcp.numChannelsPerDoc)
	for range dcp.numChannelsPerDoc {
		if chanCount == dcp.numTotalChannels {
			chanCount = 0 // wrap so documents keep cycling through the system's channels
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

	// One revision per sequence, so a rapidly updated document has a history as long as its recent
	// sequences.
	revTree := make(db.RevTree, len(seqs))
	for i := 1; i <= len(seqs); i++ {
		revID := strconv.Itoa(i) + "-abc"
		revTree[revID] = &db.RevInfo{
			ID:       revID,
			Channels: chanSetMap,
		}
	}

	return db.SyncData{
		Sequence: currSeq,
		RevAndVersion: channels.RevAndVersion{
			RevTreeID: strconv.Itoa(len(seqs)) + "-abc",
		},
		History:         revTree,
		Channels:        chanMap,
		ChannelSet:      chanSet,
		TimeSaved:       timeSaved,
		Cas:             base.CasToString(cas),
		RecentSequences: seqs,
	}, chanCount
}

// marshalValue is the -generator=marshal path: build and marshal a fresh _sync for every write. It
// allocates heavily, and that garbage is collected in the same process as the cache under test.
func (dcp *dcpDataGen) marshalValue(w *pendingWrite, chanCount int, timeSaved time.Time) ([]byte, int, error) {
	syncData, next := dcp.syncDataFor(w.recentSeqs(), chanCount, w.cas, timeSaved)
	syncJSON, err := base.JSONMarshal(syncData)
	if err != nil {
		return nil, chanCount, fmt.Errorf("marshalling sync data: %w", err)
	}
	return sgbucket.EncodeValueWithXattrs(nil, sgbucket.Xattr{Name: base.SyncXattrName, Value: syncJSON}), next, nil
}

func createDCPClient(t *testing.T, ctx context.Context, bucket *base.GocbV2Bucket, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map, numWorkers int, numVBuckets uint16) (*base.GoCBDCPClient, error) {
	options := base.GoCBDCPClientOptions{
		FeedID:            "dcpDataGeneration",
		MetadataStoreType: base.DCPMetadataStoreInMemory,
		DbStats:           dbStats,
		CollectionIDs:     []uint32{0},
		AgentPriority:     gocbcore.DcpAgentPriorityMed,
		CheckpointPrefix:  "",
		NumWorkers:        numWorkers,
		NumVBuckets:       numVBuckets,
	}
	// fake client that we want to hook into
	client, err := base.NewGocbDCPClient(ctx, callback, options, bucket)
	if err != nil {
		return nil, err
	}
	// we want to start dcp workers but not client given we aren't streaming data from a bucket for this test
	client.StartWorkersForTest(t)
	return client, err
}
