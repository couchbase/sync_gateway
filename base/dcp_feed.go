//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/couchbase/go-couchbase/cbdatasource"
	"github.com/couchbase/gomemcached"
	sgbucket "github.com/couchbase/sg-bucket"
)

type couchbaseDCPFeedImpl struct {
	bds    cbdatasource.BucketDataSource
	events chan sgbucket.TapEvent
}

func (feed *couchbaseDCPFeedImpl) Events() <-chan sgbucket.TapEvent {
	return feed.events
}

func (feed *couchbaseDCPFeedImpl) WriteEvents() chan<- sgbucket.TapEvent {
	return feed.events
}

func (feed *couchbaseDCPFeedImpl) Close() error {
	return feed.bds.Close()
}

type SimpleFeed struct {
	eventFeed chan sgbucket.TapEvent
}

func (s *SimpleFeed) Events() <-chan sgbucket.TapEvent {
	return s.eventFeed
}

func (s *SimpleFeed) WriteEvents() chan<- sgbucket.TapEvent {
	return s.eventFeed
}

func (s *SimpleFeed) Close() error { // TODO
	log.Fatalf("SimpleFeed.Close() called but not implemented")
	return nil
}

type Receiver interface {
	cbdatasource.Receiver
	SeedSeqnos(map[uint16]uint64, map[uint16]uint64)
	GetEventFeed() <-chan sgbucket.TapEvent
	SetEventFeed(chan sgbucket.TapEvent)
	GetOutput() chan sgbucket.TapEvent
	updateSeq(vbucketId uint16, seq uint64, warnOnLowerSeqNo bool)
	SetBucketNotifyFn(sgbucket.BucketNotifyFn)
	GetBucketNotifyFn() sgbucket.BucketNotifyFn
}

// DCPReceiver implements cbdatasource.Receiver to manage updates coming from a
// cbdatasource BucketDataSource.  See go-couchbase/cbdatasource for
// additional details
type DCPReceiver struct {
	m         sync.Mutex
	seqs      map[uint16]uint64 // To track max seq #'s we received per vbucketId.
	meta      map[uint16][]byte // To track metadata blob's per vbucketId.
	eventFeed <-chan sgbucket.TapEvent
	output    chan sgbucket.TapEvent  // Same as EventFeed but writeably-typed
	notify    sgbucket.BucketNotifyFn // Function to callback when we lose our dcp feed
}

func NewDCPReceiver() Receiver {
	r := &DCPReceiver{
		output: make(chan sgbucket.TapEvent, 10),
	}
	r.eventFeed = r.output
	//r.SetEventFeed(r.GetOutput())

	if LogEnabledExcludingLogStar("DCP") {
		LogTo("DCP", "Using DCP Logging Receiver")
		logRec := &DCPLoggingReceiver{rec: r}
		return logRec
	}
	return r
}

func (r *DCPReceiver) SetBucketNotifyFn(notify sgbucket.BucketNotifyFn) {
	r.notify = notify
}

func (r *DCPReceiver) GetBucketNotifyFn() sgbucket.BucketNotifyFn {
	return r.notify
}

func (r *DCPReceiver) SetEventFeed(c chan sgbucket.TapEvent) {
	r.output = c
}

func (r *DCPReceiver) GetEventFeed() <-chan sgbucket.TapEvent {
	return r.eventFeed
}

func (r *DCPReceiver) GetOutput() chan sgbucket.TapEvent {
	return r.output
}

func (r *DCPReceiver) OnError(err error) {
	Warn("Feed", "Error processing DCP stream: %v", err)

	bucketName := "unknown" // this is currently ignored anyway
	r.notify(bucketName, err)

}

func (r *DCPReceiver) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	r.updateSeq(vbucketId, seq, true)
	r.output <- makeFeedEvent(req, vbucketId, sgbucket.TapMutation)
	return nil
}

func (r *DCPReceiver) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	r.updateSeq(vbucketId, seq, true)
	r.output <- makeFeedEvent(req, vbucketId, sgbucket.TapDeletion)
	return nil
}

func makeFeedEvent(rq *gomemcached.MCRequest, vbucketId uint16, opcode sgbucket.TapOpcode) sgbucket.TapEvent {
	// not currently doing rq.Extras handling (as in gocouchbase/upr_feed, makeUprEvent) as SG doesn't use
	// expiry/flags information, and snapshot handling is done by cbdatasource and sent as
	// SnapshotStart, SnapshotEnd
	event := sgbucket.TapEvent{
		Opcode:   opcode,
		Key:      rq.Key,
		Value:    rq.Body,
		Sequence: rq.Cas,
	}
	return event
}

func (r *DCPReceiver) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	// TODO: On snapshot start, could persist high sequence information when in a bucket shadowing
	// scenario, to support restart.  Not yet implemented due to concerns about impact of persistence
	// on the shadowing DCP feed, as the SnapshotStart gets issued per vbucket.  It's not clear that the
	// performance benefit on SG restart outweighs the performance impact during regular processing.
	return nil
}

// SetMetaData and GetMetaData used internally by cbdatasource.  Expects send/recieve of opaque
// []byte data.  cbdatasource is multithreaded so need to manage synchronization
func (r *DCPReceiver) SetMetaData(vbucketId uint16, value []byte) error {

	r.m.Lock()
	defer r.m.Unlock()

	if r.meta == nil {
		r.meta = make(map[uint16][]byte)
	}
	r.meta[vbucketId] = value

	return nil
}

func (r *DCPReceiver) GetMetaData(vbucketId uint16) (
	value []byte, lastSeq uint64, err error) {

	r.m.Lock()
	defer r.m.Unlock()

	value = []byte(nil)
	if r.meta != nil {
		value = r.meta[vbucketId]
	}

	if r.seqs != nil {
		lastSeq = r.seqs[vbucketId]
	}

	return value, lastSeq, nil
}

// Until we have CBL client support for rollback, we just rollback the sequence for the
// vbucket to unblock the DCP stream.
func (r *DCPReceiver) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	Warn("DCP Rollback request - rolling back DCP feed for: vbucketId: %d, rollbackSeq: %x", vbucketId, rollbackSeq)
	r.updateSeq(vbucketId, rollbackSeq, false)
	return nil
}

// This updates the value stored in r.seqs with the given seq number for the given partition
// (whic.  Setting warnOnLowerSeqNo to true will check
// if we are setting the seq number to a _lower_ value than we already have stored for that
// vbucket and log a warning in that case.  The valid case for setting warnOnLowerSeqNo to
// false is when it's a rollback scenario.  See https://github.com/couchbase/sync_gateway/issues/1098 for dev notes.
func (r *DCPReceiver) updateSeq(vbucketId uint16, seq uint64, warnOnLowerSeqNo bool) {
	r.m.Lock()
	defer r.m.Unlock()

	if r.seqs == nil {
		r.seqs = make(map[uint16]uint64)
	}
	if seq < r.seqs[vbucketId] && warnOnLowerSeqNo == true {
		Warn("Setting to _lower_ sequence number than previous: %v -> %v", r.seqs[vbucketId], seq)
	}

	r.seqs[vbucketId] = seq // Remember the max seq for GetMetaData().

}

// Seeds the sequence numbers returned by GetMetadata to support starting DCP from a particular
// sequence.
func (r *DCPReceiver) SeedSeqnos(uuids map[uint16]uint64, seqs map[uint16]uint64) {
	r.m.Lock()
	defer r.m.Unlock()

	// Set the high seqnos as-is
	r.seqs = seqs

	// For metadata, we need to do more work to build metadata based on uuid and map values.  This
	// isn't strictly to the design of cbdatasource.Receiver, which intends metadata to be opaque, but
	// is required in order to have the BucketDataSource start the UPRStream as needed.
	// The implementation has been reviewed with the cbdatasource owners and they agree this is a
	// reasonable approach, as the structure of VBucketMetaData is expected to rarely change.
	for vbucketId, uuid := range uuids {
		failOver := make([][]uint64, 1)
		failOverEntry := []uint64{uuid, 0}
		failOver[0] = failOverEntry
		metadata := &cbdatasource.VBucketMetaData{
			SeqStart:    seqs[vbucketId],
			SeqEnd:      uint64(0xFFFFFFFFFFFFFFFF),
			SnapStart:   seqs[vbucketId],
			SnapEnd:     seqs[vbucketId],
			FailOverLog: failOver,
		}
		buf, err := json.Marshal(metadata)
		if err == nil {
			if r.meta == nil {
				r.meta = make(map[uint16][]byte)
			}
			r.meta[vbucketId] = buf
		}
	}
}

// DCPReceiver implements cbdatasource.Receiver to manage updates coming from a
// cbdatasource BucketDataSource.  See go-couchbase/cbdatasource for
// additional details
type DCPLoggingReceiver struct {
	rec Receiver
}

func (r *DCPLoggingReceiver) OnError(err error) {
	LogTo("DCP", "OnError: %v", err)
	r.rec.OnError(err)
}

func (r *DCPLoggingReceiver) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	LogTo("DCP", "DataUpdate:%d, %s, %d, %v", vbucketId, key, seq, req)
	return r.rec.DataUpdate(vbucketId, key, seq, req)
}

func (r *DCPLoggingReceiver) SetBucketNotifyFn(notify sgbucket.BucketNotifyFn) {
	LogTo("DCP", "SetBucketNotifyFn()")
	r.rec.SetBucketNotifyFn(notify)
}

func (r *DCPLoggingReceiver) GetBucketNotifyFn() sgbucket.BucketNotifyFn {
	LogTo("DCP", "GetBucketNotifyFn()")
	return r.rec.GetBucketNotifyFn()
}

func (r *DCPLoggingReceiver) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	LogTo("DCP", "DataDelete:%d, %s, %d, %v", vbucketId, key, seq, req)
	return r.rec.DataDelete(vbucketId, key, seq, req)
}

func (r *DCPLoggingReceiver) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	LogTo("DCP", "Rollback:%d, %d", vbucketId, rollbackSeq)
	return r.rec.Rollback(vbucketId, rollbackSeq)
}

func (r *DCPLoggingReceiver) SetMetaData(vbucketId uint16, value []byte) error {

	LogTo("DCP", "SetMetaData:%d, %s", vbucketId, value)
	return r.rec.SetMetaData(vbucketId, value)
}

func (r *DCPLoggingReceiver) GetMetaData(vbucketId uint16) (
	value []byte, lastSeq uint64, err error) {
	LogTo("DCP", "GetMetaData:%d", vbucketId)
	return r.rec.GetMetaData(vbucketId)
}

func (r *DCPLoggingReceiver) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	LogTo("DCP", "SnapshotStart:%d, %d, %d, %d", vbucketId, snapStart, snapEnd, snapType)
	return r.rec.SnapshotStart(vbucketId, snapStart, snapEnd, snapType)
}

func (r *DCPLoggingReceiver) SeedSeqnos(uuids map[uint16]uint64, seqs map[uint16]uint64) {
	LogTo("DCP", "SeedSeqnos:%v, %v", uuids, seqs)
	r.rec.SeedSeqnos(uuids, seqs)
}

func (r *DCPLoggingReceiver) updateSeq(vbucketId uint16, seq uint64, warnOnLowerSeqNo bool) {
	r.rec.updateSeq(vbucketId, seq, warnOnLowerSeqNo)
}

func (r *DCPLoggingReceiver) SetEventFeed(c chan sgbucket.TapEvent) {
	r.rec.SetEventFeed(c)
}

func (r *DCPLoggingReceiver) GetEventFeed() <-chan sgbucket.TapEvent {
	return r.rec.GetEventFeed()
}

func (r *DCPLoggingReceiver) GetOutput() chan sgbucket.TapEvent {
	return r.rec.GetOutput()
}
