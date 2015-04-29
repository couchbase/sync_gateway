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
	"sync"

	"github.com/couchbase/go-couchbase/cbdatasource"
	"github.com/couchbase/gomemcached"
	"github.com/couchbaselabs/walrus"
)

type couchbaseDCPFeedImpl struct {
	bds    cbdatasource.BucketDataSource
	events <-chan walrus.TapEvent
}

func (feed *couchbaseDCPFeedImpl) Events() <-chan walrus.TapEvent {
	return feed.events
}

func (feed *couchbaseDCPFeedImpl) Close() error {
	return feed.bds.Close()
}

// Implementation of couchbase.AuthHandler for use by cbdatasource during bucket connect.
type dcpAuth struct {
	Username, Password, BucketName string
}

// GetCredentials is used when cbdatasource makes a new bucket connection, based on approach we're
// using for our other bucket connections (via go-couchbase pools.go authHandler()).
// Background: After hitting an auth issue when connecting to a new cbdatasource bucket with the
// standard username/password authenticator, I talked to Steve Yen and he tipped me off about the
// default bucketname-as-username handling, and figured out that go-couchbase was doing that for us
// for our usual bucket handling.
// Reviewed with Steve and he said that the auth handling in couchbase server is undergoing changes
// as part of Sherlock, so we may need to revisit once they've stabilized on an approach.
func (a dcpAuth) GetCredentials() (string, string, string) {
	if a.Username == "" {
		return a.BucketName, "", a.BucketName
	} else {
		return a.Username, a.Password, a.BucketName
	}
}

type Receiver interface {
	cbdatasource.Receiver
	SeedSeqnos(map[uint16]uint64, map[uint16]uint64)
	GetEventFeed() <-chan walrus.TapEvent
	SetEventFeed(chan walrus.TapEvent)
	GetOutput() chan walrus.TapEvent
	updateSeq(vbucketId uint16, seq uint64)
}

// DCPReceiver implements cbdatasource.Receiver to manage updates coming from a
// cbdatasource BucketDataSource.  See go-couchbase/cbdatasource for
// additional details
type DCPReceiver struct {
	m         sync.Mutex
	seqs      map[uint16]uint64 // To track max seq #'s we received per vbucketId.
	meta      map[uint16][]byte // To track metadata blob's per vbucketId.
	eventFeed <-chan walrus.TapEvent
	output    chan walrus.TapEvent // Same as EventFeed but writeably-typed
}

func NewDCPReceiver() Receiver {
	r := &DCPReceiver{
		output: make(chan walrus.TapEvent, 10),
	}
	r.eventFeed = r.output
	//r.SetEventFeed(r.GetOutput())

	if LogKeys["DCP"] {
		LogTo("DCP", "Using DCP Logging Receiver")
		logRec := &DCPLoggingReceiver{rec: r}
		return logRec
	}
	return r
}

func (r *DCPReceiver) SetEventFeed(c chan walrus.TapEvent) {
	r.output = c
}

func (r *DCPReceiver) GetEventFeed() <-chan walrus.TapEvent {
	return r.eventFeed
}

func (r *DCPReceiver) GetOutput() chan walrus.TapEvent {
	return r.output
}

func (r *DCPReceiver) OnError(err error) {
	Warn("Feed", "Error processing DCP stream: %v", err)
}

func (r *DCPReceiver) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	r.updateSeq(vbucketId, seq)
	r.output <- makeFeedEvent(req, vbucketId, walrus.TapMutation)
	return nil
}

func (r *DCPReceiver) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	r.updateSeq(vbucketId, seq)
	r.output <- makeFeedEvent(req, vbucketId, walrus.TapDeletion)
	return nil
}

func makeFeedEvent(rq *gomemcached.MCRequest, vbucketId uint16, opcode walrus.TapOpcode) walrus.TapEvent {
	// not currently doing rq.Extras handling (as in gocouchbase/upr_feed, makeUprEvent) as SG doesn't use
	// expiry/flags information, and snapshot handling is done by cbdatasource and sent as
	// SnapshotStart, SnapshotEnd
	event := walrus.TapEvent{
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
	r.updateSeq(vbucketId, rollbackSeq)
	return nil
}

func (r *DCPReceiver) updateSeq(vbucketId uint16, seq uint64) {
	r.m.Lock()
	defer r.m.Unlock()

	if r.seqs == nil {
		r.seqs = make(map[uint16]uint64)
	}
	if r.seqs[vbucketId] < seq {
		r.seqs[vbucketId] = seq // Remember the max seq for GetMetaData().
	}
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

func (r *DCPLoggingReceiver) updateSeq(vbucketId uint16, seq uint64) {
	r.rec.updateSeq(vbucketId, seq)
}

func (r *DCPLoggingReceiver) SetEventFeed(c chan walrus.TapEvent) {
	r.rec.SetEventFeed(c)
}

func (r *DCPLoggingReceiver) GetEventFeed() <-chan walrus.TapEvent {
	return r.rec.GetEventFeed()
}

func (r *DCPLoggingReceiver) GetOutput() chan walrus.TapEvent {
	return r.rec.GetOutput()
}
