// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"time"

	"github.com/couchbase/gocbcore/v10"
	sgbucket "github.com/couchbase/sg-bucket"
)

// streamEvent is an interface for events that can be sent on a DCP stream.
type streamEvent interface {
	VbID() uint16
}

// streamEventCommon is a struct that contains common fields for all stream events.
type streamEventCommon struct {
	vbID     uint16
	streamID uint16
}

// VbID return the vBucket ID for the event.
func (sec streamEventCommon) VbID() uint16 {
	return sec.vbID
}

// snapshotEvent represents a DCP snapshot event (opcode 0x56).
type snapshotEvent struct {
	startSeq     uint64
	endSeq       uint64
	snapshotType gocbcore.SnapshotState
	streamEventCommon
}

// mutationEvent represents a DCP mutation event (opcode 0x57).
type mutationEvent struct {
	key        []byte
	value      []byte
	seq        uint64
	cas        uint64
	flags      uint32
	expiry     uint32
	collection uint32
	streamEventCommon
	datatype uint8
}

// asFeedEvent converts a mutationEvent to a sgbucket.FeedEvent.
func (e mutationEvent) asFeedEvent() sgbucket.FeedEvent {
	return sgbucket.FeedEvent{
		Opcode:       sgbucket.FeedOpMutation,
		Flags:        e.flags,
		Expiry:       e.expiry,
		CollectionID: e.collection,
		Key:          e.key,
		Value:        e.value,
		DataType:     e.datatype,
		Cas:          e.cas,
		VbNo:         e.vbID,
		TimeReceived: time.Now(),
	}
}

// deletionEvent represents a DCP deletion event (opcode 0x58).
type deletionEvent struct {
	key        []byte
	value      []byte
	seq        uint64
	cas        uint64
	collection uint32
	datatype   uint8
	streamEventCommon
}

// asFeedEvent converts a deletionEvent to a sgbucket.FeedEvent.
func (e deletionEvent) asFeedEvent() sgbucket.FeedEvent {
	return sgbucket.FeedEvent{
		Opcode:       sgbucket.FeedOpDeletion,
		CollectionID: e.collection,
		Key:          e.key,
		Value:        e.value,
		DataType:     e.datatype,
		Cas:          e.cas,
		VbNo:         e.vbID,
		TimeReceived: time.Now(),
	}
}

// endStreamEvent represents a DCP end stream event, and the error associated with the stream end (opcode 0x55).
type endStreamEvent struct {
	err error
	streamEventCommon
}

// seqnoAdvancedEvent represents a DCP Seqno advanced event (opcode 0x64).
type seqnoAdvancedEvent struct {
	streamEventCommon
	seq uint64
}
