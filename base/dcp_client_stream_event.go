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

type streamEvent interface {
	VbID() uint16
}

type streamEventCommon struct {
	vbID     uint16
	streamID uint16
}

func (sec streamEventCommon) VbID() uint16 {
	return sec.vbID
}

type snapshotEvent struct {
	streamEventCommon
	startSeq     uint64
	endSeq       uint64
	snapshotType gocbcore.SnapshotState
}

type mutationEvent struct {
	streamEventCommon
	seq        uint64
	flags      uint32
	expiry     uint32
	cas        uint64
	datatype   uint8
	collection uint32
	key        []byte
	value      []byte
}

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

type deletionEvent struct {
	streamEventCommon
	seq        uint64
	cas        uint64
	datatype   uint8
	collection uint32
	key        []byte
	value      []byte
}

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

type endStreamEvent struct {
	streamEventCommon
	err error
}

type seqnoAdvancedEvent struct {
	streamEventCommon
	seq uint64
}
