package base

import (
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
	seq      uint64
	flags    uint32
	expiry   uint32
	cas      uint64
	datatype uint8
	key      []byte
	value    []byte
}

func (e mutationEvent) asFeedEvent() sgbucket.FeedEvent {
	return sgbucket.FeedEvent{
		Opcode:   sgbucket.FeedOpMutation,
		Flags:    e.flags,
		Expiry:   e.expiry,
		Key:      e.key,
		Value:    e.value,
		DataType: e.datatype,
		Cas:      e.cas,
		VbNo:     e.vbID,
	}
}

type deletionEvent struct {
	streamEventCommon
	seq      uint64
	cas      uint64
	datatype uint8
	key      []byte
	value    []byte
}

func (e deletionEvent) asFeedEvent() sgbucket.FeedEvent {
	return sgbucket.FeedEvent{
		Opcode:   sgbucket.FeedOpDeletion,
		Key:      e.key,
		Value:    e.value,
		DataType: e.datatype,
		Cas:      e.cas,
		VbNo:     e.vbID,
	}
}

type endStreamEvent struct {
	streamEventCommon
	err error
}
