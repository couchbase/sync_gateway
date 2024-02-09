//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"github.com/couchbase/gomemcached"
	sgbucket "github.com/couchbase/sg-bucket"
)

// Memcached binary protocol datatype bit flags (https://github.com/couchbase/memcached/blob/master/docs/BinaryProtocol.md#data-types),
// used in MCRequest.DataType
const (
	MemcachedDataTypeJSON = 1 << iota
	MemcachedDataTypeSnappy
	MemcachedDataTypeXattr
)

// Memcached datatype for raw (binary) document (non-flag)
const MemcachedDataTypeRaw = 0

// DCPReceiver implements cbdatasource.Receiver to manage updates coming from a
// cbdatasource BucketDataSource.  See go-couchbase/cbdatasource for
// additional details
type DCPReceiver struct {
	*DCPCommon
}

// Make a feed event for a gomemcached request.  Extracts expiry from extras
func makeFeedEventForMCRequest(rq *gomemcached.MCRequest, opcode sgbucket.FeedOpcode) sgbucket.FeedEvent {
	return makeFeedEvent(rq.Key, rq.Body, rq.DataType, rq.Cas, ExtractExpiryFromDCPMutation(rq), rq.VBucket, 0, opcode)
}

// ShardedImportDCPMetadata is an internal struct that is exposed to enable json marshaling, used by sharded import feed. It differs from DCPMetadata because it must match the private struct used by cbgt.metadata.
type ShardedImportDCPMetadata struct {
	FailOverLog [][]uint64 `json:"failOverLog"`
	SeqStart    uint64     `json:"seqStart"`
	SeqEnd      uint64     `json:"seqEnd"`
	SnapStart   uint64     `json:"snapStart"`
	SnapEnd     uint64     `json:"snapEnd"`
}

// Generate cbdatasource's VBucketMetadata for a vbucket from underlying components
func makeVbucketMetadata(vbucketUUID uint64, sequence uint64, snapStart uint64, snapEnd uint64) []byte {
	failOver := make([][]uint64, 1)
	failOverEntry := []uint64{vbucketUUID, 0}
	failOver[0] = failOverEntry
	metadata := &ShardedImportDCPMetadata{
		SeqStart:    sequence,
		SeqEnd:      uint64(0xFFFFFFFFFFFFFFFF),
		SnapStart:   snapStart,
		SnapEnd:     snapEnd,
		FailOverLog: failOver,
	}
	metadataBytes, err := JSONMarshal(metadata)
	if err == nil {
		return metadataBytes
	} else {
		return []byte{}
	}
}

// Create cbdatasource.VBucketMetadata, marshalled to []byte
func makeVbucketMetadataForSequence(vbucketUUID uint64, sequence uint64) []byte {
	return makeVbucketMetadata(vbucketUUID, sequence, sequence, sequence)

}

// DCPLoggingReceiver wraps DCPReceiver to provide per-callback logging
type DCPLoggingReceiver struct {
	rec *DCPReceiver
}

// NoPasswordAuthHandler is used for client cert-based auth by cbdatasource
type NoPasswordAuthHandler struct {
	Handler AuthHandler
}

func (nph NoPasswordAuthHandler) GetCredentials() (username string, password string, bucketname string) {
	_, _, bucketname = nph.Handler.GetCredentials()

	return "", "", bucketname
}
