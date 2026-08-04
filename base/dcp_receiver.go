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

// Make a feed event for a gomemcached request.  Extracts expiry from extras
func makeFeedEventForMCRequest(rq *gomemcached.MCRequest, opcode sgbucket.FeedOpcode) sgbucket.FeedEvent {
	return makeFeedEvent(rq.Key, rq.Body, rq.DataType, rq.Cas, ExtractExpiryFromDCPMutation(rq), rq.VBucket, 0, 0, opcode)
}
