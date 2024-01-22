// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

// Package xdcr implements an XDCR between different buckets. This is meant to be used for testing.
package xdcr

import (
	"context"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/rosmar"
)

// Replication represents a bucket to bucket replication.
type Replication interface {
	// Start starts the replication.
	Start(context.Context) error
	// Stop terminates the replication.
	Stop(context.Context) error
	// Stats returns the stats for the replication.
	Stats(context.Context) (*Stats, error)
}

var _ Replication = &CouchbaseServerXDCR{}

// NewReplication creates an instance of XDCR. This is not started until Start is called.
func NewReplication(ctx context.Context, fromBucket, toBucket base.Bucket) (Replication, error) {
	rosmarFromBucket, _ := base.GetBaseBucket(fromBucket).(*rosmar.Bucket)
	rosmarToBucket, _ := base.GetBaseBucket(toBucket).(*rosmar.Bucket)
	if rosmarFromBucket != nil && rosmarToBucket != nil {
		return NewRosmarXDCR(ctx, rosmarFromBucket, rosmarToBucket)
	}
	gocbFromBucket, err := base.AsGocbV2Bucket(fromBucket)
	if err != nil {
		return nil, err
	}
	gocbToBucket, err := base.AsGocbV2Bucket(toBucket)
	if err != nil {
		return nil, err
	}
	return NewCouchbaseServerXDCR(ctx, gocbFromBucket, gocbToBucket)
}
