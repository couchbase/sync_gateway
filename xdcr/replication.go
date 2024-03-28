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
	"fmt"

	sgbucket "github.com/couchbase/sg-bucket"
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

// NewXDCR creates a new XDCR between two buckets.
func NewXDCR(ctx context.Context, fromBucket, toBucket base.Bucket, opts sgbucket.XDCROptions) (sgbucket.XDCR, error) {
	gocbFromBucket, err := base.AsGocbV2Bucket(fromBucket)
	if err != nil {
		rosmarFromBucket, ok := base.GetBaseBucket(fromBucket).(*rosmar.Bucket)
		if !ok {
			return nil, fmt.Errorf("fromBucket must be a *base.GocbV2Bucket or *rosmar.Bucket, got %T", fromBucket)
		}
		rosmarToBucket, ok := base.GetBaseBucket(toBucket).(*rosmar.Bucket)
		if !ok {
			return nil, fmt.Errorf("toBucket must be a *rosmar.Bucket since fromBucket was a rosmar bucket, got %T", toBucket)
		}
		return rosmar.NewXDCR(ctx, rosmarFromBucket, rosmarToBucket, opts)
	}
	gocbToBucket, err := base.AsGocbV2Bucket(toBucket)
	if err != nil {
		return nil, fmt.Errorf("toBucket must be a *base.GocbV2Bucket since fromBucket was a gocbBucket, got %T", toBucket)
	}
	return NewCouchbaseServerXDCR(ctx, gocbFromBucket, gocbToBucket, opts)
}
