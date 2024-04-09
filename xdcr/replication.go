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

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/rosmar"
)

// Manager represents a bucket to bucket replication.
type Manager interface {
	// Start starts the replication.
	Start(context.Context) error
	// Stop terminates the replication.
	Stop(context.Context) error
	// Stats returns the stats for the replication.
	Stats(context.Context) (*Stats, error)
}

// MobileSetting represents the presence of "-mobile" flag for Couchbase Server replications. -mobile implies filtering sync metadata, handling version vectors, and filtering sync documents.
type MobileSetting uint8

const (
	MobileOff = iota
	MobileOn
)

// String returns the string representation of the XDCRMobileSetting, used for directly passing on to the Couchbase Server REST API.
func (s MobileSetting) String() string {
	switch s {
	case MobileOff:
		return "Off"
	case MobileOn:
		return "Active"
	default:
		return "Unknown"
	}
}

// XDCROptions represents the options for creating an XDCR.
type XDCROptions struct {
	// FilterExpression is the filter expression to use for the replication.
	FilterExpression string
	// XDCR mobile setting defines whether XDCR replication will use -mobile setting behavior in Couchbase Server.
	Mobile MobileSetting
}

// NewXDCR creates a new XDCR between two buckets.
func NewXDCR(ctx context.Context, fromBucket, toBucket base.Bucket, opts XDCROptions) (Manager, error) {
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
		return newRosmarManager(ctx, rosmarFromBucket, rosmarToBucket, opts)
	}
	gocbToBucket, err := base.AsGocbV2Bucket(toBucket)
	if err != nil {
		return nil, fmt.Errorf("toBucket must be a *base.GocbV2Bucket since fromBucket was a gocbBucket, got %T", toBucket)
	}
	return newCouchbaseServerManager(ctx, gocbFromBucket, gocbToBucket, opts)
}
