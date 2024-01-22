// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

// Package xdcr implements an XDCR between different buckets. This is meant to be used for testing.
package xdcr

import "context"

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
