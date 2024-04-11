// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package xdcr

// Stats represents the stats of a replication.
type Stats struct {
	// DocsFiltered is the number of documents that have been filtered out and have not been replicated to the target cluster.
	DocsFiltered uint64
	// DocsWritten is the number of documents written to the destination cluster, since the start or resumption of the current replication.
	DocsWritten uint64
	// ErrorCount is the number of errors that have occurred during the replication.
	ErrorCount uint64

	// TargetNewerDocs is the number of documents that were newer on the target cluster than the source cluster.
	TargetNewerDocs uint64
}
