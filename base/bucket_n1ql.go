/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

const KeyspaceQueryToken = "$_keyspace"           // Token used for keyspace name replacement in query statement. The replacement will be an escaped keyspace.
const KeyspaceQueryAlias = "sgQueryKeyspaceAlias" // Keyspace alias set for the keyspace in FROM statements in queries
const MaxQueryRetries = 30                        // Maximum query retries on indexer error
const IndexStateOnline = "online"                 // bucket state value, as returned by SELECT FROM system:indexes.  Index has been created and built.
const IndexStateDeferred = "deferred"             // bucket state value, as returned by SELECT FROM system:indexes.  Index has been created but not built.
const IndexStatePending = "pending"               // bucket state value, as returned by SELECT FROM system:indexes.  Index has been created, build is in progress
const PrimaryIndexName = "#primary"

// IndexOptions used to build the 'with' clause
type N1qlIndexOptions struct {
	NumReplica      uint `json:"num_replica,omitempty"`          // Number of replicas
	IndexTombstones bool `json:"retain_deleted_xattr,omitempty"` // Whether system xattrs on tombstones should be indexed
	DeferBuild      bool `json:"defer_build,omitempty"`          // Whether to defer initial build of index (requires a subsequent BUILD INDEX invocation)
	NumPartitions   uint `json:"num_partition,omitempty"`        // Number of partitions
}
