//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

const (
	// StatsReplication (SGR 1.x)
	StatKeySgrActive                     = "sgr_active"
	StatKeySgrNumAttachmentsTransferred  = "sgr_num_attachments_transferred"
	StatKeySgrAttachmentBytesTransferred = "sgr_num_attachment_bytes_transferred"

	// StatsReplication (SGR 1.x and 2.x)
	StatKeySgrNumDocsPushed       = "sgr_num_docs_pushed"
	StatKeySgrNumDocsFailedToPush = "sgr_num_docs_failed_to_push"
	StatKeySgrDocsCheckedSent     = "sgr_docs_checked_sent"
)

var SyncGatewayStats = *NewSyncGatewayStats()

// Removes the per-database stats for this database by removing the database from the map
func RemovePerDbStats(dbName string) {

	// Clear out the stats for this db since they will no longer be updated.
	SyncGatewayStats.ClearDBStats(dbName)

}
