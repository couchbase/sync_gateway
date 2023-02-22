//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"fmt"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/document"
)

// Looks up the raw JSON data of a revision that's been archived to a separate doc.
// If the revision isn't found (e.g. has been deleted by compaction) returns 404 error.
func (c *DatabaseCollection) getOldRevisionJSON(ctx context.Context, docid string, revid string) ([]byte, error) {
	data, _, err := c.dataStore.GetRaw(oldRevisionKey(docid, revid))
	if base.IsDocNotFoundError(err) {
		base.DebugfCtx(ctx, base.KeyCRUD, "No old revision %q / %q", base.UD(docid), revid)
		err = ErrMissing
	}
	if data != nil {
		// Strip out the non-JSON prefix
		if len(data) > 0 && data[0] == document.NonJSONPrefix {
			data = data[1:]
		}
		base.DebugfCtx(ctx, base.KeyCRUD, "Got old revision %q / %q --> %d bytes", base.UD(docid), revid, len(data))
	}
	return data, err
}

// Makes a backup of revision body for use by delta sync, and in-flight replications requesting an old revision.
// Backup policy depends on whether delta sync and/or shared_bucket_access is enabled
//
//	delta=false || delta_rev_max_age_seconds=0
//	   - old revision stored, with expiry OldRevExpirySeconds
//	delta=true && shared_bucket_access=true
//	   - new revision stored (as duplicate), with expiry rev_max_age_seconds
//	delta=true && shared_bucket_access=false
//	   - old revision stored, with expiry rev_max_age_seconds
func (db *DatabaseCollectionWithUser) backupRevisionJSON(ctx context.Context, docId, newRevId, oldRevId string, newBody []byte, oldBody []byte, newAtts AttachmentsMeta) {

	// Without delta sync, store the old rev for in-flight replication purposes
	if !db.deltaSyncEnabled() || db.deltaSyncRevMaxAgeSeconds() == 0 {
		if len(oldBody) > 0 {
			_ = db.setOldRevisionJSON(ctx, docId, oldRevId, oldBody, db.oldRevExpirySeconds())
		}
		return
	}

	// Otherwise, store the revs for delta generation purposes, with a longer expiry

	// Special handling for Xattrs so that SG still has revisions that were updated by an SDK write
	if db.UseXattrs() {
		// Backup the current revision
		var newBodyWithAtts = newBody
		if len(newAtts) > 0 {
			var err error
			newBodyWithAtts, err = base.InjectJSONProperties(newBody, base.KVPair{
				Key: BodyAttachments,
				Val: newAtts,
			})
			if err != nil {
				base.WarnfCtx(ctx, "Unable to marshal new revision body during backupRevisionJSON: doc=%q rev=%q err=%v ", base.UD(docId), newRevId, err)
				return
			}
		}
		_ = db.setOldRevisionJSON(ctx, docId, newRevId, newBodyWithAtts, db.deltaSyncRevMaxAgeSeconds())

		// Refresh the expiry on the previous revision backup
		_ = db.refreshPreviousRevisionBackup(ctx, docId, oldRevId, oldBody, db.deltaSyncRevMaxAgeSeconds())
		return
	}

	// Non-xattr only need to store the previous revision, as all writes come through SG
	if len(oldBody) > 0 {
		_ = db.setOldRevisionJSON(ctx, docId, oldRevId, oldBody, db.deltaSyncRevMaxAgeSeconds())
	}
}

func (db *DatabaseCollectionWithUser) setOldRevisionJSON(ctx context.Context, docid string, revid string, body []byte, expiry uint32) error {

	// Setting the binary flag isn't sufficient to make N1QL ignore the doc - the binary flag is only used by the SDKs.
	// To ensure it's not available via N1QL, need to prefix the raw bytes with non-JSON data.
	// Copying byte slice to make sure we don't modify the version stored in the revcache.
	nonJSONBytes := make([]byte, 1, len(body)+1)
	nonJSONBytes[0] = document.NonJSONPrefix
	nonJSONBytes = append(nonJSONBytes, body...)
	err := db.dataStore.SetRaw(oldRevisionKey(docid, revid), expiry, nil, nonJSONBytes)
	if err == nil {
		base.DebugfCtx(ctx, base.KeyCRUD, "Backed up revision body %q/%q (%d bytes, ttl:%d)", base.UD(docid), revid, len(body), expiry)
	} else {
		base.WarnfCtx(ctx, "setOldRevisionJSON failed: doc=%q rev=%q err=%v", base.UD(docid), revid, err)
	}
	return err
}

// Extends the expiry on a revision backup.  If this fails w/ key not found, will attempt to
// recreate the revision backup when body is non-empty.
func (db *DatabaseCollectionWithUser) refreshPreviousRevisionBackup(ctx context.Context, docid string, revid string, body []byte, expiry uint32) error {

	_, err := db.dataStore.Touch(oldRevisionKey(docid, revid), expiry)
	if base.IsKeyNotFoundError(db.dataStore, err) && len(body) > 0 {
		return db.setOldRevisionJSON(ctx, docid, revid, body, expiry)
	}
	return err
}

// Currently only used by unit tests - deletes an archived old revision from the database
func (c *DatabaseCollection) PurgeOldRevisionJSON(ctx context.Context, docid string, revid string) error {
	base.DebugfCtx(ctx, base.KeyCRUD, "Purging old revision backup %q / %q ", base.UD(docid), revid)
	return c.dataStore.Delete(oldRevisionKey(docid, revid))
}

// ////// UTILITY FUNCTIONS:

func oldRevisionKey(docid string, revid string) string {
	return fmt.Sprintf("%s%s:%d:%s", base.RevPrefix, docid, len(revid), revid)
}
