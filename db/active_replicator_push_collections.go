// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"fmt"

	"github.com/couchbase/sync_gateway/base"
)

// _startPullWithCollections starts a push replication with collections enabled
// The remote must support collections for this to work which we can detect
// if we got a 404 error back from the GetCollections message
func (apr *ActivePushReplicator) _startPushWithCollections() error {
	collectionCheckpoints, err := apr._initCollections()
	if err != nil {
		return fmt.Errorf("%w: %s", fatalReplicatorConnectError, err)

	}

	if err := apr._initCheckpointer(collectionCheckpoints); err != nil {
		// clean up anything we've opened so far
		base.TracefCtx(apr.ctx, base.KeyReplicate, "Error initialising checkpoint in _connect. Closing everything.")
		apr.checkpointerCtx = nil
		apr.blipSender.Close()
		apr.blipSyncContext.Close()
		return err
	}

	return apr.forEachCollection(func(replicationCollection *activeReplicatorCollection) error {
		collectionIdx := replicationCollection.collectionIdx
		c, err := apr.blipSyncContext.collections.get(replicationCollection.collectionIdx)
		if err != nil {
			return err
		}

		dbCollectionWithUser := &DatabaseCollectionWithUser{
			DatabaseCollection: c.dbCollection,
			user:               apr.config.ActiveDB.user,
		}

		bh := newBlipHandler(apr.ctx, apr.blipSyncContext, apr.config.ActiveDB, apr.blipSyncContext.incrementSerialNumber())
		bh.collection = dbCollectionWithUser
		bh.collectionIdx = collectionIdx
		bh.loggingCtx = bh.collection.AddCollectionContext(bh.BlipSyncContext.loggingCtx)

		return apr._startSendingChanges(bh, replicationCollection.Checkpointer.lastCheckpointSeq)
	})
}
