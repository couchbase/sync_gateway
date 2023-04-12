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
	"strings"

	"github.com/couchbase/go-blip"
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

		bh := blipHandler{
			BlipSyncContext: apr.blipSyncContext,
			db:              apr.config.ActiveDB,
			collection:      dbCollectionWithUser,
			collectionIdx:   collectionIdx,
			serialNumber:    apr.blipSyncContext.incrementSerialNumber(),
		}

		var channels base.Set
		if filteredChannels := apr.config.getFilteredChannels(collectionIdx); len(filteredChannels) > 0 {
			channels = base.SetFromArray(filteredChannels)
		}

		apr.blipSyncContext.fatalErrorCallback = func(err error) {
			if strings.Contains(err.Error(), ErrUseProposeChanges.Message) {
				err = ErrUseProposeChanges
				_ = apr.setError(PreHydrogenTargetAllowConflictsError)
				err = apr.stopAndDisconnect()
				if err != nil {
					base.ErrorfCtx(apr.ctx, "Failed to stop and disconnect replication: %v", err)
				}
			} else if strings.Contains(err.Error(), ErrDatabaseWentAway.Message) {
				err = apr.reconnect()
				if err != nil {
					base.ErrorfCtx(apr.ctx, "Failed to reconnect replication: %v", err)
				}
			}
			// No special handling for error
		}
		apr.activeSendChanges.Set(true)
		go func(s *blip.Sender) {
			defer apr.activeSendChanges.Set(false)
			isComplete := bh.sendChanges(s, &sendChangesOptions{
				docIDs:            apr.config.DocIDs,
				since:             replicationCollection.Checkpointer.lastCheckpointSeq,
				continuous:        apr.config.Continuous,
				activeOnly:        apr.config.ActiveOnly,
				batchSize:         int(apr.config.ChangesBatchSize),
				revocations:       apr.config.PurgeOnRemoval,
				channels:          channels,
				clientType:        clientTypeSGR2,
				ignoreNoConflicts: true, // force the passive side to accept a "changes" message, even in no conflicts mode.
				changesCtx:        c.changesCtx,
			})
			// On a normal completion, call complete for the replication
			if isComplete {
				apr.Complete()
			}
		}(apr.blipSender)
		return nil
	})
}
