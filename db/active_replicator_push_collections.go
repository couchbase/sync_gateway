package db

import (
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
		return err
	}

	if err := apr._initCheckpointer(); err != nil {
		// clean up anything we've opened so far
		base.TracefCtx(apr.ctx, base.KeyReplicate, "Error initialising checkpoint in _connect. Closing everything.")
		apr.checkpointerCtx = nil
		apr.blipSender.Close()
		apr.blipSyncContext.Close()
		return err
	}

	for i, checkpoint := range collectionCheckpoints {
		sinceSeq, err := ParsePlainSequenceID(checkpoint.LastSeq)
		if err != nil {
			return err
		}

		collectionIdx := base.IntPtr(i)
		c, err := apr.blipSyncContext.collections.get(collectionIdx)
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
		if apr.config.FilterChannels != nil {
			channels = base.SetFromArray(apr.config.FilterChannels)
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
				since:             sinceSeq,
				continuous:        apr.config.Continuous,
				activeOnly:        apr.config.ActiveOnly,
				batchSize:         int(apr.config.ChangesBatchSize),
				revocations:       apr.config.PurgeOnRemoval,
				channels:          channels,
				clientType:        clientTypeSGR2,
				ignoreNoConflicts: true, // force the passive side to accept a "changes" message, even in no conflicts mode.
			})
			// On a normal completion, call complete for the replication
			if isComplete {
				apr.Complete()
			}
		}(apr.blipSender)
	}

	return nil
}
