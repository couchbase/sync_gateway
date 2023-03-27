/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"fmt"

	"github.com/couchbase/sync_gateway/base"
)

// _startPullWithCollections starts a pull replication with collections enabled
// The remote must support collections for this to work which we can detect
// if we got a 404 error back from the GetCollections message
func (apr *ActivePullReplicator) _startPullWithCollections() error {
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

	err = apr.forEachCollection(func(c *activeReplicatorCollection) error {
		since := c.Checkpointer.lastCheckpointSeq.String()
		err = apr._subChanges(base.IntPtr(*c.collectionIdx), since)
		return err
	})

	if err != nil {
		// clean up anything we've opened so far
		base.TracefCtx(apr.ctx, base.KeyReplicate, "cancelling the checkpointer context inside _startPullWithCollections where we send blip request")
		apr.checkpointerCtx = nil
		apr.blipSender.Close()
		apr.blipSyncContext.Close()
		return err
	}

	return nil
}
