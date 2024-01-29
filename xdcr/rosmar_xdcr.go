// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package xdcr

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/rosmar"
)

// RosmarXDCR implements a XDCR setup on rosmar.
type RosmarXDCR struct {
	fromBucketCollectionIDs map[uint32]sgbucket.DataStoreNameImpl
	fromBucket              base.Bucket
	toBucket                base.Bucket
	replicationID           string
	docsWritten             atomic.Uint64
	docsFiltered            atomic.Uint64
}

// NewRosmarXDCR creates an instance of XDCR backed by rosmar. This is not started until Start is called.
func NewRosmarXDCR(_ context.Context, fromBucket, toBucket *rosmar.Bucket) (*RosmarXDCR, error) {
	return &RosmarXDCR{
		fromBucket:              fromBucket,
		toBucket:                toBucket,
		replicationID:           fmt.Sprintf("%s-%s", fromBucket.GetName(), toBucket.GetName()),
		fromBucketCollectionIDs: map[uint32]sgbucket.DataStoreNameImpl{},
	}, nil
}

func (r *RosmarXDCR) getFromBucketCollectionName(collectionID uint32) (*base.ScopeAndCollectionName, error) {
	dsName, ok := r.fromBucketCollectionIDs[collectionID]
	if ok {
		return &dsName, nil
	}
	dataStores, err := r.fromBucket.ListDataStores()
	if err != nil {
		return nil, fmt.Errorf("Could not list data stores: %w", err)
	}
	for _, dsName := range dataStores {
		dataStore, err := r.fromBucket.NamedDataStore(dsName)
		if err != nil {
			return nil, fmt.Errorf("Could not get data store %s: %w", dsName, err)
		}
		if base.GetCollectionID(dataStore) == collectionID {
			nameImpl, ok := dsName.(base.ScopeAndCollectionName)
			if !ok {
				return nil, fmt.Errorf("Could not cast %s to base.ScopeAndCollectionName", dsName)
			}
			r.fromBucketCollectionIDs[collectionID] = nameImpl
			return &nameImpl, nil
		}
	}
	return nil, fmt.Errorf("Could not find collection with ID %d", collectionID)
}

// Start starts the replication.
func (r *RosmarXDCR) Start(ctx context.Context) error {
	args := sgbucket.FeedArguments{
		ID:       "xdcr-" + r.replicationID,
		Backfill: sgbucket.FeedNoBackfill,
	}
	callback := func(event sgbucket.FeedEvent) bool {
		docID := string(event.Key)
		base.TracefCtx(ctx, base.KeyWalrus, "Got event %s, opcode: %s", base.MD(docID), event.Opcode)
		dsName, err := r.getFromBucketCollectionName(event.CollectionID)
		if err != nil {
			base.WarnfCtx(ctx, "Could not find collection with ID %d for docID %s: %s", event.CollectionID, base.MD(docID), err)
			return false
		}
		switch event.Opcode {
		case sgbucket.FeedOpDeletion, sgbucket.FeedOpMutation:
			if strings.HasPrefix(docID, base.SyncDocPrefix) && !strings.HasPrefix(docID, base.Att2Prefix) {
				base.TracefCtx(ctx, base.KeyWalrus, "Filtering doc %s", base.MD(docID))
				r.docsFiltered.Add(1)
				return true
			}
			toDataStore, err := r.toBucket.NamedDataStore(dsName)
			if err != nil {
				base.WarnfCtx(ctx, "Replicating doc %s, could not find matching datastore for %s in target bucket", event.Key, dsName)
				return false
			}
			originalCas, err := toDataStore.Get(docID, nil)
			if err != nil && !base.IsKeyNotFoundError(toDataStore, err) {
				base.WarnfCtx(ctx, "Skipping replicating doc %s, could not before a kv op get doc in toBucket: %s", event.Key, err)
				return false
			}
			/* full LWW conflict resolution is not implemented in rosmar yet
			CBS algorithm is:

			if (command.CAS > document.CAS)
			  command succeeds
			else if (command.CAS == document.CAS)
			  // Check the RevSeqno
			  if (command.RevSeqno > document.RevSeqno)
			    command succeeds
			  else if (command.RevSeqno == document.RevSeqno)
			    // Check the expiry time
			    if (command.Expiry > document.Expiry)
			      command succeeds
			    else if (command.Expiry == document.Expiry)
			      // Finally check flags
			      if (command.Flags < document.Flags)
			        command succeeds

			command fails

			In the current state of rosmar:

			1. all CAS values are unique.
			2. RevSeqno is not implemented
			3. Expiry is implemented and could be compared except all CAS values are unique.
			4. Flags are not implemented
			*/
			if event.Cas <= originalCas {
				base.TracefCtx(ctx, base.KeyWalrus, "Skipping replicating doc %s, cas %d <= %d", base.MD(docID), event.Cas, originalCas)
				return true
			}
			err = writeDoc(ctx, toDataStore, originalCas, event)
			if err != nil {
				base.WarnfCtx(ctx, "Replicating doc %s, could not write doc: %s", event.Key, err)
				return false
			}
			r.docsWritten.Add(1)
		}

		return true
	}
	return r.fromBucket.StartDCPFeed(ctx, args, callback, nil)
}

// Stop terminates the replication.
func (r *RosmarXDCR) Stop(_ context.Context) error {
	return nil
}

// writeDoc writes a document to the target datastore. This will not return an error on a CAS mismatch, but will return error on other types of write.
func writeDoc(ctx context.Context, datastore base.DataStore, originalCas uint64, event sgbucket.FeedEvent) error {
	collection, ok := base.GetBaseDataStore(datastore).(*rosmar.Collection)
	if !ok {
		base.WarnfCtx(ctx, "Could not cast datastore %T to rosmar.Collection", datastore)
		return fmt.Errorf("Could not cast datastore %T to rosmar.Collection", datastore)
	}
	if event.Opcode == sgbucket.FeedOpDeletion {
		_, err := collection.Remove(string(event.Key), originalCas)
		if !base.IsCasMismatch(err) {
			return err
		}
		return nil
	}
	var xattrs []byte
	var body []byte
	if event.DataType&sgbucket.FeedDataTypeXattr != 0 {
		var err error
		var dcpXattrs []sgbucket.Xattr
		body, dcpXattrs, err = sgbucket.DecodeValueWithXattrs(event.Value)
		if err != nil {
			return err
		}
		xattrs, err = xattrToBytes(dcpXattrs)
		if err != nil {
			return err
		}
	} else {
		body = event.Value
	}
	err := collection.SetWithMeta(ctx, string(event.Key), originalCas, event.Cas, event.Expiry, xattrs, body, event.DataType)
	if !base.IsCasMismatch(err) {
		return err
	}
	return nil
}

// Stats returns the stats of the XDCR replication.
func (r *RosmarXDCR) Stats(context.Context) (*Stats, error) {
	return &Stats{
		DocsWritten:  r.docsWritten.Load(),
		DocsFiltered: r.docsFiltered.Load()}, nil
}

// xattrToBytes converts a slice of Xattrs to a byte slice of marshalled json.
func xattrToBytes(xattrs []sgbucket.Xattr) ([]byte, error) {
	xattrMap := make(map[string]json.RawMessage)
	for _, xattr := range xattrs {
		xattrMap[xattr.Name] = xattr.Value
	}
	return base.JSONMarshal(xattrMap)
}
