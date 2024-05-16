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
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/rosmar"
)

// rosmarManager implements a XDCR bucket to bucket replication within rosmar.
type rosmarManager struct {
	filterFunc          xdcrFilterFunc
	terminator          chan bool
	toBucketCollections map[uint32]*rosmar.Collection
	fromBucket          *rosmar.Bucket
	toBucket            *rosmar.Bucket
	replicationID       string
	docsFiltered        atomic.Uint64
	docsWritten         atomic.Uint64
	errorCount          atomic.Uint64
	targetNewerDocs     atomic.Uint64
}

// newRosmarManager creates an instance of XDCR backed by rosmar. This is not started until Start is called.
func newRosmarManager(_ context.Context, fromBucket, toBucket *rosmar.Bucket, opts XDCROptions) (Manager, error) {
	if opts.Mobile != MobileOn {
		return nil, errors.New("Only sgbucket.XDCRMobileOn is supported in rosmar")
	}
	return &rosmarManager{
		fromBucket:          fromBucket,
		toBucket:            toBucket,
		replicationID:       fmt.Sprintf("%s-%s", fromBucket.GetName(), toBucket.GetName()),
		toBucketCollections: make(map[uint32]*rosmar.Collection),
		terminator:          make(chan bool),
		filterFunc:          mobileXDCRFilter,
	}, nil

}

// processEvent processes a DCP event coming from a toBucket and replicates it to the target datastore.
func (r *rosmarManager) processEvent(ctx context.Context, event sgbucket.FeedEvent) bool {
	docID := string(event.Key)
	base.TracefCtx(ctx, base.KeyWalrus, "Got event %s, opcode: %s", docID, event.Opcode)
	col, ok := r.toBucketCollections[event.CollectionID]
	if !ok {
		base.ErrorfCtx(ctx, "This violates the assumption that all collections are mapped to a target collection. This should not happen. Found event=%+v", event)
		r.errorCount.Add(1)
		return false
	}

	switch event.Opcode {
	case sgbucket.FeedOpDeletion, sgbucket.FeedOpMutation:
		// Filter out events if we have a non XDCR filter
		if r.filterFunc != nil && !r.filterFunc(&event) {
			base.TracefCtx(ctx, base.KeyWalrus, "Filtering doc %s", docID)
			r.docsFiltered.Add(1)
			return true
		}

		toCas, err := col.Get(docID, nil)
		if err != nil && !col.IsError(err, sgbucket.KeyNotFoundError) {
			base.WarnfCtx(ctx, "Skipping replicating doc %s, could not perform a kv op get doc in toBucket: %s", event.Key, err)
			r.errorCount.Add(1)
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

		if event.Cas <= toCas {
			r.targetNewerDocs.Add(1)
			base.TracefCtx(ctx, base.KeyWalrus, "Skipping replicating doc %s, cas %d <= %d", docID, event.Cas, toCas)
			return true
		}

		err = opWithMeta(ctx, col, toCas, event)
		if err != nil {
			base.WarnfCtx(ctx, "Replicating doc %s, could not write doc: %s", event.Key, err)
			r.errorCount.Add(1)
			return false

		}
		r.docsWritten.Add(1)
	}

	return true

}

// Start starts the replication for all existing replications. Errors if there aren't corresponding named collections on each bucket.
func (r *rosmarManager) Start(ctx context.Context) error {
	// set up replication to target all existing collections, and map to other collections
	scopes := make(map[string][]string)
	fromDataStores, err := r.fromBucket.ListDataStores()
	if err != nil {
		return fmt.Errorf("Could not list data stores: %w", err)
	}
	toDataStores, err := r.toBucket.ListDataStores()
	if err != nil {
		return fmt.Errorf("Could not list toBucket data stores: %w", err)
	}
	for _, fromName := range fromDataStores {
		fromDataStore, err := r.fromBucket.NamedDataStore(fromName)
		if err != nil {
			return fmt.Errorf("Could not get data store %s: %w when starting XDCR", fromName, err)
		}
		collectionID := fromDataStore.GetCollectionID()
		for _, toName := range toDataStores {
			if fromName.ScopeName() != toName.ScopeName() || fromName.CollectionName() != toName.CollectionName() {
				continue
			}
			toDataStore, err := r.toBucket.NamedDataStore(toName)
			if err != nil {
				return fmt.Errorf("There is not a matching datastore name in the toBucket for the fromBucket %s", toName)
			}
			col, ok := toDataStore.(*rosmar.Collection)
			if !ok {
				return fmt.Errorf("DataStore %s is not of rosmar.Collection: %T", toDataStore, toDataStore)
			}
			r.toBucketCollections[collectionID] = col
			scopes[fromName.ScopeName()] = append(scopes[fromName.ScopeName()], fromName.CollectionName())
			break
		}
	}

	args := sgbucket.FeedArguments{
		ID:         "xdcr-" + r.replicationID,
		Backfill:   sgbucket.FeedNoBackfill,
		Terminator: r.terminator,
		Scopes:     scopes,
	}

	callback := func(event sgbucket.FeedEvent) bool {
		return r.processEvent(ctx, event)
	}

	return r.fromBucket.StartDCPFeed(ctx, args, callback, nil)
}

// Stop terminates the replication.
func (r *rosmarManager) Stop(_ context.Context) error {
	close(r.terminator)
	r.terminator = nil
	return nil
}

// opWithMeta writes a document to the target datastore given a type of Deletion or Mutation event with a specific cas.
func opWithMeta(ctx context.Context, collection *rosmar.Collection, originalCas uint64, event sgbucket.FeedEvent) error {
	var xattrs []byte
	var body []byte
	if event.DataType&sgbucket.FeedDataTypeXattr != 0 {
		var err error
		var dcpXattrs map[string][]byte
		body, dcpXattrs, err = sgbucket.DecodeValueWithAllXattrs(event.Value)
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

	if event.Opcode == sgbucket.FeedOpDeletion {
		return collection.DeleteWithMeta(ctx, string(event.Key), originalCas, event.Cas, event.Expiry, xattrs)
	}

	return collection.SetWithMeta(ctx, string(event.Key), originalCas, event.Cas, event.Expiry, xattrs, body, event.DataType)

}

// Stats returns the stats of the XDCR replication.
func (r *rosmarManager) Stats(context.Context) (*Stats, error) {

	return &Stats{
		DocsWritten:     r.docsWritten.Load(),
		DocsFiltered:    r.docsFiltered.Load(),
		ErrorCount:      r.errorCount.Load(),
		TargetNewerDocs: r.targetNewerDocs.Load(),
	}, nil
}

// xattrToBytes converts a map of xattrs of marshalled json.
func xattrToBytes(xattrs map[string][]byte) ([]byte, error) {
	xattrMap := make(map[string]json.RawMessage)
	for k, v := range xattrs {
		xattrMap[k] = v
	}
	return json.Marshal(xattrMap)
}

// xdcrFilterFunc is a function that filters out events from the replication.
type xdcrFilterFunc func(event *sgbucket.FeedEvent) bool

// mobileXDCRFilter is the implicit key filtering function that Couchbase Server -mobile XDCR works on.
func mobileXDCRFilter(event *sgbucket.FeedEvent) bool {
	return !(strings.HasPrefix(string(event.Key), base.SyncDocPrefix) && !strings.HasPrefix(string(event.Key), base.Att2Prefix))
}
