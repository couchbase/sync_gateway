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

	"golang.org/x/exp/maps"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/rosmar"
)

// rosmarManager implements a XDCR bucket to bucket replication within rosmar.
type rosmarManager struct {
	filterFunc          xdcrFilterFunc
	terminator          chan bool
	fromBucketKeyspaces map[uint32]string
	toBucketCollections map[uint32]*rosmar.Collection
	fromBucket          *rosmar.Bucket
	fromBucketSourceID  string
	toBucket            *rosmar.Bucket
	replicationID       string
	mobileDocsFiltered  atomic.Uint64
	docsWritten         atomic.Uint64
	errorCount          atomic.Uint64
	targetNewerDocs     atomic.Uint64
}

// newRosmarManager creates an instance of XDCR backed by rosmar. This is not started until Start is called.
func newRosmarManager(ctx context.Context, fromBucket, toBucket *rosmar.Bucket, opts XDCROptions) (Manager, error) {
	if opts.Mobile != MobileOn {
		return nil, errors.New("Only sgbucket.XDCRMobileOn is supported in rosmar")
	}
	fromBucketSourceID, err := GetSourceID(ctx, fromBucket)
	if err != nil {
		return nil, fmt.Errorf("Could not get source ID for %s: %w", fromBucket.GetName(), err)
	}
	return &rosmarManager{
		fromBucket:          fromBucket,
		fromBucketSourceID:  fromBucketSourceID,
		toBucket:            toBucket,
		replicationID:       fmt.Sprintf("%s-%s", fromBucket.GetName(), toBucket.GetName()),
		toBucketCollections: make(map[uint32]*rosmar.Collection),
		fromBucketKeyspaces: make(map[uint32]string),
		terminator:          make(chan bool),
		filterFunc:          mobileXDCRFilter,
	}, nil

}

// processEvent processes a DCP event coming from a toBucket and replicates it to the target datastore.
func (r *rosmarManager) processEvent(ctx context.Context, event sgbucket.FeedEvent) bool {
	docID := string(event.Key)
	base.TracefCtx(ctx, base.KeySGTest, "Got event %s, opcode: %s", docID, event.Opcode)
	col, ok := r.toBucketCollections[event.CollectionID]
	if !ok {
		base.ErrorfCtx(ctx, "This violates the assumption that all collections are mapped to a target collection. This should not happen. Found event=%+v", event)
		r.errorCount.Add(1)
		return false
	}
	ctx = base.CorrelationIDLogCtx(context.Background(), fmt.Sprintf("%s->%s\n\t", r.fromBucketKeyspaces[event.CollectionID], col.GetName())) // use context.Background() to drop test information since context is too long

	switch event.Opcode {
	case sgbucket.FeedOpDeletion, sgbucket.FeedOpMutation:
		// Filter out events if we have a non XDCR filter
		if r.filterFunc != nil && !r.filterFunc(&event) {
			base.TracefCtx(ctx, base.KeySGTest, "Filtering doc %s", docID)
			r.mobileDocsFiltered.Add(1)
			return true
		}

		// Have to use GetWithXattrs to get a cas value back if there are no xattrs (GetWithXattrs will not return a cas if there are no xattrs)
		_, targetXattrs, actualTargetCas, err := col.GetWithXattrs(ctx, docID, []string{base.VvXattrName, base.MouXattrName, base.SyncXattrName})
		if err != nil && !base.IsDocNotFoundError(err) {
			base.WarnfCtx(ctx, "Skipping replicating doc %s, could not perform a kv op get doc in toBucket: %s", event.Key, err)
			r.errorCount.Add(1)
			return false
		}

		sourceHLV, sourceMou, nonMobileXattrs, body, err := processDCPEvent(&event)
		if err != nil {
			base.WarnfCtx(ctx, "Replicating doc %s, could not get body, hlv, and mou: %s", event.Key, err)
			r.errorCount.Add(1)
			return false
		}

		// When doing the evaluation of cas, we want to ignore import mutations, marked with _mou.cas == cas. In that case, we will just use the _vv.cvCAS for conflict resolution. If _mou.cas is present but out of date, continue to use _vv.ver.
		sourceCas := event.Cas
		if sourceMou != nil && base.HexCasToUint64(sourceMou.CAS) == sourceCas && sourceHLV != nil {
			sourceCas = sourceHLV.CurrentVersionCAS
			base.InfofCtx(ctx, base.KeySGTest, "XDCR doc:%s source _mou.cas=cas (%d), using _vv.cvCAS (%d) for conflict resolution", docID, event.Cas, sourceCas)
		}
		targetCas := actualTargetCas
		targetHLV, targetMou, err := getHLVAndMou(targetXattrs)
		if err != nil {
			base.WarnfCtx(ctx, "Replicating doc %s, could not get target hlv and mou: %s", event.Key, err)
			r.errorCount.Add(1)
			return false
		}
		if targetMou != nil && targetHLV != nil {
			// _mou.CAS matches the CAS value, use the _vv.cvCAS for conflict resolution
			if base.HexCasToUint64(targetMou.CAS) == targetCas {
				targetCas = targetHLV.CurrentVersionCAS
				base.InfofCtx(ctx, base.KeySGTest, "XDCR doc:%s target _mou.cas=cas (%d), using _vv.cvCAS (%d) for conflict resolution", docID, targetCas, targetHLV.CurrentVersionCAS)
			}
		}

		/*  full LWW conflict resolution is implemented in rosmar. There is no need to implement this since CAS will always be unique due to rosmar limitations.

		CBS algorithm is, return true when a document should be copied:

		if source.CAS > target.CAS {
			return true
		} else if source.CAS < target.CAS {
			return false
		}
		// Check the RevSeqno
		if source.RevSeqno > target.RevSeqno {
			return true
		} else if source.RevSeqno < target.RevSeqno {
			return false
		}
		// Check the expiry time
		if source.Expiry > target.Expiry {
			return true
		} else if source.Expiry < target.Expiry {
			return false
		}
		// Check flags
		if source.Flags > target.Flags {
			return true
		} else if source.Flags < target.Flags {
			return false
		}
		// Check xattrs
		return source_has_xattrs && !target_has_xattrs

		In the current state of rosmar:

		1. all CAS values are unique.
		2. RevSeqno is not implemented
		3. Expiry is implemented and could be compared except all CAS values are unique.
		4. Flags are not implemented
		5. Presence of xattrs on the source and not the target. (CBG-4334 is not implemented.)

		*/

		if sourceCas <= targetCas {
			base.InfofCtx(ctx, base.KeySGTest, "XDCR doc:%s skipping replication since sourceCas (%d) < targetCas (%d)", docID, sourceCas, targetCas)
			r.targetNewerDocs.Add(1)
			base.TracefCtx(ctx, base.KeySGTest, "Skipping replicating doc %s, cas %d <= %d", docID, event.Cas, targetCas)
			return true
		} /* else if sourceCas == targetCas {
			// CBG-4334, check datatype for targetXattrs to see if there are any xattrs present
			hasSourceXattrs := event.DataType&sgbucket.FeedDataTypeXattr != 0
			hasTargetXattrs := len(targetXattrs) > 0
			if !(hasSourceXattrs && !hasTargetXattrs) {
				base.InfofCtx(ctx, base.KeySGTest, "skipping %q skipping replication since sourceCas (%d) < targetCas (%d)", docID, sourceCas, targetCas)
				return true
			}
		}
		*/
		newXattrs := nonMobileXattrs
		if targetSyncXattr, ok := targetXattrs[base.SyncXattrName]; ok {
			newXattrs[base.SyncXattrName] = targetSyncXattr
		}
		err = updateHLV(newXattrs, sourceHLV, sourceMou, r.fromBucketSourceID, event.Cas)
		if err != nil {
			base.WarnfCtx(ctx, "Replicating doc %s, could not update hlv: %s", event.Key, err)
			r.errorCount.Add(1)
			return false
		}
		base.InfofCtx(ctx, base.KeySGTest, "Replicating doc %q, with cas (%d), body %s, xattrsKeys: %+v", event.Key, event.Cas, string(body), maps.Keys(newXattrs))
		err = opWithMeta(ctx, col, actualTargetCas, newXattrs, body, &event)
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
			r.fromBucketKeyspaces[collectionID] = fromDataStore.GetName()
			scopes[fromName.ScopeName()] = append(scopes[fromName.ScopeName()], fromName.CollectionName())
			break
		}
	}

	args := sgbucket.FeedArguments{
		ID:         "xdcr-" + r.replicationID,
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

// opWithMeta writes a document to the target datastore given a type of Deletion or Mutation event with a specific cas, xattrs, and body.
func opWithMeta(ctx context.Context, collection *rosmar.Collection, originalCas uint64, xattrs map[string][]byte, body []byte, event *sgbucket.FeedEvent) error {
	xattrBytes, err := xattrToBytes(xattrs)
	if err != nil {
		return err
	}

	if event.Opcode == sgbucket.FeedOpDeletion {
		return collection.DeleteWithMeta(ctx, string(event.Key), originalCas, event.Cas, event.Expiry, xattrBytes)
	}

	return collection.SetWithMeta(ctx, string(event.Key), originalCas, event.Cas, event.Expiry, xattrBytes, body, event.DataType)

}

// Stats returns the stats of the XDCR replication.
func (r *rosmarManager) Stats(context.Context) (*Stats, error) {
	stats := &Stats{
		DocsWritten:        r.docsWritten.Load(),
		MobileDocsFiltered: r.mobileDocsFiltered.Load(),
		ErrorCount:         r.errorCount.Load(),
		TargetNewerDocs:    r.targetNewerDocs.Load(),
	}
	stats.DocsProcessed = stats.DocsWritten + stats.MobileDocsFiltered + stats.TargetNewerDocs
	return stats, nil
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

// processDCPEvent gets the body, non mobile, xattrs, vv, and mou from the event.
func processDCPEvent(event *sgbucket.FeedEvent) (*db.HybridLogicalVector, *db.MetadataOnlyUpdate, map[string][]byte, []byte, error) {
	if event.DataType&sgbucket.FeedDataTypeXattr == 0 {
		xattrs := make(map[string][]byte)
		return nil, nil, xattrs, event.Value, nil
	}
	body, xattrs, err := sgbucket.DecodeValueWithAllXattrs(event.Value)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if xattrs == nil {
		xattrs = make(map[string][]byte)
	}
	hlv, mou, err := getHLVAndMou(xattrs)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for _, xattrName := range []string{base.VvXattrName, base.MouXattrName, base.SyncXattrName} {
		delete(xattrs, xattrName)
	}
	return hlv, mou, xattrs, body, nil
}

// getHLVAndMou gets the hlv and mou from the xattrs.
func getHLVAndMou(xattrs map[string][]byte) (*db.HybridLogicalVector, *db.MetadataOnlyUpdate, error) {
	var hlv *db.HybridLogicalVector
	if bytes, ok := xattrs[base.VvXattrName]; ok {
		err := json.Unmarshal(bytes, &hlv)
		if err != nil {
			return nil, nil, fmt.Errorf("Could not unmarshal the vv xattr %s: %w", string(bytes), err)
		}
	}
	var mou *db.MetadataOnlyUpdate
	if bytes, ok := xattrs[base.MouXattrName]; ok {
		err := json.Unmarshal(bytes, &mou)
		if err != nil {
			return nil, nil, fmt.Errorf("Could not unmarshal the mou xattr %s: %w", string(bytes), err)
		}
	}
	return hlv, mou, nil
}

func updateHLV(xattrs map[string][]byte, sourceHLV *db.HybridLogicalVector, sourceMou *db.MetadataOnlyUpdate, sourceID string, sourceCas uint64) error {
	var targetHLV *db.HybridLogicalVector
	if sourceHLV != nil {
		// TODO: read existing targetXattrs[base.VvXattrName] and update the pv CBG-4250
		targetHLV = sourceHLV
	} else {
		hlv := db.NewHybridLogicalVector()
		err := hlv.AddVersion(db.Version{
			SourceID: sourceID,
			Value:    sourceCas,
		})
		if err != nil {
			return err
		}
		hlv.CurrentVersionCAS = sourceCas
		targetHLV = &hlv
	}
	var err error
	xattrs[base.VvXattrName], err = json.Marshal(targetHLV)
	if err != nil {
		return err
	}
	if sourceMou != nil {
		var err error
		xattrs[base.MouXattrName], err = json.Marshal(sourceMou)
		if err != nil {
			return err
		}
	}
	return nil
}
