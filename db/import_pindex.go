/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/base"
)

// registerImportPindexImplMutex locks access to cbgt.RegisterImportPindexImpl.
var registerImportPindexImplMutex = sync.Mutex{}

// RegisterImportPindexImpl registers the PIndex type definition.  This is invoked by cbgt when a Pindex (collection of
// vbuckets) is assigned to this node.
func RegisterImportPindexImpl(ctx context.Context, configGroup string) {
	registerImportPindexImplMutex.Lock()
	defer registerImportPindexImplMutex.Unlock()

	// Since RegisterPIndexImplType is a global var without synchronization, index type needs to be
	// config group scoped.  The associated importListener within the context is retrieved based on the
	// dbname in the index params
	pIndexType := base.CBGTIndexTypeSyncGatewayImport + configGroup
	base.InfofCtx(ctx, base.KeyDCP, "Registering PindexImplType for %s", pIndexType)
	cbgt.RegisterPIndexImplType(pIndexType,
		&cbgt.PIndexImplType{
			New:       getNewPIndexImplType(ctx),
			Open:      OpenImportPIndexImpl,
			OpenUsing: getOpenImportPIndexImplUsing(ctx),
			Description: "general/syncGateway-import " +
				" - import processing for shared bucket access",
		})
}

// getListenerForIndex looks up the importListener for the dbName specified in the index params
func getListenerImportDest(ctx context.Context, indexParams string, restart func()) (cbgt.Dest, error) {

	var outerParams struct {
		Params    string `json:"params"`
		IndexName string `json:"indexName"`
	}
	err := base.JSONUnmarshal([]byte(indexParams), &outerParams)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling cbgt index params outer: %w", err)
	}

	var sgIndexParams base.SGFeedIndexParams
	err = base.JSONUnmarshal([]byte(outerParams.Params), &sgIndexParams)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling dbname from cbgt index params: %w", err)
	}

	base.DebugfCtx(ctx, base.KeyDCP, "Fetching listener import dest for %v", base.MD(sgIndexParams.DestKey))
	destFactory, fetchErr := base.FetchDestFactory(sgIndexParams.DestKey)
	if fetchErr != nil {
		return nil, fmt.Errorf("error retrieving listener for indexParams %v: %v", indexParams, fetchErr)
	}
	return destFactory(outerParams.IndexName, restart)
}

func getNewPIndexImplType(ctx context.Context) func(indexType, indexParams, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	// NewImportPIndexImpl is called when the node is first added to the cbgt cfg.  On a node restart,
	// OpenImportPindexImplUsing is called.
	newImportPIndexImpl := func(indexType, indexParams, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
		defer base.FatalPanicHandler()

		importDest, err := getListenerImportDest(ctx, indexParams, restart)
		if err != nil {
			// This error can occur when a stale index definition hasn't yet been removed from the plan (e.g. on update to db config)
			base.InfofCtx(ctx, base.KeyDCP, "No importDest found for indexParams - usually an obsolete index pending removal. %v", err)
		}
		return nil, importDest, err
	}
	return newImportPIndexImpl
}

// OpenImportPIndexImpl is required to have an implementation from cbgt.PIndexImplType.Open. When this function fails, PIndexImplType will fall back to using PIndexImplType.OpenUsing
func OpenImportPIndexImpl(indexType, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	return nil, nil, errors.New("Open PIndexImpl not supported for SG 3.0 databases - must provide index params")
}

func getOpenImportPIndexImplUsing(ctx context.Context) func(indexType, indexParams, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {

	openImportPIndexImplUsing := func(indexType, path, indexParams string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
		importDest, err := getListenerImportDest(ctx, indexParams, restart)
		return nil, importDest, err
	}
	return openImportPIndexImplUsing
}

// NewImportDest returns a cbgt.Dest targeting the importListener's ProcessFeedEvent
func (il *importListener) NewImportDest(indexName string, janitorRollback func()) (cbgt.Dest, error) {
	if indexName == "" {
		return nil, errors.New("NewImportDest called with empty indexName")
	}
	bucket, err := base.AsGocbV2Bucket(il.bucket)
	if err != nil {
		return nil, base.RedactErrorf("Bucket %s in NewImportDest must be a gocb bucket, was %T", base.MD(il.bucket), il.bucket)
	}
	importDest, _, err := base.NewDCPDest(il.loggingCtx, base.DCPDestOptions{
		Callback:            il.ProcessFeedEvent,
		Bucket:              bucket,
		PersistCheckpoints:  true,
		DCPStats:            il.dbStats.ImportFeedMapStats.Map,
		FeedID:              base.DCPImportFeedID,
		PIndexName:          indexName,
		ImportPartitionStat: il.importStats.ImportPartitions,
		CheckpointPrefix:    il.checkpointPrefix,
		MetadataStore:       il.metadataStore,
		MetadataKeys:        il.metadataKeys,
		Rollback:            janitorRollback,
		CbgtManager:         il.cbgtManager,
	})

	if err != nil {
		return nil, err
	}
	return importDest, nil
}
