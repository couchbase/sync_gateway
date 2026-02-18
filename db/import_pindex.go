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

// registerPindexImplMutex locks access to cbgt.RegisterPindexImpl.
var registerPindexImplMutex = sync.Mutex{}

// RegisterPindexImpl registers the PIndex type definition.  This is invoked by cbgt when a Pindex (collection of
// vbuckets) is assigned to this node.
func RegisterPindexImpl(ctx context.Context, configGroup string) {
	registerPindexImplMutex.Lock()
	defer registerPindexImplMutex.Unlock()

	// Since RegisterPIndexImplType is a global var without synchronization, index type needs to be
	// config group scoped.  The associated importListener within the context is retrieved based on the
	// dbname in the index params
	for _, pIndexName := range []string{base.CBGTIndexTypeSyncGatewayImport, base.CBGTIndexTypeSyncGatewayResync} {
		pIndexType := pIndexName + configGroup
		base.InfofCtx(ctx, base.KeyDCP, "Registering PindexImplType for %s", pIndexType)
		cbgt.RegisterPIndexImplType(pIndexType,
			&cbgt.PIndexImplType{
				New:       getNewPIndexImplType(ctx),
				Open:      OpenPIndexImpl,
				OpenUsing: getOpenPIndexImplUsing(ctx),
				Description: "general/syncGateway " +
					" - import processing and resync for shared bucket access",
			})
	}
}

// getListenerForIndex looks up the Dest instance for a given set of cbgt index parameters.
func getListenerDest(ctx context.Context, indexParams string, restart func()) (cbgt.Dest, error) {

	var outerParams struct {
		Params string `json:"params"`
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

	base.DebugfCtx(ctx, base.KeyDCP, "Fetching dest for %v", base.MD(sgIndexParams.DestKey))
	destFactory, fetchErr := base.FetchDestFactory(sgIndexParams.DestKey)
	if fetchErr != nil {
		return nil, fmt.Errorf("error retrieving dest for indexParams %v: %v", indexParams, fetchErr)
	}
	return destFactory(restart)
}

func getNewPIndexImplType(ctx context.Context) func(indexType, indexParams, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	// NewImportPIndexImpl is called when the node is first added to the cbgt cfg.  On a node restart,
	// OpenImportindexImplUsing is called.
	newPIndexImpl := func(indexType, indexParams, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
		defer base.FatalPanicHandler()

		dest, err := getListenerDest(ctx, indexParams, restart)
		if err != nil {
			// This error can occur when a stale index definition hasn't yet been removed from the plan (e.g. on update to db config)
			base.InfofCtx(ctx, base.KeyDCP, "No dest found for indexParams - usually an obsolete index pending removal. %v", err)
		}
		return nil, dest, err
	}
	return newPIndexImpl
}

// OpenPIndexImpl is required to have an implementation from cbgt.PIndexImplType.Open. When this function fails, PIndexImplType will fall back to using PIndexImplType.OpenUsing
func OpenPIndexImpl(indexType, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	return nil, nil, errors.New("Open PIndexImpl not supported for SG 3.0 databases - must provide index params")
}

func getOpenPIndexImplUsing(ctx context.Context) func(indexType, indexParams, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {

	openImportPIndexImplUsing := func(indexType, path, indexParams string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
		importDest, err := getListenerDest(ctx, indexParams, restart)
		return nil, importDest, err
	}
	return openImportPIndexImplUsing
}

// NewImportDest returns a cbgt.Dest targeting the importListener's ProcessFeedEvent
func (il *importListener) NewImportDest(janitorRollback func()) (cbgt.Dest, error) {
	callback := il.ProcessFeedEvent

	maxVbNo, err := il.bucket.GetMaxVbno() // can safely assume that all collections on the same bucket will have the same vbNo
	if err != nil {
		return nil, err
	}

	importFeedStatsMap := il.dbStats.ImportFeedMapStats
	importPartitionStat := il.importStats.ImportPartitions

	importDest, _, err := base.NewDCPDest(il.loggingCtx, callback, il.bucket, maxVbNo, true, importFeedStatsMap.Map, base.DCPImportFeedID, importPartitionStat, il.checkpointPrefix, il.metadataKeys)
	if err != nil {
		return nil, err
	}

	return importDest, nil
}
