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

// registerPindexImplMutex serializes access to cbgt.RegisterPIndexImplType, which uses global state without its own synchronization.
var registerPindexImplMutex = sync.Mutex{}

// RegisterPindexImpl registers the PIndex type definition.  This is invoked by cbgt when a Pindex (collection of
// vbuckets) is assigned to this node.
func RegisterPindexImpl(ctx context.Context, configGroup string) {
	registerPindexImplMutex.Lock()
	defer registerPindexImplMutex.Unlock()

	// Since RegisterPIndexImplType is a global var without synchronization, index type needs to be
	// config group scoped, only for import indexes as import can be enabled only for a config group.
	// Resync Indexes do not require config group, as the resync will be distributed across all nodes and is not config
	// specific.
	for _, pIndexType := range []string{base.CBGTIndexTypeSyncGatewayImport + configGroup, base.CBGTIndexTypeSyncGatewayResync} {
		base.InfofCtx(ctx, base.KeyDCP, "Registering PindexImplType for %s", pIndexType)
		var desc string
		if pIndexType == base.CBGTIndexTypeSyncGatewayResync {
			desc = "general/syncGateway-resync - distributed resync"
		} else {
			desc = "general/syncGateway-import - import processing for shared bucket access"
		}
		cbgt.RegisterPIndexImplType(pIndexType,
			&cbgt.PIndexImplType{
				New:         getUnsupportedNewPIndexImpl(ctx),
				NewEx:       getNewExPIndexImpl(ctx),
				Rollback:    getNewExPIndexImpl(ctx),
				Open:        getUnsupportedOpenPIndexImpl(ctx),
				OpenEx:      getUnsupportedOpenExPIndexImpl(ctx),
				Description: desc,
			})
	}
}

// getCbgtDest creates the cbgt.Dest that mgr registered for the destKey specified in indexParams.
func getCbgtDest(ctx context.Context, mgr *cbgt.Manager, indexParams string, restart func()) (cbgt.Dest, error) {

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
		return nil, fmt.Errorf("error unmarshalling SGFeedIndexParams from cbgt indexParams.params %s: %w", base.MD(outerParams.Params), err)
	}

	base.DebugfCtx(ctx, base.KeyDCP, "Fetching dest for %v", base.MD(sgIndexParams.DestKey))
	destFactory, fetchErr := base.FetchCbgtDestFactory(mgr, sgIndexParams.DestKey)
	if fetchErr != nil {
		return nil, fmt.Errorf("error retrieving listener for indexParams %v: %v", indexParams, fetchErr)
	}
	return destFactory(restart)
}

// getNewExPIndexImpl creates the cbgt.Dest for a pindex assigned to mgr, using the destKey in indexParams set by Sync Gateway.
func getNewExPIndexImpl(ctx context.Context) func(indexType, indexParams, sourceParams, path string, mgr *cbgt.Manager, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	return func(indexType, indexParams, sourceParams, path string, mgr *cbgt.Manager, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
		defer base.FatalPanicHandler(ctx)

		dest, err := getCbgtDest(ctx, mgr, indexParams, restart)
		if err != nil {
			// This error can occur when a stale index definition hasn't yet been removed from the plan (e.g. on update to db config)
			base.DebugfCtx(ctx, base.KeyDCP, "No dest found for indexParams - usually an obsolete index pending removal. %v", err)
		}
		return nil, dest, err
	}
}

// getUnsupportedNewPIndexImpl rejects PIndexImplType.New, which has no cbgt.Manager to look up the dest with.
func getUnsupportedNewPIndexImpl(ctx context.Context) func(indexType, indexParams, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	return func(indexType, indexParams, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
		base.AssertfCtx(ctx, "cbgt PIndexImplType.New called for index type %s, which Sync Gateway does not support", indexType)
		return nil, nil, errors.New("cbgt PIndexImplType.New is not supported by Sync Gateway")
	}
}

// getUnsupportedOpenPIndexImpl rejects PIndexImplType.Open. cbgt only opens pindexes persisted to disk, and Sync
// Gateway does not persist them.
func getUnsupportedOpenPIndexImpl(ctx context.Context) func(indexType, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	return func(indexType, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
		base.AssertfCtx(ctx, "cbgt PIndexImplType.Open called for index type %s, which Sync Gateway does not support", indexType)
		return nil, nil, errors.New("cbgt PIndexImplType.Open is not supported by Sync Gateway")
	}
}

// getUnsupportedOpenExPIndexImpl rejects PIndexImplType.OpenEx, for the same reason as Open.
func getUnsupportedOpenExPIndexImpl(ctx context.Context) func(indexType, path string, restart func(), options map[string]any) (cbgt.PIndexImpl, cbgt.Dest, error) {
	return func(indexType, path string, restart func(), options map[string]any) (cbgt.PIndexImpl, cbgt.Dest, error) {
		base.AssertfCtx(ctx, "cbgt PIndexImplType.OpenEx called for index type %s, which Sync Gateway does not support", indexType)
		return nil, nil, errors.New("cbgt PIndexImplType.OpenEx is not supported by Sync Gateway")
	}
}
