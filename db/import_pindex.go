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
			New:    getNewPIndexImplType(ctx),
			Open:   openPIndexImpl,
			OpenEx: getOpenExPIndexImpl(ctx),
			Description: "general/syncGateway-import " +
				" - import processing for shared bucket access",
		})
}

// getListenerForIndex looks up the importListener for the dbName specified in the index params
func getListenerImportDest(ctx context.Context, indexParams string, restart func()) (cbgt.Dest, error) {

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

	base.DebugfCtx(ctx, base.KeyDCP, "Fetching listener import dest for %v", base.MD(sgIndexParams.DestKey))
	destFactory, fetchErr := base.FetchDestFactory(sgIndexParams.DestKey)
	if fetchErr != nil {
		return nil, fmt.Errorf("error retrieving listener for indexParams %v: %v", indexParams, fetchErr)
	}
	return destFactory(restart)
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

// openPIndexImpl is required for cbgt, but this is used by Sync Gateway.
func openPIndexImpl(indexType, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	// This callback is used by cbft to read pindex definitions from disk.
	return nil, nil, errors.New("Open PIndexImpl not supported for SG 3.0 databases - must provide index params")
}

// getOpenExPIndexImpl is required for cbgt, but this is not supported for Sync Gateway, only uses PIndexImplType.New
func getOpenImportPIndexImplUsing(ctx context.Context) func(indexType, path string, restart func(), options map[string]any) (cbgt.PIndexImpl, cbgt.Dest, error) {
	// This callback is used by cbft to read pindex definitions from disk.
	// Implement usage here in case cbgt changes behavior in the future.
	return func(indexType, path string, restart func(), options map[string]any) (cbgt.PIndexImpl, cbgt.Dest, error) {
		p, ok := options["indexParams"]
		if !ok {
			return nil, nil, errors.New("indexParams missing from options for OpenExPIndexImpl")
		}
		indexParams, ok := p.(string)
		if !ok {
			return nil, nil, errors.New("indexParams in options is not a string for OpenExPIndexImpl")
		}
		importDest, err := getListenerImportDest(ctx, indexParams, restart)
		return nil, importDest, err
	}
}
