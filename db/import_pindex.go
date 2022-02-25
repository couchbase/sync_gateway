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

	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/base"
)

// RegisterImportPindexImpl registers the PIndex type definition.  This is invoked by cbgt when a Pindex (collection of
// vbuckets) is assigned to this node.

func RegisterImportPindexImpl(configGroup string) {

	// Since RegisterPIndexImplType is a global var without synchronization, index type needs to be
	// config group scoped.  The associated importListener within the context is retrieved based on the
	// dbname in the index params
	pIndexType := base.CBGTIndexTypeSyncGatewayImport + configGroup
	base.InfofCtx(context.TODO(), base.KeyDCP, "Registering PindexImplType for %s", pIndexType)
	cbgt.RegisterPIndexImplType(pIndexType,
		&cbgt.PIndexImplType{
			New:       NewImportPIndexImpl,
			Open:      OpenImportPIndexImpl,
			OpenUsing: OpenImportPIndexImplUsing,
			Description: "general/syncGateway-import " +
				" - import processing for shared bucket access",
		})
}

// getListenerForIndex looks up the importListener for the dbName specified in the index params
func getListenerImportDest(indexParams string) (cbgt.Dest, error) {

	var sgIndexParams base.SGFeedIndexParams
	err := base.JSONUnmarshal([]byte(indexParams), &sgIndexParams)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling dbname from cbgt index params: %w", err)
	}

	destFactory, fetchErr := base.FetchDestFactory(sgIndexParams.DestKey)
	if fetchErr != nil {
		return nil, fmt.Errorf("error retrieving listener for indexParams %v: %v", indexParams, fetchErr)
	}
	return destFactory()
}

// NewImportPIndexImpl is called when the node is first added to the cbgt cfg.  On a node restart,
// OpenImportPindexImplUsing is called.
func NewImportPIndexImpl(indexType, indexParams, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	defer base.FatalPanicHandler()

	importDest, err := getListenerImportDest(indexParams)
	if err != nil {
		base.ErrorfCtx(context.TODO(), "Error creating NewImportDest during NewImportPIndexImpl: %v", err)
	}
	return nil, importDest, err
}

func OpenImportPIndexImpl(indexType, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	return nil, nil, errors.New("Open PIndexImpl not supported for SG 3.0 databases - must provide index params")
}

func OpenImportPIndexImplUsing(indexType, path, indexParams string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	importDest, err := getListenerImportDest(indexParams)
	return nil, importDest, err
}

// Returns a cbgt.Dest targeting the importListener's ProcessFeedEvent
func (il *importListener) NewImportDest() (cbgt.Dest, error) {
	callback := il.ProcessFeedEvent
	bucket := il.database.Bucket

	maxVbNo, err := bucket.GetMaxVbno()
	if err != nil {
		return nil, err
	}

	importFeedStatsMap := il.database.DbStats.Database().ImportFeedMapStats
	importPartitionStat := il.database.DbStats.SharedBucketImport().ImportPartitions

	importDest, _ := base.NewDCPDest(callback, bucket, maxVbNo, true, importFeedStatsMap.Map, base.DCPImportFeedID, importPartitionStat, il.checkpointPrefix)
	return importDest, nil
}
