package db

import (
	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/base"
)

// init registers the PIndex type definition.  This is invoked by cbgt when a Pindex (collection of
// vbuckets) is assigned to this node.
func (il *importListener) RegisterImportPindexImpl() {

	// Since RegisterPIndexImplType is a global var, index type needs to be database-scoped to support
	// running multiple databases.  This avoids requiring a database lookup based in indexParams at PIndex creation
	// time, which introduces deadlock potential

	pIndexType := base.CBGTIndexTypeSyncGatewayImport + il.database.Name
	base.Infof(base.KeyDCP, "Registering PindexImplType for %s", pIndexType)
	cbgt.RegisterPIndexImplType(pIndexType,
		&cbgt.PIndexImplType{
			New:       il.NewImportPIndexImpl,
			Open:      il.OpenImportPIndexImpl,
			OpenUsing: il.OpenImportPIndexImplUsing,
			Description: "general/syncGateway-import " +
				" - import processing for shared bucket access",
		})
}

// NewImportPIndexImpl is called when the node is first added to the cbgt cfg.  On a node restart,
// OpenImportPindexImpl is called, and indexParams aren't included.
func (il *importListener) NewImportPIndexImpl(indexType, indexParams, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {

	importDest, err := il.NewImportDest()
	if err != nil {
		base.Errorf("Error creating NewImportDest during NewImportPIndexImpl: %v", err)
	}
	return nil, importDest, err
}

func (il *importListener) OpenImportPIndexImpl(indexType, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {

	importDest, err := il.NewImportDest()
	if err != nil {
		base.Errorf("Error creating NewImportDest during OpenImportPIndexImpl: %v", err)
	}
	return nil, importDest, err
}

func (il *importListener) OpenImportPIndexImplUsing(indexType, path, indexParams string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	return il.OpenImportPIndexImpl(indexType, path, restart)
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

	importDest, _ := base.NewDCPDest(callback, bucket, maxVbNo, true, importFeedStatsMap.Map, base.DCPImportFeedID, importPartitionStat)
	return importDest, nil
}
