package db

import (
	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/base"
)

// init registers the PIndex type definition.  This is invoked by cbgt when a Pindex (collection of
// vbuckets) is assigned to this node.
func (l *changeListener) RegisterCachingPindexImpl() {

	// Since RegisterPIndexImplType is a global var, index type needs to be database-scoped to support
	// running multiple databases.  This avoids requiring a database lookup based in indexParams at PIndex creation
	// time, which introduces deadlock potential

	// TODO: should be dbname, not bucketname
	pIndexType := base.CBGTIndexTypeSyncGatewayCaching + l.bucketName
	base.Infof(base.KeyDCP, "Registering PindexImplType for %s", pIndexType)
	cbgt.RegisterPIndexImplType(pIndexType,
		&cbgt.PIndexImplType{
			New:       l.NewCachingPIndexImpl,
			Open:      l.OpenCachingPIndexImpl,
			OpenUsing: l.OpenCachingPIndexImplUsing,
			Description: "general/syncGateway-caching " +
				" - import processing for shared bucket access",
		})
}

// NewImportPIndexImpl is called when the node is first added to the cbgt cfg.  On a node restart,
// OpenImportPindexImpl is called, and indexParams aren't included.
func (l *changeListener) NewCachingPIndexImpl(indexType, indexParams, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {

	importDest, err := l.NewCachingDest()
	if err != nil {
		base.Errorf("Error creating NewImportDest during NewImportPIndexImpl: %v", err)
	}
	return nil, importDest, err
}

func (l *changeListener) OpenCachingPIndexImpl(indexType, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {

	importDest, err := l.NewCachingDest()
	if err != nil {
		base.Errorf("Error creating NewImportDest during OpenImportPIndexImpl: %v", err)
	}
	return nil, importDest, err
}

func (l *changeListener) OpenCachingPIndexImplUsing(indexType, path, indexParams string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	return l.OpenCachingPIndexImpl(indexType, path, restart)
}

// Returns a cbgt.Dest targeting the importListener's ProcessFeedEvent
func (l *changeListener) NewCachingDest() (cbgt.Dest, error) {

	maxVbNo, err := l.bucket.GetMaxVbno()
	if err != nil {
		return nil, err
	}

	/*  TODO: Restore caching stats
	importFeedStatsMap, ok := il.database.DbStats.StatsDatabase().Get(base.StatKeyImportDcpStats).(*expvar.Map)
	if !ok {
		return nil, errors.New("Import feed stats map not initialized")
	}

	importPartitionStat, ok := il.database.DbStats.SharedBucketImport().Get(base.StatKeyImportPartitions).(*expvar.Int)
	if !ok {
		return nil, errors.New("Import partitions stat not initialized")
	}
	*/
	cachingDest, _ := base.NewDCPDest(l.ProcessFeedEvent, l.bucket, maxVbNo, true, nil, base.DCPCachingFeedID, nil)
	return cachingDest, nil
}
