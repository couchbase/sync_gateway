package db

import (
	"expvar"
	"fmt"
	"sync"

	"github.com/couchbase/sync_gateway/base"
	"github.com/prometheus/client_golang/prometheus"
)

// Wrapper around *expvars.Map for database stats that provide:
//
//    - A lazy loading mechanism
//    - Initialize all stats in a stat group to their zero values
//
type DatabaseStats struct {
	dbName string

	// The expvars map the stats for this db will be stored in
	storage *expvar.Map

	statsCacheMapSync,
	statsDatabaseMapSync,
	statsDeltaSyncMapSync,
	sharedBucketImportMapSync,
	cblReplicationPushSync,
	cblReplicationPullSync,
	statsSecuritySync,
	statsGsiViewsSync sync.Once

	statsCacheMap,
	statsDatabaseMap,
	statsDeltaSyncMap,
	sharedBucketImportMap,
	cblReplicationPush,
	cblReplicationPull,
	statsSecurity,
	statsGsiViews *expvar.Map
}

func NewDatabaseStats(dbName string) *DatabaseStats {
	dbStats := DatabaseStats{
		dbName:  dbName,
		storage: new(expvar.Map).Init(),
	}
	return &dbStats
}

// Convert the entire dbstats structure into an expvar map to embed into a parent expvar map
func (d *DatabaseStats) ExpvarMap() *expvar.Map {
	return d.storage
}

func (d *DatabaseStats) StatsCache() (stats *expvar.Map) {
	d.statsCacheMapSync.Do(func() {
		d.StatsByKey(base.StatsGroupKeyCache)
	})
	return d.statsCacheMap
}

func (d *DatabaseStats) StatsDatabase() (stats *expvar.Map) {
	d.statsDatabaseMapSync.Do(func() {
		d.StatsByKey(base.StatsGroupKeyDatabase)
	})
	return d.statsDatabaseMap
}

func (d *DatabaseStats) StatsDeltaSync() (stats *expvar.Map) {
	d.statsDeltaSyncMapSync.Do(func() {
		d.StatsByKey(base.StatsGroupKeyDeltaSync)
	})
	return d.statsDeltaSyncMap
}

func (d *DatabaseStats) StatsSharedBucketImport() (stats *expvar.Map) {
	d.sharedBucketImportMapSync.Do(func() {
		d.StatsByKey(base.StatsGroupKeySharedBucketImport)
	})
	return d.sharedBucketImportMap
}

func (d *DatabaseStats) StatsCblReplicationPush() (stats *expvar.Map) {
	d.cblReplicationPushSync.Do(func() {
		d.StatsByKey(base.StatsGroupKeyCblReplicationPush)
	})
	return d.cblReplicationPush
}

func (d *DatabaseStats) StatsCblReplicationPull() (stats *expvar.Map) {
	d.cblReplicationPullSync.Do(func() {
		d.StatsByKey(base.StatsGroupKeyCblReplicationPull)
	})
	return d.cblReplicationPull
}

func (d *DatabaseStats) StatsSecurity() (stats *expvar.Map) {
	d.statsSecuritySync.Do(func() {
		d.StatsByKey(base.StatsGroupKeySecurity)
	})
	return d.statsSecurity
}

func (d *DatabaseStats) StatsGsiViews() (stats *expvar.Map) {

	d.statsGsiViewsSync.Do(func() {
		d.StatsByKey(base.StatsGroupKeyGsiViews)
	})
	return d.statsGsiViews
}

func (d *DatabaseStats) StatsByKey(key string) (stats *expvar.Map) {
	var subStatsMap *expvar.Map
	subStatsVar := d.storage.Get(key)
	if subStatsVar == nil {
		subStatsMap = initEmptyStatsMap(key, d)
		d.storage.Set(key, subStatsMap)
	} else {
		subStatsMap = subStatsVar.(*expvar.Map)
	}

	fmt.Println(subStatsMap)

	return subStatsMap
}

func initEmptyStatsMap(key string, d *DatabaseStats) *expvar.Map {

	result := new(expvar.Map).Init()

	switch key {
	case base.StatsGroupKeyCache:
		result.Set(base.StatKeyRevisionCacheHits, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRevisionCacheMisses, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRevisionCacheBypass, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheHits, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheMisses, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheRevsActive, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheRevsTombstone, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheRevsRemoval, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheNumChannels, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheMaxEntries, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCachePendingQueries, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheChannelsAdded, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheChannelsEvictedInactive, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheChannelsEvictedNRU, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheCompactCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheCompactTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheBypassCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyActiveChannels, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumSkippedSeqs, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAbandonedSeqs, base.ExpvarIntVal(0))
		result.Set(base.StatKeyHighSeqCached, base.ExpvarIntVal(0))
		result.Set(base.StatKeyHighSeqStable, base.ExpvarIntVal(0))
		result.Set(base.StatKeySkippedSeqLen, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPendingSeqLen, base.ExpvarIntVal(0))
		d.statsCacheMap = result

		c := &base.Collector{
			DBName:    d.dbName,
			Subsystem: "cache",
			Info: map[string]base.StatComponents{
				base.StatKeyRevisionCacheHits:                   {ValueType: prometheus.CounterValue},
				base.StatKeyRevisionCacheMisses:                 {ValueType: prometheus.CounterValue},
				base.StatKeyRevisionCacheBypass:                 {ValueType: prometheus.GaugeValue},
				base.StatKeyChannelCacheHits:                    {ValueType: prometheus.CounterValue},
				base.StatKeyChannelCacheMisses:                  {ValueType: prometheus.CounterValue},
				base.StatKeyChannelCacheRevsActive:              {ValueType: prometheus.GaugeValue},
				base.StatKeyChannelCacheRevsTombstone:           {ValueType: prometheus.GaugeValue},
				base.StatKeyChannelCacheRevsRemoval:             {ValueType: prometheus.GaugeValue},
				base.StatKeyChannelCacheNumChannels:             {ValueType: prometheus.GaugeValue},
				base.StatKeyChannelCacheMaxEntries:              {ValueType: prometheus.GaugeValue},
				base.StatKeyChannelCachePendingQueries:          {ValueType: prometheus.GaugeValue},
				base.StatKeyChannelCacheChannelsAdded:           {ValueType: prometheus.CounterValue},
				base.StatKeyChannelCacheChannelsEvictedInactive: {ValueType: prometheus.CounterValue},
				base.StatKeyChannelCacheChannelsEvictedNRU:      {ValueType: prometheus.CounterValue},
				base.StatKeyChannelCacheCompactCount:            {ValueType: prometheus.CounterValue},
				base.StatKeyChannelCacheBypassCount:             {ValueType: prometheus.CounterValue},
				base.StatKeyActiveChannels:                      {ValueType: prometheus.GaugeValue},
				base.StatKeyNumSkippedSeqs:                      {ValueType: prometheus.CounterValue},
				base.StatKeyAbandonedSeqs:                       {ValueType: prometheus.CounterValue},
				base.StatKeyHighSeqCached:                       {ValueType: prometheus.CounterValue},
				base.StatKeyHighSeqStable:                       {ValueType: prometheus.CounterValue},
				base.StatKeySkippedSeqLen:                       {ValueType: prometheus.GaugeValue},
				base.StatKeyPendingSeqLen:                       {ValueType: prometheus.GaugeValue},
			},
			VarMap: d.statsCacheMap,
		}
		prometheus.MustRegister(c)

	case base.StatsGroupKeyDatabase:
		result.Set(base.StatKeySequenceGetCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeySequenceReservedCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeySequenceReleasedCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAbandonedSeqs, base.ExpvarIntVal(0))
		result.Set(base.StatKeyCrc32cMatchCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumReplicationsActive, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumReplicationsTotal, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumDocWrites, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDocWritesBytes, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDocWritesXattrBytes, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumDocReadsRest, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumDocReadsBlip, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDocWritesBytesBlip, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDocReadsBytesBlip, base.ExpvarIntVal(0))
		result.Set(base.StatKeyWarnXattrSizeCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyWarnChannelsPerDocCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyWarnGrantsPerDocCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDcpCachingCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDcpCachingTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDcpReceivedCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDcpReceivedTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyCachingDcpStats, new(expvar.Map).Init())
		result.Set(base.StatKeyImportDcpStats, new(expvar.Map).Init())
		result.Set(base.StatKeyHighSeqFeed, new(base.IntMax))
		d.statsDatabaseMap = result

		c := &base.Collector{
			DBName:    d.dbName,
			Subsystem: "database",
			Info: map[string]base.StatComponents{
				base.StatKeySequenceGetCount:        {ValueType: prometheus.CounterValue},
				base.StatKeySequenceReservedCount:   {ValueType: prometheus.CounterValue},
				base.StatKeySequenceReleasedCount:   {ValueType: prometheus.CounterValue},
				base.StatKeyAbandonedSeqs:           {ValueType: prometheus.CounterValue},
				base.StatKeyCrc32cMatchCount:        {ValueType: prometheus.GaugeValue},
				base.StatKeyNumReplicationsActive:   {ValueType: prometheus.GaugeValue},
				base.StatKeyNumReplicationsTotal:    {ValueType: prometheus.CounterValue},
				base.StatKeyNumDocWrites:            {ValueType: prometheus.CounterValue},
				base.StatKeyDocWritesBytes:          {ValueType: prometheus.CounterValue},
				base.StatKeyDocWritesXattrBytes:     {ValueType: prometheus.CounterValue},
				base.StatKeyNumDocReadsRest:         {ValueType: prometheus.CounterValue},
				base.StatKeyNumDocReadsBlip:         {ValueType: prometheus.CounterValue},
				base.StatKeyDocWritesBytesBlip:      {ValueType: prometheus.CounterValue},
				base.StatKeyDocReadsBytesBlip:       {ValueType: prometheus.CounterValue},
				base.StatKeyWarnXattrSizeCount:      {ValueType: prometheus.CounterValue},
				base.StatKeyWarnChannelsPerDocCount: {ValueType: prometheus.CounterValue},
				base.StatKeyWarnGrantsPerDocCount:   {ValueType: prometheus.CounterValue},
				base.StatKeyDcpCachingCount:         {ValueType: prometheus.GaugeValue},
				base.StatKeyDcpCachingTime:          {ValueType: prometheus.GaugeValue},
				base.StatKeyDcpReceivedCount:        {ValueType: prometheus.GaugeValue},
				base.StatKeyDcpReceivedTime:         {ValueType: prometheus.GaugeValue},
				base.StatKeyCachingDcpStats:         {ValueType: prometheus.GaugeValue},
				base.StatKeyHighSeqFeed:             {ValueType: prometheus.CounterValue},
			},
			VarMap: d.statsDatabaseMap,
		}
		prometheus.MustRegister(c)

	case base.StatsGroupKeyDeltaSync:
		result.Set(base.StatKeyDeltasRequested, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDeltasSent, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDeltaPullReplicationCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDeltaCacheHits, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDeltaCacheMisses, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDeltaPushDocCount, base.ExpvarIntVal(0))
		d.statsDeltaSyncMap = result

		c := &base.Collector{
			DBName:    d.dbName,
			Subsystem: "delta_sync",
			Info: map[string]base.StatComponents{
				base.StatKeyDeltasRequested:           {ValueType: prometheus.CounterValue},
				base.StatKeyDeltasSent:                {ValueType: prometheus.CounterValue},
				base.StatKeyDeltaPullReplicationCount: {ValueType: prometheus.CounterValue},
				base.StatKeyDeltaCacheHits:            {ValueType: prometheus.CounterValue},
				base.StatKeyDeltaCacheMisses:          {ValueType: prometheus.CounterValue},
				base.StatKeyDeltaPushDocCount:         {ValueType: prometheus.CounterValue},
			},
			VarMap: d.statsDeltaSyncMap,
		}
		prometheus.MustRegister(c)

	case base.StatsGroupKeySharedBucketImport:
		result.Set(base.StatKeyImportCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyImportCancelCAS, base.ExpvarIntVal(0))
		result.Set(base.StatKeyImportErrorCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyImportProcessingTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyImportHighSeq, base.ExpvarUInt64Val(0))
		result.Set(base.StatKeyImportPartitions, base.ExpvarIntVal(0))
		d.sharedBucketImportMap = result

		c := &base.Collector{
			DBName:    d.dbName,
			Subsystem: "shared_bucket_import",
			Info: map[string]base.StatComponents{
				base.StatKeyImportCount:          {ValueType: prometheus.CounterValue},
				base.StatKeyImportCancelCAS:      {ValueType: prometheus.CounterValue},
				base.StatKeyImportErrorCount:     {ValueType: prometheus.CounterValue},
				base.StatKeyImportProcessingTime: {ValueType: prometheus.GaugeValue},
				base.StatKeyImportHighSeq:        {ValueType: prometheus.CounterValue},
				base.StatKeyImportPartitions:     {ValueType: prometheus.GaugeValue},
			},
			VarMap: d.sharedBucketImportMap,
		}
		prometheus.MustRegister(c)

	case base.StatsGroupKeyCblReplicationPush:
		result.Set(base.StatKeyDocPushCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyWriteProcessingTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeySyncFunctionCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeySyncFunctionTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyProposeChangeCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyProposeChangeTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAttachmentPushCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAttachmentPushBytes, base.ExpvarIntVal(0))
		result.Set(base.StatKeyConflictWriteCount, base.ExpvarIntVal(0))
		d.cblReplicationPush = result

		c := &base.Collector{
			DBName:    d.dbName,
			Subsystem: "replication_push",
			Info: map[string]base.StatComponents{
				base.StatKeyDocPushCount:        {ValueType: prometheus.GaugeValue},
				base.StatKeyWriteProcessingTime: {ValueType: prometheus.GaugeValue},
				base.StatKeySyncFunctionCount:   {ValueType: prometheus.CounterValue},
				base.StatKeySyncFunctionTime:    {ValueType: prometheus.CounterValue},
				base.StatKeyProposeChangeCount:  {ValueType: prometheus.CounterValue},
				base.StatKeyProposeChangeTime:   {ValueType: prometheus.CounterValue},
				base.StatKeyAttachmentPushCount: {ValueType: prometheus.CounterValue},
				base.StatKeyAttachmentPushBytes: {ValueType: prometheus.CounterValue},
				base.StatKeyConflictWriteCount:  {ValueType: prometheus.CounterValue},
			},
			VarMap: d.cblReplicationPush,
		}
		prometheus.MustRegister(c)

	case base.StatsGroupKeyCblReplicationPull:
		result.Set(base.StatKeyPullReplicationsActiveContinuous, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsActiveOneShot, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsTotalContinuous, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsTotalOneShot, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsSinceZero, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsCaughtUp, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRequestChangesCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRequestChangesTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRevSendCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRevSendLatency, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRevProcessingTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyMaxPending, new(base.IntMax))
		result.Set(base.StatKeyAttachmentPullCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAttachmentPullBytes, base.ExpvarIntVal(0))
		d.cblReplicationPull = result

		c := &base.Collector{
			DBName:    d.dbName,
			Subsystem: "replication_pull",
			Info: map[string]base.StatComponents{
				base.StatKeyPullReplicationsActiveContinuous: {ValueType: prometheus.GaugeValue},
				base.StatKeyPullReplicationsActiveOneShot:    {ValueType: prometheus.GaugeValue},
				base.StatKeyPullReplicationsTotalContinuous:  {ValueType: prometheus.GaugeValue},
				base.StatKeyPullReplicationsTotalOneShot:     {ValueType: prometheus.GaugeValue},
				base.StatKeyPullReplicationsSinceZero:        {ValueType: prometheus.CounterValue},
				base.StatKeyPullReplicationsCaughtUp:         {ValueType: prometheus.GaugeValue},
				base.StatKeyRequestChangesCount:              {ValueType: prometheus.CounterValue},
				base.StatKeyRequestChangesTime:               {ValueType: prometheus.CounterValue},
				base.StatKeyRevSendCount:                     {ValueType: prometheus.CounterValue},
				base.StatKeyRevSendLatency:                   {ValueType: prometheus.CounterValue},
				base.StatKeyRevProcessingTime:                {ValueType: prometheus.GaugeValue},
				base.StatKeyMaxPending:                       {ValueType: prometheus.GaugeValue},
				base.StatKeyAttachmentPullCount:              {ValueType: prometheus.CounterValue},
				base.StatKeyAttachmentPullBytes:              {ValueType: prometheus.CounterValue},
			},
			VarMap: d.cblReplicationPull,
		}
		prometheus.MustRegister(c)

	case base.StatsGroupKeySecurity:
		result.Set(base.StatKeyNumDocsRejected, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumAccessErrors, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAuthSuccessCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAuthFailedCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyTotalAuthTime, base.ExpvarIntVal(0))
		d.statsSecurity = result

		c := &base.Collector{
			DBName:    d.dbName,
			Subsystem: "security",
			Info: map[string]base.StatComponents{
				base.StatKeyNumDocsRejected:  {ValueType: prometheus.CounterValue},
				base.StatKeyNumAccessErrors:  {ValueType: prometheus.CounterValue},
				base.StatKeyAuthSuccessCount: {ValueType: prometheus.CounterValue},
				base.StatKeyAuthFailedCount:  {ValueType: prometheus.CounterValue},
				base.StatKeyTotalAuthTime:    {ValueType: prometheus.GaugeValue},
			},
			VarMap: d.statsSecurity,
		}
		prometheus.MustRegister(c)

	case base.StatsGroupKeyGsiViews:
		// GsiView stat keys are dynamically generated based on query names - see query.go
		d.statsGsiViews = result

		// TODO: Probably will need to show all possibilities here into prometheus.
	}

	return result
}

// Update database-specific stats that are more efficiently calculated at stats collection time
func (db *DatabaseContext) UpdateCalculatedStats() {

	if db.changeCache != nil {
		db.changeCache.updateStats()
		channelCache := db.changeCache.getChannelCache()
		db.DbStats.StatsCache().Set(base.StatKeyChannelCacheMaxEntries, base.ExpvarIntVal(channelCache.MaxCacheSize()))
		db.DbStats.StatsCache().Set(base.StatKeyHighSeqCached, base.ExpvarUInt64Val(channelCache.GetHighCacheSequence()))
	}

}
