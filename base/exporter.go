package base

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

type SgwStats struct {
	GlobalStats GlobalStat          `json:"global_stats"`
	DbStats     map[string]*DbStats `json:"db_stats"`
}

func (s SgwStats) String() string {
	bytes, _ := JSONMarshal(s)
	return string(bytes)
}

func (s *SgwStats) Magic() interface{} {
	return *s
}

type GlobalStat struct {
	ResourceUtilization ResourceUtilization `json:"resource_utilization"`
}

type ResourceUtilization struct {
	AdminNetworkInterfaceBytesReceived  *SgwIntStat   `json:"admin_network_interface_bytes_recv"`
	AdminNetworkInterfaceBytesSent      *SgwIntStat   `json:"admin_network_interface_bytes_sent"`
	ErrorCount                          *SgwIntStat   `json:"error_count"`
	GoMemstatsHeapAlloc                 *SgwIntStat   `json:"go_memstats_heapalloc"`
	GoMemstatsHeapIdle                  *SgwIntStat   `json:"go_memstats_heapidle"`
	GoMemstatsHeapInUse                 *SgwIntStat   `json:"go_memstats_heapinuse"`
	GoMemstatsHeapReleased              *SgwIntStat   `json:"go_memstats_heapreleased"`
	GoMemstatsPauseTotalNS              *SgwIntStat   `json:"go_memstats_pausetotalns"`
	GoMemstatsStackInUse                *SgwIntStat   `json:"go_memstats_stackinuse"`
	GoMemstatsStackSys                  *SgwIntStat   `json:"go_memstats_stacksys"`
	GoMemstatsSys                       *SgwIntStat   `json:"go_memstats_sys"`
	GoroutinesHighWatermark             *SgwIntStat   `json:"goroutines_high_watermark"`
	NumGoroutines                       *SgwIntStat   `json:"num_goroutines"`
	ProcessMemoryResident               *SgwIntStat   `json:"process_memory_resident"`
	PublicNetworkInterfaceBytesReceived *SgwIntStat   `json:"pub_net_bytes_recv"`
	PublicNetworkInterfaceBytesSent     *SgwIntStat   `json:"pub_net_bytes_sent"`
	SystemMemoryTotal                   *SgwIntStat   `json:"system_memory_total"`
	WarnCount                           *SgwIntStat   `json:"warn_count"`
	CpuPercentUtil                      *SgwFloatStat `json:"cpu_percent_util"`
}

type DbStats struct {
	dbName string
	// Cache                   *CacheStats              `json:"cache"`
	DatabaseStats           *DatabaseStats           `json:"database_stats,omitempty"`
	DeltaSyncStats          *DeltaSyncStats          `json:"delta_sync_stats,omitempty"`
	GsiStats                *GsiStats                `json:"gsi_views,omitempty"`
	SharedBucketImportStats *SharedBucketImportStats `json:"shared_bucket_import_stats,omitempty"`
}

type GsiStats struct {
	Stats map[string]*QueryStat
}

type CacheStats struct {
	RevisionCacheHits                   *SgwIntStat `json:"revision_cache_hits"`
	RevisionCacheMisses                 *SgwIntStat `json:"revision_cache_misses"`
	RevisionCacheBypass                 *SgwIntStat `json:"revision_cache_bypass"`
	ChannelCacheHits                    *SgwIntStat `json:"channel_cache_hits"`
	ChannelCacheMisses                  *SgwIntStat `json:"channel_cache_misses"`
	ChannelCacheRevsActive              *SgwIntStat `json:"channel_cache_revs_active"`
	ChannelCacheRevsTombstone           *SgwIntStat `json:"channel_cache_revs_tombstone"`
	ChannelCacheRevsRemoval             *SgwIntStat `json:"channel_cache_revs_removal"`
	ChannelCacheNumChannels             *SgwIntStat `json:"channel_cache_num_channels"`
	ChannelCacheMaxEntries              *SgwIntStat `json:"channel_cache_max_entries"`
	ChannelCachePendingQueries          *SgwIntStat `json:"channel_cache_pending_queries"`
	ChannelCacheChannelsAdded           *SgwIntStat `json:"channel_cache_channels_added"`
	ChannelCacheChannelsEvictedInactive *SgwIntStat `json:"channel_cache_channels_evicted_inactive"`
	ChannelCacheChannelsEvictedNRU      *SgwIntStat `json:"channel_cache_channels_evicted_nru"`
	ChannelCacheCompactCount            *SgwIntStat `json:"channel_cache_compact_count"`
	ChannelCacheCompactTime             *SgwIntStat `json:"channel_cache_compact_time"`
	ChannelCacheBypassCount             *SgwIntStat `json:"channel_cache_bypass_count"`
	ActiveChannels                      *SgwIntStat `json:"active_channels"`
	NumSkippedSeqs                      *SgwIntStat `json:"num_skipped_seqs"`
	AbandonedSeqs                       *SgwIntStat `json:"abandoned_seqs"`
	HighSeqCached                       *SgwIntStat `json:"high_seq_cached"`
	HighSeqStable                       *SgwIntStat `json:"high_seq_stable"`
	SkippedSeqLen                       *SgwIntStat `json:"skipped_seq_len"`
	PendingSeqLen                       *SgwIntStat `json:"pending_seq_len"`
}

type DatabaseStats struct {
	// TODO : Cache feed & Import feed
	AbandonedSeqs           *SgwIntStat `json:"abandoned_seqs"`
	Crc32MatchCount         *SgwIntStat `json:"crc_32_match_count"`
	DCPCachingCount         *SgwIntStat `json:"dcp_caching_count"`
	DCPCachingTime          *SgwIntStat `json:"dcp_caching_time"`
	DCPReceivedCount        *SgwIntStat `json:"dcp_received_count"`
	DCPReceivedTime         *SgwIntStat `json:"dcp_received_time"`
	DocReadsBytesBlip       *SgwIntStat `json:"doc_reads_bytes_blip"`
	DocWritesBytes          *SgwIntStat `json:"doc_writes_bytes"`
	DocWritesBytesBlip      *SgwIntStat `json:"doc_writes_bytes_blip"`
	DocWritesXattrBytes     *SgwIntStat `json:"doc_writes_xattr_bytes"`
	HighSeqFeed             *SgwIntStat `json:"high_seq_feed"`
	NumDocReadsBlip         *SgwIntStat `json:"num_doc_reads_blip"`
	NumDocReadsRest         *SgwIntStat `json:"num_doc_reads_rest"`
	NumDocWrites            *SgwIntStat `json:"num_doc_writes"`
	NumReplicationsActive   *SgwIntStat `json:"num_replications_active"`
	NumReplicationsTotal    *SgwIntStat `json:"num_replications_total"`
	NumTombstonesCompacted  *SgwIntStat `json:"num_tombstones_compacted"`
	SequenceAssignedCount   *SgwIntStat `json:"sequence_assigned_count"`
	SequenceGetCount        *SgwIntStat `json:"sequence_get_count"`
	SequenceIncrCount       *SgwIntStat `json:"sequence_incr_count"`
	SequenceReleasedCount   *SgwIntStat `json:"sequence_released_count"`
	SequenceReservedCount   *SgwIntStat `json:"sequence_reserved_count"`
	WarnChannelsPerDocCount *SgwIntStat `json:"warn_channels_per_doc_count"`
	WarnGrantsPerDocCount   *SgwIntStat `json:"warn_grants_per_doc_count"`
	WarnXattrSizeCount      *SgwIntStat `json:"warn_xattr_size_count"`
}

type DeltaSyncStats struct {
	DeltaCacheHit             *SgwIntStat `json:"delta_cache_hit"`
	DeltaCacheMiss            *SgwIntStat `json:"delta_cache_miss"`
	DeltaPullReplicationCount *SgwIntStat `json:"delta_pull_replication_count"`
	DeltaPushDocCount         *SgwIntStat `json:"delta_push_doc_count"`
	DeltasRequested           *SgwIntStat `json:"deltas_requested"`
	DeltasSent                *SgwIntStat `json:"deltas_sent"`
}

type SharedBucketImportStats struct {
	ImportCount          *SgwIntStat `json:"import_count"`
	ImportCancelCAS      *SgwIntStat `json:"import_cancel_cas"`
	ImportErrorCount     *SgwIntStat `json:"import_error_count"`
	ImportProcessingTime *SgwIntStat `json:"import_processing_time"`
	ImportHighSeq        *SgwIntStat `json:"import_high_seq"`
	ImportPartitions     *SgwIntStat `json:"import_partitions"`
}

type SgwStat struct {
	statFQDN      string
	statDesc      *prometheus.Desc
	dbName        string
	statValueType prometheus.ValueType
}

type SgwIntStat struct {
	SgwStat
	Value int64
}

type SgwFloatStat struct {
	SgwStat
	Value float64
}

func NewIntStat(subsystem string, key string, dbName string, statValueType prometheus.ValueType, initialValue int64) *SgwIntStat {
	var label []string
	if dbName != "" {
		label = []string{"database"}
	}

	name := prometheus.BuildFQName("sgw", subsystem, key)
	desc := prometheus.NewDesc(name, key, label, nil)

	stat := &SgwIntStat{
		SgwStat: SgwStat{
			statFQDN:      name,
			statDesc:      desc,
			dbName:        dbName,
			statValueType: statValueType,
		},
		Value: initialValue,
	}
	prometheus.MustRegister(stat)
	return stat
}

func (s *SgwIntStat) Describe(ch chan<- *prometheus.Desc) {
	return
}

func (s *SgwIntStat) Collect(ch chan<- prometheus.Metric) {
	var labelVals []string
	if s.dbName != "" {
		labelVals = append(labelVals, s.dbName)
	}
	ch <- prometheus.MustNewConstMetric(s.statDesc, s.statValueType, float64(s.Value), labelVals...)
}

func (s *SgwIntStat) Set(newV int64) {
	s.Value = newV
	fmt.Println(s.Value)
}

func (s *SgwIntStat) SetIfMax(newV int64) {
	if newV > s.Value {
		s.Value = newV
	}
}

func (s *SgwIntStat) Add(newV int64) {
	s.Value += newV
}

func (s *SgwIntStat) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%v", s.Value)), nil
}

func (s *SgwIntStat) String() string {
	return fmt.Sprintf("%v", s.Value)
}

func NewFloatStat(subsystem string, key string, dbName string, statValueType prometheus.ValueType, initialValue float64) *SgwFloatStat {
	var label []string
	if dbName != "" {
		label = []string{"database"}
	}

	name := prometheus.BuildFQName("sgw", subsystem, key)
	desc := prometheus.NewDesc(name, key, label, nil)

	stat := &SgwFloatStat{
		SgwStat: SgwStat{
			statFQDN:      name,
			statDesc:      desc,
			dbName:        dbName,
			statValueType: statValueType,
		},
		Value: initialValue,
	}
	prometheus.MustRegister(stat)
	return stat
}

func (s *SgwFloatStat) Describe(ch chan<- *prometheus.Desc) {
	return
}

func (s *SgwFloatStat) Collect(ch chan<- prometheus.Metric) {
	var labelVals []string
	if s.dbName != "" {
		labelVals = append(labelVals, s.dbName)
	}
	ch <- prometheus.MustNewConstMetric(s.statDesc, s.statValueType, s.Value, labelVals...)
}

func (s *SgwFloatStat) Set(newV float64) {
	s.Value = newV
	fmt.Println(s.Value)
}

func (s *SgwFloatStat) SetIfMax(newV float64) {
	if newV > s.Value {
		s.Value = newV
	}
}

func (s *SgwFloatStat) Add(newV float64) {
	s.Value += newV
}

func (s *SgwFloatStat) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%v", s.Value)), nil
}

func (s *SgwFloatStat) String() string {
	return fmt.Sprintf("%v", s.Value)
}

type QueryStat struct {
	QueryCount      *SgwIntStat
	QueryErrorCount *SgwIntStat
	QueryTime       *SgwIntStat
}

func (s *SgwStats) DBStats(name string) *DbStats {

	if s.DbStats == nil {
		s.DbStats = map[string]*DbStats{}
	}

	if _, ok := s.DbStats[name]; !ok {
		s.DbStats[name] = &DbStats{}
	}
	return s.DbStats[name]
}

func (d *DbStats) DeltaSync() *DeltaSyncStats {
	if d.DeltaSyncStats == nil {
		dbName := d.dbName
		d.DeltaSyncStats = &DeltaSyncStats{
			DeltasRequested:           NewIntStat("delta_sync", "deltas_requested", dbName, prometheus.CounterValue, 0),
			DeltasSent:                NewIntStat("delta_sync", "deltas_sent", dbName, prometheus.CounterValue, 0),
			DeltaPullReplicationCount: NewIntStat("delta_sync", "delta_pull_replication_count", dbName, prometheus.CounterValue, 0),
			DeltaCacheHit:             NewIntStat("delta_sync", "delta_cache_hit", dbName, prometheus.CounterValue, 0),
			DeltaCacheMiss:            NewIntStat("delta_sync", "delta_sync_miss", dbName, prometheus.CounterValue, 0),
			DeltaPushDocCount:         NewIntStat("delta_sync", "delta_push_doc_count", dbName, prometheus.CounterValue, 0),
		}
	}
	return d.DeltaSyncStats
}

func (d *DbStats) SharedBucketImport() *SharedBucketImportStats {
	if d.SharedBucketImportStats == nil {
		dbName := d.dbName
		d.SharedBucketImportStats = &SharedBucketImportStats{
			ImportCount:          NewIntStat("shared_bucket_import", "import_count", dbName, prometheus.CounterValue, 0),
			ImportCancelCAS:      NewIntStat("shared_bucket_import", "import_cancel_cas", dbName, prometheus.CounterValue, 0),
			ImportErrorCount:     NewIntStat("shared_bucket_import", "import_error_count", dbName, prometheus.CounterValue, 0),
			ImportProcessingTime: NewIntStat("shared_bucket_import", "import_processing_time", dbName, prometheus.GaugeValue, 0),
			ImportHighSeq:        NewIntStat("shared_bucket_import", "import_high_seq", dbName, prometheus.CounterValue, 0),
			ImportPartitions:     NewIntStat("shared_bucket_import", "import_partitions", dbName, prometheus.GaugeValue, 0),
		}
	}

	return d.SharedBucketImportStats
}

func (d *DbStats) GSIStats(queryName string) *QueryStat {
	if d.GsiStats == nil {
		d.GsiStats = &GsiStats{
			Stats: map[string]*QueryStat{},
		}
	}

	if _, ok := d.GsiStats.Stats[queryName]; !ok {
		dbName := d.dbName
		d.GsiStats.Stats[queryName] = &QueryStat{
			QueryCount:      NewIntStat("gsi_views", queryName+"_query_count", dbName, prometheus.CounterValue, 0),
			QueryErrorCount: NewIntStat("gsi_views", queryName+"_query_error_count", dbName, prometheus.CounterValue, 0),
			QueryTime:       NewIntStat("gsi_views", queryName+"_query_time", dbName, prometheus.CounterValue, 0),
		}
	}

	return d.GsiStats.Stats[queryName]
}

func (g *GsiStats) MarshalJSON() ([]byte, error) {
	ret := map[string]interface{}{}
	for queryName, queryMap := range g.Stats {
		ret[queryName+"_query_count"] = queryMap.QueryCount
		ret[queryName+"_query_error_count"] = queryMap.QueryErrorCount
		ret[queryName+"_query_time"] = queryMap.QueryTime
	}

	return JSONMarshal(ret)
}
