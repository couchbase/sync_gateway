package base

import (
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type SgwStats struct {
	GlobalStats GlobalStat          `json:"global_stats"`
	DbStats     map[string]*DbStats `json:"per_db"`
	initDBStats sync.Once
}

func (s *SgwStats) String() string {
	bytes, _ := JSONMarshal(s)
	return string(bytes)
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
	dbName                  string
	CacheStats              *CacheStats              `json:"cache"`
	CBLReplicationPullStats *CBLReplicationPullStats `json:"cbl_replication_pull_stats"`
	CBLReplicationPushStats *CBLReplicationPushStats `json:"cbl_replication_push_stats"`
	DatabaseStats           *DatabaseStats           `json:"database_stats,omitempty"`
	DeltaSyncStats          *DeltaSyncStats          `json:"delta_sync_stats,omitempty"`
	GsiStats                *GsiStats                `json:"gsi_views,omitempty"`
	ReplicationStats        *ReplicatorStats         `json:"replication_stats"`
	SecurityStats           *SecurityStats           `json:"security"`
	SharedBucketImportStats *SharedBucketImportStats `json:"shared_bucket_import_stats,omitempty"`

	initCacheStats              sync.Once
	initCBLReplicationPullStats sync.Once
	initCBLReplicationPushStats sync.Once
	initDatabaseStats           sync.Once
	initDeltaSyncStats          sync.Once
	initGsiStats                sync.Once
	initReplicatorStats         sync.Once
	initSecurityStats           sync.Once
	initSharedBucketImportStats sync.Once
}

type CacheStats struct {
	AbandonedSeqs                       *SgwIntStat `json:"abandoned_seqs"`
	ChannelCacheRevsActive              *SgwIntStat `json:"channel_cache_revs_active"`
	ChannelCacheBypassCount             *SgwIntStat `json:"channel_cache_bypass_count"`
	ChannelCacheChannelsAdded           *SgwIntStat `json:"channel_cache_channels_added"`
	ChannelCacheChannelsEvictedInactive *SgwIntStat `json:"channel_cache_channels_evicted_inactive"`
	ChannelCacheChannelsEvictedNRU      *SgwIntStat `json:"channel_cache_channels_evicted_nru"`
	ChannelCacheCompactCount            *SgwIntStat `json:"channel_cache_compact_count"`
	ChannelCacheCompactTime             *SgwIntStat `json:"channel_cache_compact_time"`
	ChannelCacheHits                    *SgwIntStat `json:"channel_cache_hits"`
	ChannelCacheMaxEntries              *SgwIntStat `json:"channel_cache_max_entries"`
	ChannelCacheMisses                  *SgwIntStat `json:"channel_cache_misses"`
	ChannelCacheNumChannels             *SgwIntStat `json:"channel_cache_num_channels"`
	ChannelCachePendingQueries          *SgwIntStat `json:"channel_cache_pending_queries"`
	ChannelCacheRevsRemoval             *SgwIntStat `json:"channel_cache_revs_removal"`
	ChannelCacheRevsTombstone           *SgwIntStat `json:"channel_cache_revs_tombstone"`
	HighSeqCached                       *SgwIntStat `json:"high_seq_cached"`
	HighSeqStable                       *SgwIntStat `json:"high_seq_stable"`
	NumActiveChannels                   *SgwIntStat `json:"num_active_channels"`
	NumSkippedSeqs                      *SgwIntStat `json:"num_skipped_seqs"`
	PendingSeqLen                       *SgwIntStat `json:"pending_seq_len"`
	RevisionCacheBypass                 *SgwIntStat `json:"revision_cache_bypass"`
	RevisionCacheHits                   *SgwIntStat `json:"revision_cache_hits"`
	RevisionCacheMisses                 *SgwIntStat `json:"revision_cache_misses"`
	SkippedSeqLen                       *SgwIntStat `json:"skipped_seq_len"`
}

type CBLReplicationPullStats struct {
	AttachmentPullBytes         *SgwIntStat `json:"attachment_pull_bytes"`
	AttachmentPullCount         *SgwIntStat `json:"attachment_pull_count"`
	MaxPending                  *SgwIntStat `json:"max_pending"`
	NumReplicationsActive       *SgwIntStat `json:"num_replications_active"`
	NumPullReplActiveContinuous *SgwIntStat `json:"num_pull_repl_active_continuous"`
	NumPullReplActiveOneShot    *SgwIntStat `json:"num_pull_repl_active_one_shot"`
	NumPullReplCaughtUp         *SgwIntStat `json:"num_pull_repl_caught_up"`
	NumPullReplSinceZero        *SgwIntStat `json:"num_pull_repl_since_zero"`
	NumPullReplTotalContinuous  *SgwIntStat `json:"num_pull_repl_total_continuous"`
	NumPullReplTotalOneShot     *SgwIntStat `json:"num_pull_repl_total_one_shot"`
	RequestChangesCount         *SgwIntStat `json:"request_changes_count"`
	RequestChangesTime          *SgwIntStat `json:"request_changes_time"`
	RevProcessingTime           *SgwIntStat `json:"rev_processing_time"`
	RevSendCount                *SgwIntStat `json:"rev_send_count"`
	RevSendLatency              *SgwIntStat `json:"rev_send_latency"`
}

type CBLReplicationPushStats struct {
	AttachmentPushBytes *SgwIntStat `json:"attachment_push_bytes"`
	AttachmentPushCount *SgwIntStat `json:"attachment_push_count"`
	DocPushCount        *SgwIntStat `json:"doc_push_count"`
	ProposeChangeCount  *SgwIntStat `json:"propose_change_count"`
	ProposeChangeTime   *SgwIntStat `json:"propose_change_time"`
	SyncFunctionCount   *SgwIntStat `json:"sync_function_count"`
	SyncFunctionTime    *SgwIntStat `json:"sync_function_time"`
	WriteProcessingTime *SgwIntStat `json:"write_processing_time"`
}

type DatabaseStats struct {
	// TODO : Cache feed & Import feed
	AbandonedSeqs           *SgwIntStat `json:"abandoned_seqs"`
	ConflictWriteCount      *SgwIntStat `json:"conflict_write_count"`
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

type GsiStats struct {
	Stats map[string]*QueryStat
	mutex sync.Mutex
}

type ReplicatorStats struct {
	Active                        *SgwBoolStat `json:"active"`
	DocsCheckedSent               *SgwIntStat  `json:"docs_checked_sent"`
	NumAttachmentBytesTransferred *SgwIntStat  `json:"num_attachment_bytes_transferred"`
	NumAttachmentsTransferred     *SgwIntStat  `json:"num_attachments_transferred"`
	NumDocsFailedToPush           *SgwIntStat  `json:"num_docs_failed_to_push"`
	NumDocsPushed                 *SgwIntStat  `json:"num_docs_pushed"`
}

type SecurityStats struct {
	AuthFailedCount  *SgwIntStat `json:"auth_failed_count"`
	AuthSuccessCount *SgwIntStat `json:"auth_success_count"`
	NumAccessErrors  *SgwIntStat `json:"num_access_errors"`
	NumDocsRejected  *SgwIntStat `json:"num_docs_rejected"`
	TotalAuthTime    *SgwIntStat `json:"total_auth_time"`
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
	mutex         sync.Mutex
}

type SgwIntStat struct {
	SgwStat
	Val int64
}

type SgwFloatStat struct {
	SgwStat
	Val float64
}

type SgwBoolStat struct {
	SgwStat
	Val bool
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
		Val: initialValue,
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
	ch <- prometheus.MustNewConstMetric(s.statDesc, s.statValueType, float64(s.Val), labelVals...)
}

func (s *SgwIntStat) Set(newV int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.Val = newV
	fmt.Println(s.Val)
}

func (s *SgwIntStat) SetIfMax(newV int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if newV > s.Val {
		s.Val = newV
	}
}

func (s *SgwIntStat) Add(newV int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.Val += newV
}

func (s *SgwIntStat) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%v", s.Val)), nil
}

func (s *SgwIntStat) String() string {
	return fmt.Sprintf("%v", s.Val)
}

func (s *SgwIntStat) Value() int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.Val
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
		Val: initialValue,
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
	ch <- prometheus.MustNewConstMetric(s.statDesc, s.statValueType, s.Val, labelVals...)
}

func (s *SgwFloatStat) Set(newV float64) {
	s.Val = newV
	fmt.Println(s.Val)
}

func (s *SgwFloatStat) SetIfMax(newV float64) {
	if newV > s.Val {
		s.Val = newV
	}
}

func (s *SgwFloatStat) Add(newV float64) {
	s.Val += newV
}

func (s *SgwFloatStat) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%v", s.Val)), nil
}

func (s *SgwFloatStat) String() string {
	return fmt.Sprintf("%v", s.Val)
}

func (s *SgwFloatStat) Value() float64 {
	return s.Val
}

func NewBoolStat(subsystem string, key string, dbName string, statValueType prometheus.ValueType, initialValue bool) *SgwBoolStat {
	var label []string
	if dbName != "" {
		label = []string{"database"}
	}

	name := prometheus.BuildFQName("sgw", subsystem, key)
	desc := prometheus.NewDesc(name, key, label, nil)

	stat := &SgwBoolStat{
		SgwStat: SgwStat{
			statFQDN:      name,
			statDesc:      desc,
			dbName:        dbName,
			statValueType: statValueType,
		},
		Val: initialValue,
	}
	// prometheus.MustRegister(stat)
	return stat
}

func (s *SgwBoolStat) Set(newV bool) {
	s.Val = newV
	fmt.Println(s.Val)
}

func (s *SgwBoolStat) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%v", s.Val)), nil
}

func (s *SgwBoolStat) String() string {
	return fmt.Sprintf("%v", s.Val)
}

func (s *SgwBoolStat) Value() bool {
	return s.Val
}

type QueryStat struct {
	QueryCount      *SgwIntStat
	QueryErrorCount *SgwIntStat
	QueryTime       *SgwIntStat
}

func (s *SgwStats) NewDBStats(name string) *DbStats {
	s.initDBStats.Do(func() {
		if s.DbStats == nil {
			s.DbStats = map[string]*DbStats{}
		}
	})

	s.DbStats[name] = &DbStats{}
	return s.DbStats[name]
}

func (d *DbStats) Cache() *CacheStats {
	d.initCacheStats.Do(func() {
		if d.CacheStats == nil {
			dbName := d.dbName
			d.CacheStats = &CacheStats{
				AbandonedSeqs:                       NewIntStat("cache", "abadoned_seqs", dbName, prometheus.CounterValue, 0),
				ChannelCacheRevsActive:              NewIntStat("cache", "chan_cache_active_revs", dbName, prometheus.GaugeValue, 0),
				ChannelCacheBypassCount:             NewIntStat("cache", "chan_cache_bypass_count", dbName, prometheus.CounterValue, 0),
				ChannelCacheChannelsAdded:           NewIntStat("cache", "chan_cache_channels_added", dbName, prometheus.CounterValue, 0),
				ChannelCacheChannelsEvictedInactive: NewIntStat("cache", "chan_cache_channels_evicted_inactive", dbName, prometheus.CounterValue, 0),
				ChannelCacheChannelsEvictedNRU:      NewIntStat("cache", "chan_cache_channels_evicted_nru", dbName, prometheus.CounterValue, 0),
				ChannelCacheCompactCount:            NewIntStat("cache", "chan_cache_compact_count", dbName, prometheus.CounterValue, 0),
				ChannelCacheCompactTime:             NewIntStat("cache", "chan_cache_compact_time", dbName, prometheus.CounterValue, 0),
				ChannelCacheHits:                    NewIntStat("cache", "chan_cache_hits", dbName, prometheus.CounterValue, 0),
				ChannelCacheMaxEntries:              NewIntStat("cache", "chan_cache_max_entries", dbName, prometheus.GaugeValue, 0),
				ChannelCacheMisses:                  NewIntStat("cache", "chan_cache_misses", dbName, prometheus.CounterValue, 0),
				ChannelCacheNumChannels:             NewIntStat("cache", "chan_cache_num_channels", dbName, prometheus.GaugeValue, 0),
				ChannelCachePendingQueries:          NewIntStat("cache", "chan_cache_pending_queries", dbName, prometheus.GaugeValue, 0),
				ChannelCacheRevsRemoval:             NewIntStat("cache", "chan_cache_removal_revs", dbName, prometheus.GaugeValue, 0),
				ChannelCacheRevsTombstone:           NewIntStat("cache", "chan_cache_tombstone_revs", dbName, prometheus.GaugeValue, 0),
				HighSeqCached:                       NewIntStat("cache", "high_seq_cached", dbName, prometheus.CounterValue, 0),
				HighSeqStable:                       NewIntStat("cache", "high_seq_stable", dbName, prometheus.CounterValue, 0),
				NumActiveChannels:                   NewIntStat("cache", "num_active_channels", dbName, prometheus.GaugeValue, 0),
				NumSkippedSeqs:                      NewIntStat("cache", "num_skipped_seqs", dbName, prometheus.CounterValue, 0),
				PendingSeqLen:                       NewIntStat("cache", "pending_seq_len", dbName, prometheus.GaugeValue, 0),
				RevisionCacheBypass:                 NewIntStat("cache", "rev_cache_bypass", dbName, prometheus.GaugeValue, 0),
				RevisionCacheHits:                   NewIntStat("cache", "rev_cache_hits", dbName, prometheus.CounterValue, 0),
				RevisionCacheMisses:                 NewIntStat("cache", "rev_cache_misses", dbName, prometheus.CounterValue, 0),
				SkippedSeqLen:                       NewIntStat("cache", "skipped_seq_len", dbName, prometheus.GaugeValue, 0),
			}
		}
	})
	return d.CacheStats
}

func (d *DbStats) CBLReplicationPull() *CBLReplicationPullStats {
	d.initCBLReplicationPullStats.Do(func() {
		if d.CBLReplicationPullStats == nil {
			dbName := d.dbName
			d.CBLReplicationPullStats = &CBLReplicationPullStats{
				AttachmentPullBytes:         NewIntStat("replication_pull", "attachment_pull_bytes", dbName, prometheus.CounterValue, 0),
				AttachmentPullCount:         NewIntStat("replication_pull", "attachment_pull_count", dbName, prometheus.CounterValue, 0),
				MaxPending:                  NewIntStat("replication_pull", "max_pending", dbName, prometheus.GaugeValue, 0),
				NumReplicationsActive:       NewIntStat("replication_pull", "num_pull_repl_active_continuous", dbName, prometheus.GaugeValue, 0),
				NumPullReplActiveContinuous: NewIntStat("replication_pull", "num_pull_repl_active_one_shot", dbName, prometheus.GaugeValue, 0),
				NumPullReplActiveOneShot:    NewIntStat("replication_pull", "num_replications_active", dbName, prometheus.GaugeValue, 0),
				NumPullReplCaughtUp:         NewIntStat("replication_pull", "num_pull_repl_caught_up", dbName, prometheus.GaugeValue, 0),
				NumPullReplSinceZero:        NewIntStat("replication_pull", "num_pull_repl_since_zero", dbName, prometheus.CounterValue, 0),
				NumPullReplTotalContinuous:  NewIntStat("replication_pull", "num_pull_repl_total_continuous", dbName, prometheus.GaugeValue, 0),
				NumPullReplTotalOneShot:     NewIntStat("replication_pull", "num_pull_repl_total_one_shot", dbName, prometheus.GaugeValue, 0),
				RequestChangesCount:         NewIntStat("replication_pull", "request_changes_count", dbName, prometheus.CounterValue, 0),
				RequestChangesTime:          NewIntStat("replication_pull", "request_changes_time", dbName, prometheus.CounterValue, 0),
				RevProcessingTime:           NewIntStat("replication_pull", "rev_processing_time", dbName, prometheus.GaugeValue, 0),
				RevSendCount:                NewIntStat("replication_pull", "rev_send_count", dbName, prometheus.CounterValue, 0),
				RevSendLatency:              NewIntStat("replication_pull", "rev_send_latency", dbName, prometheus.CounterValue, 0),
			}
		}
	})
	return d.CBLReplicationPullStats
}

func (d *DbStats) CBLReplicationPush() *CBLReplicationPushStats {
	d.initCBLReplicationPushStats.Do(func() {
		if d.CBLReplicationPushStats == nil {
			dbName := d.dbName
			d.CBLReplicationPushStats = &CBLReplicationPushStats{
				AttachmentPushBytes: NewIntStat("replication_push", "attachment_push_bytes", dbName, prometheus.CounterValue, 0),
				AttachmentPushCount: NewIntStat("replication_push", "attachment_push_count", dbName, prometheus.CounterValue, 0),
				DocPushCount:        NewIntStat("replication_push", "doc_push_count", dbName, prometheus.GaugeValue, 0),
				ProposeChangeCount:  NewIntStat("replication_push", "propose_change_count", dbName, prometheus.CounterValue, 0),
				ProposeChangeTime:   NewIntStat("replication_push", "propose_change_time", dbName, prometheus.CounterValue, 0),
				SyncFunctionCount:   NewIntStat("replication_push", "sync_function_count", dbName, prometheus.CounterValue, 0),
				SyncFunctionTime:    NewIntStat("replication_push", "sync_function_time", dbName, prometheus.CounterValue, 0),
				WriteProcessingTime: NewIntStat("replication_push", "write_processing_time", dbName, prometheus.GaugeValue, 0),
			}
		}
	})
	return d.CBLReplicationPushStats
}

func (d *DbStats) Database() *DatabaseStats {
	d.initDatabaseStats.Do(func() {
		if d.DatabaseStats == nil {
			dbName := d.dbName
			d.DatabaseStats = &DatabaseStats{
				AbandonedSeqs:           NewIntStat("database", "abandoned_seqs", dbName, prometheus.CounterValue, 0),
				ConflictWriteCount:      NewIntStat("database", "conflict_write_count", dbName, prometheus.CounterValue, 0),
				Crc32MatchCount:         NewIntStat("database", "crc32c_match_count", dbName, prometheus.GaugeValue, 0),
				DCPCachingCount:         NewIntStat("database", "dcp_caching_count", dbName, prometheus.GaugeValue, 0),
				DCPCachingTime:          NewIntStat("database", "dcp_caching_time", dbName, prometheus.GaugeValue, 0),
				DCPReceivedCount:        NewIntStat("database", "dcp_received_count", dbName, prometheus.GaugeValue, 0),
				DCPReceivedTime:         NewIntStat("database", "dcp_received_time", dbName, prometheus.GaugeValue, 0),
				DocReadsBytesBlip:       NewIntStat("database", "doc_reads_bytes_blip", dbName, prometheus.CounterValue, 0),
				DocWritesBytes:          NewIntStat("database", "doc_writes_bytes", dbName, prometheus.CounterValue, 0),
				DocWritesXattrBytes:     NewIntStat("database", "doc_writes_xattr_bytes", dbName, prometheus.CounterValue, 0),
				HighSeqFeed:             NewIntStat("database", "high_seq_feed", dbName, prometheus.CounterValue, 0),
				DocWritesBytesBlip:      NewIntStat("database", "doc_writes_bytes_blip", dbName, prometheus.CounterValue, 0),
				NumDocReadsBlip:         NewIntStat("database", "num_doc_reads_blip", dbName, prometheus.CounterValue, 0),
				NumDocReadsRest:         NewIntStat("database", "num_doc_reads_rest", dbName, prometheus.CounterValue, 0),
				NumDocWrites:            NewIntStat("database", "num_doc_writes", dbName, prometheus.CounterValue, 0),
				NumReplicationsActive:   NewIntStat("database", "num_replications_active", dbName, prometheus.GaugeValue, 0),
				NumReplicationsTotal:    NewIntStat("database", "num_replications_total", dbName, prometheus.CounterValue, 0),
				NumTombstonesCompacted:  NewIntStat("database", "num_tombstones_compacted", dbName, prometheus.CounterValue, 0),
				SequenceAssignedCount:   NewIntStat("database", "sequence_assigned_count", dbName, prometheus.CounterValue, 0),
				SequenceGetCount:        NewIntStat("database", "sequence_get_count", dbName, prometheus.CounterValue, 0),
				SequenceIncrCount:       NewIntStat("database", "sequence_incr_count", dbName, prometheus.CounterValue, 0),
				SequenceReleasedCount:   NewIntStat("database", "sequence_released_count", dbName, prometheus.CounterValue, 0),
				SequenceReservedCount:   NewIntStat("database", "sequence_reserved_count", dbName, prometheus.CounterValue, 0),
				WarnChannelsPerDocCount: NewIntStat("database", "warn_channels_per_doc_count", dbName, prometheus.CounterValue, 0),
				WarnGrantsPerDocCount:   NewIntStat("database", "warn_grants_per_doc_count", dbName, prometheus.CounterValue, 0),
				WarnXattrSizeCount:      NewIntStat("database", "warn_xattr_size_count", dbName, prometheus.CounterValue, 0),
			}
		}
	})
	return d.DatabaseStats
}

func (d *DbStats) DeltaSync() *DeltaSyncStats {
	d.initDeltaSyncStats.Do(func() {
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
	})
	return d.DeltaSyncStats
}

func (d *DbStats) Replication() *ReplicatorStats {
	d.initReplicatorStats.Do(func() {
		if d.ReplicationStats == nil {
			dbName := d.dbName
			d.ReplicationStats = &ReplicatorStats{
				Active:                        NewBoolStat("replication", "active", dbName, prometheus.GaugeValue, false),
				DocsCheckedSent:               NewIntStat("replication", "docs_checked_sent", dbName, prometheus.CounterValue, 0),
				NumAttachmentBytesTransferred: NewIntStat("replication", "attachment_bytes_transferred", dbName, prometheus.CounterValue, 0),
				NumAttachmentsTransferred:     NewIntStat("replication", "attachments_transferred", dbName, prometheus.CounterValue, 0),
				NumDocsFailedToPush:           NewIntStat("replication", "num_docs_failed_to_push", dbName, prometheus.CounterValue, 0),
				NumDocsPushed:                 NewIntStat("replication", "num_docs_pushed", dbName, prometheus.CounterValue, 0),
			}
		}
	})
	return d.ReplicationStats
}

func (d *DbStats) Security() *SecurityStats {
	d.initSecurityStats.Do(func() {
		if d.SecurityStats == nil {
			dbName := d.dbName
			d.SecurityStats = &SecurityStats{
				AuthFailedCount:  NewIntStat("security", "auth_failed_count", dbName, prometheus.CounterValue, 0),
				AuthSuccessCount: NewIntStat("security", "auth_success_count", dbName, prometheus.CounterValue, 0),
				NumAccessErrors:  NewIntStat("security", "num_access_errors", dbName, prometheus.CounterValue, 0),
				NumDocsRejected:  NewIntStat("security", "num_docs_rejected", dbName, prometheus.CounterValue, 0),
				TotalAuthTime:    NewIntStat("security", "total_auth_time", dbName, prometheus.GaugeValue, 0),
			}
		}
	})
	return d.SecurityStats
}

func (d *DbStats) SharedBucketImport() *SharedBucketImportStats {
	d.initSharedBucketImportStats.Do(func() {
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
	})
	return d.SharedBucketImportStats
}

func (d *DbStats) GSIStats(queryName string) *QueryStat {
	d.initGsiStats.Do(func() {
		if d.GsiStats == nil {
			d.GsiStats = &GsiStats{
				Stats: map[string]*QueryStat{},
			}
		}
	})

	d.GsiStats.mutex.Lock()
	if _, ok := d.GsiStats.Stats[queryName]; !ok {
		dbName := d.dbName
		d.GsiStats.Stats[queryName] = &QueryStat{
			QueryCount:      NewIntStat("gsi_views", queryName+"_query_count", dbName, prometheus.CounterValue, 0),
			QueryErrorCount: NewIntStat("gsi_views", queryName+"_query_error_count", dbName, prometheus.CounterValue, 0),
			QueryTime:       NewIntStat("gsi_views", queryName+"_query_time", dbName, prometheus.CounterValue, 0),
		}
	}
	d.GsiStats.mutex.Unlock()

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
