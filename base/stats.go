/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"expvar"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	StatViewFormat = "%s.%s"

	NamespaceKey                 = "sgw"
	ResourceUtilizationSubsystem = "resource_utilization"

	SubsystemCacheKey           = "cache"
	SubsystemDatabaseKey        = "database"
	SubsystemDeltaSyncKey       = "delta_sync"
	SubsystemGSIViews           = "gsi_views"
	SubsystemReplication        = "replication"
	SubsystemReplicationPull    = "replication_pull"
	SubsystemReplicationPush    = "replication_push"
	SubsystemSecurity           = "security"
	SubsystemSharedBucketImport = "shared_bucket_import"

	DatabaseLabelKey    = "database"
	ReplicationLabelKey = "replication"
)

const (
	// StatsReplication (SGR 1.x)
	StatKeySgrActive                     = "sgr_active"
	StatKeySgrNumAttachmentsTransferred  = "sgr_num_attachments_transferred"
	StatKeySgrAttachmentBytesTransferred = "sgr_num_attachment_bytes_transferred"

	// StatsReplication (SGR 1.x and 2.x)
	StatKeySgrNumDocsPushed       = "sgr_num_docs_pushed"
	StatKeySgrNumDocsFailedToPush = "sgr_num_docs_failed_to_push"
	StatKeySgrDocsCheckedSent     = "sgr_docs_checked_sent"
)

const StatsGroupKeySyncGateway = "syncgateway"

type SgwStats struct {
	GlobalStats     *GlobalStat         `json:"global"`
	DbStats         map[string]*DbStats `json:"per_db"`
	ReplicatorStats *ReplicatorStats    `json:"per_replication,omitempty"`

	dbStatsMapMutex sync.Mutex
}

var SyncGatewayStats SgwStats

// SkipPrometheusStatsRegistration used to avoid registering a stat with prometheus
// Defaults to false - Only intended for test use
// Note that due to NewSyncGatewayStats() being ran as part of init the initial stats will still be registered and so
// global stats are unaffected
var SkipPrometheusStatsRegistration bool

func NewSyncGatewayStats() *SgwStats {
	sgwStats := SgwStats{
		GlobalStats: &GlobalStat{},
		DbStats:     map[string]*DbStats{},
	}

	sgwStats.GlobalStats.initResourceUtilizationStats()
	sgwStats.initReplicationStats()

	// This provides a stat for sgw_up where the value will be fixed to one. This is to allow backwards compatibility
	// where the standalone exporter would export a value of 1 if it has contact with SGW.
	NewIntStat("", "up", nil, nil, prometheus.GaugeValue, 1)

	return &sgwStats
}

func init() {
	// Initialize Sync Gateway Stats

	// All stats will be stored as part of this struct. Global variable accessible everywhere. To add stats see stats.go
	SyncGatewayStats = *NewSyncGatewayStats()

	// Publish our stats to expvars. This will run String method on SyncGatewayStats ( type SgwStats ) which will
	// marshal the stats to JSON
	expvar.Publish(StatsGroupKeySyncGateway, &SyncGatewayStats)
}

// This String() is to satisfy the expvar.Var interface which is used to produce the expvar endpoint output.
func (s *SgwStats) String() string {
	s.dbStatsMapMutex.Lock()
	bytes, err := JSONMarshalCanonical(s)
	s.dbStatsMapMutex.Unlock()
	if err != nil {
		Errorf("Unable to Marshal SgwStats: %v", err)
		return "null"
	}
	return string(bytes)
}

type GlobalStat struct {
	ResourceUtilization *ResourceUtilization `json:"resource_utilization"`
}

func (g *GlobalStat) initResourceUtilizationStats() {
	g.ResourceUtilization = &ResourceUtilization{
		AdminNetworkInterfaceBytesReceived:  NewIntStat(ResourceUtilizationSubsystem, "admin_net_bytes_recv", nil, nil, prometheus.CounterValue, 0),
		AdminNetworkInterfaceBytesSent:      NewIntStat(ResourceUtilizationSubsystem, "admin_net_bytes_sent", nil, nil, prometheus.CounterValue, 0),
		ErrorCount:                          NewIntStat(ResourceUtilizationSubsystem, "error_count", nil, nil, prometheus.CounterValue, 0),
		GoMemstatsHeapAlloc:                 NewIntStat(ResourceUtilizationSubsystem, "go_memstats_heapalloc", nil, nil, prometheus.GaugeValue, 0),
		GoMemstatsHeapIdle:                  NewIntStat(ResourceUtilizationSubsystem, "go_memstats_heapidle", nil, nil, prometheus.GaugeValue, 0),
		GoMemstatsHeapInUse:                 NewIntStat(ResourceUtilizationSubsystem, "go_memstats_heapinuse", nil, nil, prometheus.GaugeValue, 0),
		GoMemstatsHeapReleased:              NewIntStat(ResourceUtilizationSubsystem, "go_memstats_heapreleased", nil, nil, prometheus.GaugeValue, 0),
		GoMemstatsPauseTotalNS:              NewIntStat(ResourceUtilizationSubsystem, "go_memstats_pausetotalns", nil, nil, prometheus.GaugeValue, 0),
		GoMemstatsStackInUse:                NewIntStat(ResourceUtilizationSubsystem, "go_memstats_stackinuse", nil, nil, prometheus.GaugeValue, 0),
		GoMemstatsStackSys:                  NewIntStat(ResourceUtilizationSubsystem, "go_memstats_stacksys", nil, nil, prometheus.GaugeValue, 0),
		GoMemstatsSys:                       NewIntStat(ResourceUtilizationSubsystem, "go_memstats_sys", nil, nil, prometheus.GaugeValue, 0),
		GoroutinesHighWatermark:             NewIntStat(ResourceUtilizationSubsystem, "goroutines_high_watermark", nil, nil, prometheus.GaugeValue, 0),
		NumGoroutines:                       NewIntStat(ResourceUtilizationSubsystem, "num_goroutines", nil, nil, prometheus.GaugeValue, 0),
		ProcessMemoryResident:               NewIntStat(ResourceUtilizationSubsystem, "process_memory_resident", nil, nil, prometheus.GaugeValue, 0),
		PublicNetworkInterfaceBytesReceived: NewIntStat(ResourceUtilizationSubsystem, "pub_net_bytes_recv", nil, nil, prometheus.CounterValue, 0),
		PublicNetworkInterfaceBytesSent:     NewIntStat(ResourceUtilizationSubsystem, "pub_net_bytes_sent", nil, nil, prometheus.CounterValue, 0),
		SystemMemoryTotal:                   NewIntStat(ResourceUtilizationSubsystem, "system_memory_total", nil, nil, prometheus.GaugeValue, 0),
		WarnCount:                           NewIntStat(ResourceUtilizationSubsystem, "warn_count", nil, nil, prometheus.CounterValue, 0),
		CpuPercentUtil:                      NewFloatStat(ResourceUtilizationSubsystem, "process_cpu_percent_utilization", nil, nil, prometheus.GaugeValue, 0),
		Uptime:                              NewDurStat(ResourceUtilizationSubsystem, "uptime", nil, nil, prometheus.CounterValue, time.Now()),
	}
}

func (g *GlobalStat) ResourceUtilizationStats() *ResourceUtilization {
	return g.ResourceUtilization
}

func (s *SgwStats) initReplicationStats() {
	s.ReplicatorStats = &ReplicatorStats{
		new(expvar.Map).Init(),
	}
	prometheus.MustRegister(s.ReplicatorStats)
}

func (s *SgwStats) ReplicationStats() *expvar.Map {
	return s.ReplicatorStats.Map
}

type ResourceUtilization struct {
	AdminNetworkInterfaceBytesReceived  *SgwIntStat   `json:"admin_net_bytes_recv"`
	AdminNetworkInterfaceBytesSent      *SgwIntStat   `json:"admin_net_bytes_sent"`
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
	CpuPercentUtil                      *SgwFloatStat `json:"process_cpu_percent_utilization"`
	ProcessMemoryResident               *SgwIntStat   `json:"process_memory_resident"`
	PublicNetworkInterfaceBytesReceived *SgwIntStat   `json:"pub_net_bytes_recv"`
	PublicNetworkInterfaceBytesSent     *SgwIntStat   `json:"pub_net_bytes_sent"`
	SystemMemoryTotal                   *SgwIntStat   `json:"system_memory_total"`
	WarnCount                           *SgwIntStat   `json:"warn_count"`
	Uptime                              *SgwDurStat   `json:"uptime"`
}

type DbStats struct {
	dbName                  string
	CacheStats              *CacheStats                   `json:"cache,omitempty"`
	CBLReplicationPullStats *CBLReplicationPullStats      `json:"cbl_replication_pull,omitempty"`
	CBLReplicationPushStats *CBLReplicationPushStats      `json:"cbl_replication_push,omitempty"`
	DatabaseStats           *DatabaseStats                `json:"database,omitempty"`
	DeltaSyncStats          *DeltaSyncStats               `json:"delta_sync,omitempty"`
	QueryStats              *QueryStats                   `json:"gsi_views,omitempty"`
	DbReplicatorStats       map[string]*DbReplicatorStats `json:"replications,omitempty"`
	SecurityStats           *SecurityStats                `json:"security,omitempty"`
	SharedBucketImportStats *SharedBucketImportStats      `json:"shared_bucket_import,omitempty"`
}

type CacheStats struct {
	AbandonedSeqs                       *SgwIntStat `json:"abandoned_seqs"`
	ChannelCacheRevsActive              *SgwIntStat `json:"chan_cache_active_revs"`
	ChannelCacheBypassCount             *SgwIntStat `json:"chan_cache_bypass_count"`
	ChannelCacheChannelsAdded           *SgwIntStat `json:"chan_cache_channels_added"`
	ChannelCacheChannelsEvictedInactive *SgwIntStat `json:"chan_cache_channels_evicted_inactive"`
	ChannelCacheChannelsEvictedNRU      *SgwIntStat `json:"chan_cache_channels_evicted_nru"`
	ChannelCacheCompactCount            *SgwIntStat `json:"chan_cache_compact_count"`
	ChannelCacheCompactTime             *SgwIntStat `json:"chan_cache_compact_time"`
	ChannelCacheHits                    *SgwIntStat `json:"chan_cache_hits"`
	ChannelCacheMaxEntries              *SgwIntStat `json:"chan_cache_max_entries"`
	ChannelCacheMisses                  *SgwIntStat `json:"chan_cache_misses"`
	ChannelCacheNumChannels             *SgwIntStat `json:"chan_cache_num_channels"`
	ChannelCachePendingQueries          *SgwIntStat `json:"chan_cache_pending_queries"`
	ChannelCacheRevsRemoval             *SgwIntStat `json:"chan_cache_removal_revs"`
	ChannelCacheRevsTombstone           *SgwIntStat `json:"chan_cache_tombstone_revs"`
	HighSeqCached                       *SgwIntStat `json:"high_seq_cached"`
	HighSeqStable                       *SgwIntStat `json:"high_seq_stable"`
	NonMobileIgnoredCount               *SgwIntStat `json:"non_mobile_ignored_count"`
	NumActiveChannels                   *SgwIntStat `json:"num_active_channels"`
	NumSkippedSeqs                      *SgwIntStat `json:"num_skipped_seqs"`
	PendingSeqLen                       *SgwIntStat `json:"pending_seq_len"`
	RevisionCacheBypass                 *SgwIntStat `json:"rev_cache_bypass"`
	RevisionCacheHits                   *SgwIntStat `json:"rev_cache_hits"`
	RevisionCacheMisses                 *SgwIntStat `json:"rev_cache_misses"`
	SkippedSeqLen                       *SgwIntStat `json:"skipped_seq_len"`
	ViewQueries                         *SgwIntStat `json:"view_queries"`
}

type CBLReplicationPullStats struct {
	AttachmentPullBytes         *SgwIntStat `json:"attachment_pull_bytes"`
	AttachmentPullCount         *SgwIntStat `json:"attachment_pull_count"`
	MaxPending                  *SgwIntStat `json:"max_pending"`
	NumReplicationsActive       *SgwIntStat `json:"num_replications_active"`
	NumPullReplActiveContinuous *SgwIntStat `json:"num_pull_repl_active_continuous"`
	NumPullReplActiveOneShot    *SgwIntStat `json:"num_pull_repl_active_one_shot"`
	NumPullReplCaughtUp         *SgwIntStat `json:"num_pull_repl_caught_up"`
	NumPullReplTotalCaughtUp    *SgwIntStat `json:"num_pull_repl_total_caught_up"`
	NumPullReplSinceZero        *SgwIntStat `json:"num_pull_repl_since_zero"`
	NumPullReplTotalContinuous  *SgwIntStat `json:"num_pull_repl_total_continuous"`
	NumPullReplTotalOneShot     *SgwIntStat `json:"num_pull_repl_total_one_shot"`
	RequestChangesCount         *SgwIntStat `json:"request_changes_count"`
	RequestChangesTime          *SgwIntStat `json:"request_changes_time"`
	RevProcessingTime           *SgwIntStat `json:"rev_processing_time"`
	RevSendCount                *SgwIntStat `json:"rev_send_count"`
	RevErrorCount               *SgwIntStat `json:"rev_error_count"`
	RevSendLatency              *SgwIntStat `json:"rev_send_latency"`
}

type CBLReplicationPushStats struct {
	AttachmentPushBytes *SgwIntStat `json:"attachment_push_bytes"`
	AttachmentPushCount *SgwIntStat `json:"attachment_push_count"`
	DocPushCount        *SgwIntStat `json:"doc_push_count"`
	DocPushErrorCount   *SgwIntStat `json:"doc_push_error_count"`
	ProposeChangeCount  *SgwIntStat `json:"propose_change_count"`
	ProposeChangeTime   *SgwIntStat `json:"propose_change_time"`
	WriteProcessingTime *SgwIntStat `json:"write_processing_time"`
}

type DatabaseStats struct {
	CompactionAttachmentStartTime *SgwIntStat `json:"compaction_attachment_start_time"`
	CompactionTombstoneStartTime  *SgwIntStat `json:"compaction_tombstone_start_time"`
	ConflictWriteCount            *SgwIntStat `json:"conflict_write_count"`
	Crc32MatchCount               *SgwIntStat `json:"crc32c_match_count"`
	DCPCachingCount               *SgwIntStat `json:"dcp_caching_count"`
	DCPCachingTime                *SgwIntStat `json:"dcp_caching_time"`
	DCPReceivedCount              *SgwIntStat `json:"dcp_received_count"`
	DCPReceivedTime               *SgwIntStat `json:"dcp_received_time"`
	DocReadsBytesBlip             *SgwIntStat `json:"doc_reads_bytes_blip"`
	DocWritesBytes                *SgwIntStat `json:"doc_writes_bytes"`
	DocWritesBytesBlip            *SgwIntStat `json:"doc_writes_bytes_blip"`
	DocWritesXattrBytes           *SgwIntStat `json:"doc_writes_xattr_bytes"`
	HighSeqFeed                   *SgwIntStat `json:"high_seq_feed"`
	NumAttachmentsCompacted       *SgwIntStat `json:"num_attachments_compacted"`
	NumDocReadsBlip               *SgwIntStat `json:"num_doc_reads_blip"`
	NumDocReadsRest               *SgwIntStat `json:"num_doc_reads_rest"`
	NumDocWrites                  *SgwIntStat `json:"num_doc_writes"`
	NumReplicationsActive         *SgwIntStat `json:"num_replications_active"`
	NumReplicationsTotal          *SgwIntStat `json:"num_replications_total"`
	NumTombstonesCompacted        *SgwIntStat `json:"num_tombstones_compacted"`
	SequenceAssignedCount         *SgwIntStat `json:"sequence_assigned_count"`
	SequenceGetCount              *SgwIntStat `json:"sequence_get_count"`
	SequenceIncrCount             *SgwIntStat `json:"sequence_incr_count"`
	SequenceReleasedCount         *SgwIntStat `json:"sequence_released_count"`
	SequenceReservedCount         *SgwIntStat `json:"sequence_reserved_count"`
	WarnChannelNameSizeCount      *SgwIntStat `json:"warn_channel_name_size_count"`
	WarnChannelsPerDocCount       *SgwIntStat `json:"warn_channels_per_doc_count"`
	WarnGrantsPerDocCount         *SgwIntStat `json:"warn_grants_per_doc_count"`
	WarnXattrSizeCount            *SgwIntStat `json:"warn_xattr_size_count"`
	SyncFunctionCount             *SgwIntStat `json:"sync_function_count"`
	SyncFunctionTime              *SgwIntStat `json:"sync_function_time"`

	// These can be cleaned up in future versions of SGW, implemented as maps to reduce amount of potential risk
	// prior to Hydrogen release. These are not exported as part of prometheus and only exposed through expvars
	CacheFeedMapStats  *ExpVarMapWrapper `json:"cache_feed"`
	ImportFeedMapStats *ExpVarMapWrapper `json:"import_feed"`
}

// This wrapper ensures that an expvar.Map type can be marshalled into JSON. The expvar.Map has no method to go direct to
// JSON however the map.String() method returns JSON and so we can use this.
type ExpVarMapWrapper struct {
	*expvar.Map
}

func (wrapper *ExpVarMapWrapper) MarshalJSON() ([]byte, error) {
	return []byte(wrapper.String()), nil
}

type ReplicatorStats struct {
	*expvar.Map
}

func (rs *ReplicatorStats) GetDesc(value expvar.KeyValue) []*prometheus.Desc {
	constLabels := make(prometheus.Labels)
	constLabels[ReplicationLabelKey] = value.Key

	checkedSentDesc := prometheus.NewDesc(prometheus.BuildFQName(NamespaceKey, SubsystemReplication, "sgr_docs_checked_sent"), "sgr_docs_checked_sent", nil, constLabels)
	numAttachmentBytesTransferred := prometheus.NewDesc(prometheus.BuildFQName(NamespaceKey, SubsystemReplication, "sgr_num_attachment_bytes_transferred"), "sgr_num_attachment_bytes_transferred", nil, constLabels)
	numAttachmentsTransferred := prometheus.NewDesc(prometheus.BuildFQName(NamespaceKey, SubsystemReplication, "sgr_num_attachments_transferred"), "sgr_num_attachments_transferred", nil, constLabels)
	numDocsFailedToPush := prometheus.NewDesc(prometheus.BuildFQName(NamespaceKey, SubsystemReplication, "sgr_num_docs_failed_to_push"), "sgr_num_docs_failed_to_push", nil, constLabels)
	numDocsPushed := prometheus.NewDesc(prometheus.BuildFQName(NamespaceKey, SubsystemReplication, "sgr_num_docs_pushed"), "sgr_num_docs_pushed", nil, constLabels)

	return []*prometheus.Desc{checkedSentDesc, numAttachmentBytesTransferred, numAttachmentsTransferred, numDocsFailedToPush, numDocsPushed}
}

func (rs *ReplicatorStats) MarshalJSON() ([]byte, error) {
	return []byte(rs.String()), nil
}

func (rs *ReplicatorStats) Describe(ch chan<- *prometheus.Desc) {
	rs.Do(func(value expvar.KeyValue) {
		descriptions := rs.GetDesc(value)
		ch <- descriptions[0]
		ch <- descriptions[1]
		ch <- descriptions[2]
		ch <- descriptions[3]
		ch <- descriptions[4]
	})
}

func (rs *ReplicatorStats) Collect(ch chan<- prometheus.Metric) {
	rs.Do(func(value expvar.KeyValue) {
		descriptions := rs.GetDesc(value)
		ch <- prometheus.MustNewConstMetric(
			descriptions[0],
			prometheus.CounterValue,
			float64(value.Value.(*expvar.Map).Get("sgr_docs_checked_sent").(*expvar.Int).Value()),
		)
		ch <- prometheus.MustNewConstMetric(
			descriptions[1],
			prometheus.CounterValue,
			float64(value.Value.(*expvar.Map).Get("sgr_num_attachment_bytes_transferred").(*expvar.Int).Value()),
		)
		ch <- prometheus.MustNewConstMetric(
			descriptions[2],
			prometheus.CounterValue,
			float64(value.Value.(*expvar.Map).Get("sgr_num_attachments_transferred").(*expvar.Int).Value()),
		)
		ch <- prometheus.MustNewConstMetric(
			descriptions[3],
			prometheus.CounterValue,
			float64(value.Value.(*expvar.Map).Get("sgr_num_docs_failed_to_push").(*expvar.Int).Value()),
		)
		ch <- prometheus.MustNewConstMetric(
			descriptions[4],
			prometheus.CounterValue,
			float64(value.Value.(*expvar.Map).Get("sgr_num_docs_pushed").(*expvar.Int).Value()),
		)
	})
}

type DeltaSyncStats struct {
	DeltaCacheHit             *SgwIntStat `json:"delta_cache_hit"`
	DeltaCacheMiss            *SgwIntStat `json:"delta_cache_miss"`
	DeltaPullReplicationCount *SgwIntStat `json:"delta_pull_replication_count"`
	DeltaPushDocCount         *SgwIntStat `json:"delta_push_doc_count"`
	DeltasRequested           *SgwIntStat `json:"deltas_requested"`
	DeltasSent                *SgwIntStat `json:"deltas_sent"`
}

type QueryStats struct {
	Stats map[string]*QueryStat
	mutex sync.Mutex
}

type DbReplicatorStats struct {
	NumAttachmentBytesPushed *SgwIntStat `json:"sgr_num_attachment_bytes_pushed"`
	NumAttachmentPushed      *SgwIntStat `json:"sgr_num_attachments_pushed"`
	NumDocPushed             *SgwIntStat `json:"sgr_num_docs_pushed"`
	NumDocsFailedToPush      *SgwIntStat `json:"sgr_num_docs_failed_to_push"`
	PushConflictCount        *SgwIntStat `json:"sgr_push_conflict_count"`
	PushRejectedCount        *SgwIntStat `json:"sgr_push_rejected_count"`
	PushDeltaSentCount       *SgwIntStat `json:"sgr_deltas_sent"`
	DocsCheckedSent          *SgwIntStat `json:"sgr_docs_checked_sent" `
	NumConnectAttemptsPull   *SgwIntStat `json:"sgr_num_connect_attempts_pull"`
	NumReconnectsAbortedPull *SgwIntStat `json:"sgr_num_reconnects_aborted_pull"`

	NumAttachmentBytesPulled *SgwIntStat `json:"sgr_num_attachment_bytes_pulled"`
	NumAttachmentsPulled     *SgwIntStat `json:"sgr_num_attachments_pulled"`
	PulledCount              *SgwIntStat `json:"sgr_num_docs_pulled"`
	PurgedCount              *SgwIntStat `json:"sgr_num_docs_purged"`
	FailedToPullCount        *SgwIntStat `json:"sgr_num_docs_failed_to_pull"`
	DeltaReceivedCount       *SgwIntStat `json:"sgr_deltas_recv"`
	DeltaRequestedCount      *SgwIntStat `json:"sgr_deltas_requested"`
	DocsCheckedReceived      *SgwIntStat `json:"sgr_docs_checked_recv"`
	NumConnectAttemptsPush   *SgwIntStat `json:"sgr_num_connect_attempts_push"`
	NumReconnectsAbortedPush *SgwIntStat `json:"sgr_num_reconnects_aborted_push"`

	ConflictResolvedLocalCount  *SgwIntStat `json:"sgr_conflict_resolved_local_count"`
	ConflictResolvedRemoteCount *SgwIntStat `json:"sgr_conflict_resolved_remote_count"`
	ConflictResolvedMergedCount *SgwIntStat `json:"sgr_conflict_resolved_merge_count"`
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
	statFQN       string
	statDesc      *prometheus.Desc
	labelValues   []string
	statValueType prometheus.ValueType
}

type SgwIntStat struct {
	SgwStat
	AtomicInt
}

// uint64 is used here because atomic ints do not support floats. Floats are encoded to uint64
type SgwFloatStat struct {
	SgwStat
	Val uint64
}

// Just a bool wrapper. Prometheus doesn't support boolean metrics and so this just goes to expvars
type SgwBoolStat struct {
	Val bool
}

func newSGWStat(subsystem string, key string, labelKeys []string, labelVals []string, statValueType prometheus.ValueType) *SgwStat {
	name := prometheus.BuildFQName(NamespaceKey, subsystem, key)

	constLabels := make(prometheus.Labels)
	for i, labelKey := range labelKeys {
		constLabels[labelKey] = labelVals[i]
	}

	desc := prometheus.NewDesc(name, key, nil, constLabels)

	return &SgwStat{
		statFQN:       name,
		statDesc:      desc,
		labelValues:   labelVals,
		statValueType: statValueType,
	}
}

func NewIntStat(subsystem string, key string, labelKeys []string, labelVals []string, statValueType prometheus.ValueType, initialValue int64) *SgwIntStat {
	stat := &SgwIntStat{
		SgwStat: *newSGWStat(subsystem, key, labelKeys, labelVals, statValueType),
	}

	stat.Set(initialValue)

	if !SkipPrometheusStatsRegistration {
		prometheus.MustRegister(stat)
	}

	return stat
}

func (s *SgwIntStat) Describe(ch chan<- *prometheus.Desc) {
	ch <- s.statDesc
}

func (s *SgwIntStat) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(s.statDesc, s.statValueType, float64(s.Value()))
}

func (s *SgwIntStat) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatInt(s.Value(), 10)), nil
}

func (s *SgwIntStat) String() string {
	return strconv.FormatInt(s.Value(), 10)
}

func NewFloatStat(subsystem string, key string, labelKeys []string, labelVals []string, statValueType prometheus.ValueType, initialValue float64) *SgwFloatStat {
	stat := &SgwFloatStat{
		SgwStat: *newSGWStat(subsystem, key, labelKeys, labelVals, statValueType),
		Val:     math.Float64bits(initialValue),
	}

	if !SkipPrometheusStatsRegistration {
		prometheus.MustRegister(stat)
	}

	return stat
}

func (s *SgwFloatStat) Describe(ch chan<- *prometheus.Desc) {
	ch <- s.statDesc
}

func (s *SgwFloatStat) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(s.statDesc, s.statValueType, math.Float64frombits(atomic.LoadUint64(&s.Val)))
}

func (s *SgwFloatStat) Set(newV float64) {
	atomic.StoreUint64(&s.Val, math.Float64bits(newV))
}

func (s *SgwFloatStat) SetIfMax(newV float64) {
	for {
		cur := atomic.LoadUint64(&s.Val)
		curVal := math.Float64frombits(cur)

		if curVal >= newV {
			return
		}

		nxt := math.Float64bits(newV)
		if atomic.CompareAndSwapUint64(&s.Val, cur, nxt) {
			return
		}
	}
}

func (s *SgwFloatStat) Add(delta float64) {
	for {
		cur := atomic.LoadUint64(&s.Val)
		curVal := math.Float64frombits(cur)
		nxtVal := curVal + delta
		nxt := math.Float64bits(nxtVal)
		if atomic.CompareAndSwapUint64(&s.Val, cur, nxt) {
			return
		}
	}
}

func (s *SgwFloatStat) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatFloat(math.Float64frombits(atomic.LoadUint64(&s.Val)), 'g', -1, 64)), nil
}

func (s *SgwFloatStat) String() string {
	return strconv.FormatFloat(math.Float64frombits(atomic.LoadUint64(&s.Val)), 'g', -1, 64)
}

func (s *SgwFloatStat) Value() float64 {
	return math.Float64frombits(atomic.LoadUint64(&s.Val))
}

// SgwDurStat is a wrapper around SgwStat for reporting time duration stats.
type SgwDurStat struct {
	SgwStat             // SGW stats for sending metrics to Prometheus.
	StartTime time.Time // Start time to calculate the time duration.
}

// NewDurStat creates a new collector for time duration metric, registers it with the
// Prometheus's DefaultRegisterer and returns the collector. It panics if any error
// occurs while registering the collector on Prometheus registry.
func NewDurStat(subsystem string, key string, labelKeys []string, labelVals []string,
	statValueType prometheus.ValueType, initialValue time.Time) *SgwDurStat {
	stat := &SgwDurStat{
		SgwStat:   *newSGWStat(subsystem, key, labelKeys, labelVals, statValueType),
		StartTime: initialValue,
	}

	if !SkipPrometheusStatsRegistration {
		prometheus.MustRegister(stat)
	}

	return stat
}

func (s *SgwDurStat) Describe(ch chan<- *prometheus.Desc) {
	ch <- s.statDesc
}

func (s *SgwDurStat) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(s.statDesc, s.statValueType, float64(time.Since(s.StartTime)))
}

// MarshalJSON returns the JSON encoding of duration - the time elapsed since s.Val.
func (s *SgwDurStat) MarshalJSON() ([]byte, error) {
	return []byte(s.String()), nil
}

// String returns the String representation of duration - the time elapsed since s.Val.
func (s *SgwDurStat) String() string {
	return strconv.Itoa(int(time.Since(s.StartTime).Nanoseconds()))
}

type QueryStat struct {
	QueryCount      *SgwIntStat
	QueryErrorCount *SgwIntStat
	QueryTime       *SgwIntStat
}

func (s *SgwStats) NewDBStats(name string, deltaSyncEnabled bool, importEnabled bool, viewsEnabled bool, queryNames ...string) *DbStats {
	s.dbStatsMapMutex.Lock()
	defer s.dbStatsMapMutex.Unlock()
	s.DbStats[name] = &DbStats{
		dbName: name,
	}

	// These have a pretty good chance of being used so we'll initialise these for every database stat struct created
	s.DbStats[name].initCacheStats()
	s.DbStats[name].initCBLReplicationPullStats()
	s.DbStats[name].initCBLReplicationPushStats()
	s.DbStats[name].initDatabaseStats()
	s.DbStats[name].initSecurityStats()

	if deltaSyncEnabled {
		s.DbStats[name].InitDeltaSyncStats()
	}

	if importEnabled {
		s.DbStats[name].InitSharedBucketImportStats()
	}

	if viewsEnabled {
		s.DbStats[name].InitQueryStats(
			true,
			queryNames...,
		)
	} else {
		s.DbStats[name].InitQueryStats(
			false,
			queryNames...,
		)
	}

	return s.DbStats[name]
}

func (s *SgwStats) ClearDBStats(name string) {
	s.dbStatsMapMutex.Lock()
	defer s.dbStatsMapMutex.Unlock()

	if _, ok := s.DbStats[name]; !ok {
		return
	}

	s.DbStats[name].unregisterCacheStats()
	s.DbStats[name].unregisterCBLReplicationPullStats()
	s.DbStats[name].unregisterCBLReplicationPushStats()
	for replName := range s.DbStats[name].DbReplicatorStats {
		s.DbStats[name].unregisterReplicationStats(replName)
	}
	s.DbStats[name].unregisterDatabaseStats()
	s.DbStats[name].unregisterSecurityStats()

	if s.DbStats[name].DeltaSyncStats != nil {
		s.DbStats[name].unregisterDeltaSyncStats()
	}

	if s.DbStats[name].SharedBucketImportStats != nil {
		s.DbStats[name].unregisterSharedBucketImportStats()
	}

	s.DbStats[name].unregisterQueryStats()

	delete(s.DbStats, name)

}

// Removes the per-database stats for this database by removing the database from the map
func RemovePerDbStats(dbName string) {

	// Clear out the stats for this db since they will no longer be updated.
	SyncGatewayStats.ClearDBStats(dbName)

}

func (d *DbStats) initCacheStats() {
	labelKeys := []string{DatabaseLabelKey}
	labelVals := []string{d.dbName}
	d.CacheStats = &CacheStats{
		AbandonedSeqs:                       NewIntStat(SubsystemCacheKey, "abandoned_seqs", labelKeys, labelVals, prometheus.CounterValue, 0),
		ChannelCacheRevsActive:              NewIntStat(SubsystemCacheKey, "chan_cache_active_revs", labelKeys, labelVals, prometheus.GaugeValue, 0),
		ChannelCacheBypassCount:             NewIntStat(SubsystemCacheKey, "chan_cache_bypass_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		ChannelCacheChannelsAdded:           NewIntStat(SubsystemCacheKey, "chan_cache_channels_added", labelKeys, labelVals, prometheus.CounterValue, 0),
		ChannelCacheChannelsEvictedInactive: NewIntStat(SubsystemCacheKey, "chan_cache_channels_evicted_inactive", labelKeys, labelVals, prometheus.CounterValue, 0),
		ChannelCacheChannelsEvictedNRU:      NewIntStat(SubsystemCacheKey, "chan_cache_channels_evicted_nru", labelKeys, labelVals, prometheus.CounterValue, 0),
		ChannelCacheCompactCount:            NewIntStat(SubsystemCacheKey, "chan_cache_compact_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		ChannelCacheCompactTime:             NewIntStat(SubsystemCacheKey, "chan_cache_compact_time", labelKeys, labelVals, prometheus.CounterValue, 0),
		ChannelCacheHits:                    NewIntStat(SubsystemCacheKey, "chan_cache_hits", labelKeys, labelVals, prometheus.CounterValue, 0),
		ChannelCacheMaxEntries:              NewIntStat(SubsystemCacheKey, "chan_cache_max_entries", labelKeys, labelVals, prometheus.GaugeValue, 0),
		ChannelCacheMisses:                  NewIntStat(SubsystemCacheKey, "chan_cache_misses", labelKeys, labelVals, prometheus.CounterValue, 0),
		ChannelCacheNumChannels:             NewIntStat(SubsystemCacheKey, "chan_cache_num_channels", labelKeys, labelVals, prometheus.GaugeValue, 0),
		ChannelCachePendingQueries:          NewIntStat(SubsystemCacheKey, "chan_cache_pending_queries", labelKeys, labelVals, prometheus.GaugeValue, 0),
		ChannelCacheRevsRemoval:             NewIntStat(SubsystemCacheKey, "chan_cache_removal_revs", labelKeys, labelVals, prometheus.GaugeValue, 0),
		ChannelCacheRevsTombstone:           NewIntStat(SubsystemCacheKey, "chan_cache_tombstone_revs", labelKeys, labelVals, prometheus.GaugeValue, 0),
		HighSeqCached:                       NewIntStat(SubsystemCacheKey, "high_seq_cached", labelKeys, labelVals, prometheus.CounterValue, 0),
		HighSeqStable:                       NewIntStat(SubsystemCacheKey, "high_seq_stable", labelKeys, labelVals, prometheus.CounterValue, 0),
		NonMobileIgnoredCount:               NewIntStat(SubsystemCacheKey, "non_mobile_ignored_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		NumActiveChannels:                   NewIntStat(SubsystemCacheKey, "num_active_channels", labelKeys, labelVals, prometheus.GaugeValue, 0),
		NumSkippedSeqs:                      NewIntStat(SubsystemCacheKey, "num_skipped_seqs", labelKeys, labelVals, prometheus.CounterValue, 0),
		PendingSeqLen:                       NewIntStat(SubsystemCacheKey, "pending_seq_len", labelKeys, labelVals, prometheus.GaugeValue, 0),
		RevisionCacheBypass:                 NewIntStat(SubsystemCacheKey, "rev_cache_bypass", labelKeys, labelVals, prometheus.GaugeValue, 0),
		RevisionCacheHits:                   NewIntStat(SubsystemCacheKey, "rev_cache_hits", labelKeys, labelVals, prometheus.CounterValue, 0),
		RevisionCacheMisses:                 NewIntStat(SubsystemCacheKey, "rev_cache_misses", labelKeys, labelVals, prometheus.CounterValue, 0),
		SkippedSeqLen:                       NewIntStat(SubsystemCacheKey, "skipped_seq_len", labelKeys, labelVals, prometheus.GaugeValue, 0),
		ViewQueries:                         NewIntStat(SubsystemCacheKey, "view_queries", labelKeys, labelVals, prometheus.CounterValue, 0),
	}
}

func (d *DbStats) unregisterCacheStats() {
	prometheus.Unregister(d.CacheStats.AbandonedSeqs)
	prometheus.Unregister(d.CacheStats.ChannelCacheRevsActive)
	prometheus.Unregister(d.CacheStats.ChannelCacheBypassCount)
	prometheus.Unregister(d.CacheStats.ChannelCacheChannelsAdded)
	prometheus.Unregister(d.CacheStats.ChannelCacheChannelsEvictedInactive)
	prometheus.Unregister(d.CacheStats.ChannelCacheChannelsEvictedNRU)
	prometheus.Unregister(d.CacheStats.ChannelCacheCompactCount)
	prometheus.Unregister(d.CacheStats.ChannelCacheCompactTime)
	prometheus.Unregister(d.CacheStats.ChannelCacheHits)
	prometheus.Unregister(d.CacheStats.ChannelCacheMaxEntries)
	prometheus.Unregister(d.CacheStats.ChannelCacheMisses)
	prometheus.Unregister(d.CacheStats.ChannelCacheNumChannels)
	prometheus.Unregister(d.CacheStats.ChannelCachePendingQueries)
	prometheus.Unregister(d.CacheStats.ChannelCacheRevsRemoval)
	prometheus.Unregister(d.CacheStats.ChannelCacheRevsTombstone)
	prometheus.Unregister(d.CacheStats.HighSeqCached)
	prometheus.Unregister(d.CacheStats.HighSeqStable)
	prometheus.Unregister(d.CacheStats.NonMobileIgnoredCount)
	prometheus.Unregister(d.CacheStats.NumActiveChannels)
	prometheus.Unregister(d.CacheStats.NumSkippedSeqs)
	prometheus.Unregister(d.CacheStats.PendingSeqLen)
	prometheus.Unregister(d.CacheStats.RevisionCacheBypass)
	prometheus.Unregister(d.CacheStats.RevisionCacheHits)
	prometheus.Unregister(d.CacheStats.RevisionCacheMisses)
	prometheus.Unregister(d.CacheStats.SkippedSeqLen)
	prometheus.Unregister(d.CacheStats.ViewQueries)
}

func (d *DbStats) Cache() *CacheStats {
	return d.CacheStats
}

func (d *DbStats) initCBLReplicationPullStats() {
	labelKeys := []string{DatabaseLabelKey}
	labelVals := []string{d.dbName}
	d.CBLReplicationPullStats = &CBLReplicationPullStats{
		AttachmentPullBytes:         NewIntStat(SubsystemReplicationPull, "attachment_pull_bytes", labelKeys, labelVals, prometheus.CounterValue, 0),
		AttachmentPullCount:         NewIntStat(SubsystemReplicationPull, "attachment_pull_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		MaxPending:                  NewIntStat(SubsystemReplicationPull, "max_pending", labelKeys, labelVals, prometheus.GaugeValue, 0),
		NumReplicationsActive:       NewIntStat(SubsystemReplicationPull, "num_pull_repl_active_continuous", labelKeys, labelVals, prometheus.GaugeValue, 0),
		NumPullReplActiveContinuous: NewIntStat(SubsystemReplicationPull, "num_pull_repl_active_one_shot", labelKeys, labelVals, prometheus.GaugeValue, 0),
		NumPullReplActiveOneShot:    NewIntStat(SubsystemReplicationPull, "num_replications_active", labelKeys, labelVals, prometheus.GaugeValue, 0),
		NumPullReplCaughtUp:         NewIntStat(SubsystemReplicationPull, "num_pull_repl_caught_up", labelKeys, labelVals, prometheus.GaugeValue, 0),
		NumPullReplTotalCaughtUp:    NewIntStat(SubsystemReplicationPull, "num_pull_repl_total_caught_up", labelKeys, labelVals, prometheus.GaugeValue, 0),
		NumPullReplSinceZero:        NewIntStat(SubsystemReplicationPull, "num_pull_repl_since_zero", labelKeys, labelVals, prometheus.CounterValue, 0),
		NumPullReplTotalContinuous:  NewIntStat(SubsystemReplicationPull, "num_pull_repl_total_continuous", labelKeys, labelVals, prometheus.GaugeValue, 0),
		NumPullReplTotalOneShot:     NewIntStat(SubsystemReplicationPull, "num_pull_repl_total_one_shot", labelKeys, labelVals, prometheus.GaugeValue, 0),
		RequestChangesCount:         NewIntStat(SubsystemReplicationPull, "request_changes_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		RequestChangesTime:          NewIntStat(SubsystemReplicationPull, "request_changes_time", labelKeys, labelVals, prometheus.CounterValue, 0),
		RevProcessingTime:           NewIntStat(SubsystemReplicationPull, "rev_processing_time", labelKeys, labelVals, prometheus.GaugeValue, 0),
		RevSendCount:                NewIntStat(SubsystemReplicationPull, "rev_send_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		RevErrorCount:               NewIntStat(SubsystemReplicationPull, "rev_error_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		RevSendLatency:              NewIntStat(SubsystemReplicationPull, "rev_send_latency", labelKeys, labelVals, prometheus.CounterValue, 0),
	}
}

func (d *DbStats) unregisterCBLReplicationPullStats() {
	prometheus.Unregister(d.CBLReplicationPullStats.AttachmentPullBytes)
	prometheus.Unregister(d.CBLReplicationPullStats.AttachmentPullCount)
	prometheus.Unregister(d.CBLReplicationPullStats.MaxPending)
	prometheus.Unregister(d.CBLReplicationPullStats.NumReplicationsActive)
	prometheus.Unregister(d.CBLReplicationPullStats.NumPullReplActiveContinuous)
	prometheus.Unregister(d.CBLReplicationPullStats.NumPullReplActiveOneShot)
	prometheus.Unregister(d.CBLReplicationPullStats.NumPullReplCaughtUp)
	prometheus.Unregister(d.CBLReplicationPullStats.NumPullReplTotalCaughtUp)
	prometheus.Unregister(d.CBLReplicationPullStats.NumPullReplSinceZero)
	prometheus.Unregister(d.CBLReplicationPullStats.NumPullReplTotalContinuous)
	prometheus.Unregister(d.CBLReplicationPullStats.NumPullReplTotalOneShot)
	prometheus.Unregister(d.CBLReplicationPullStats.RequestChangesCount)
	prometheus.Unregister(d.CBLReplicationPullStats.RequestChangesTime)
	prometheus.Unregister(d.CBLReplicationPullStats.RevProcessingTime)
	prometheus.Unregister(d.CBLReplicationPullStats.RevSendCount)
	prometheus.Unregister(d.CBLReplicationPullStats.RevErrorCount)
	prometheus.Unregister(d.CBLReplicationPullStats.RevSendLatency)
}

func (d *DbStats) CBLReplicationPull() *CBLReplicationPullStats {
	return d.CBLReplicationPullStats
}

func (d *DbStats) initCBLReplicationPushStats() {
	labelKeys := []string{DatabaseLabelKey}
	labelVals := []string{d.dbName}
	d.CBLReplicationPushStats = &CBLReplicationPushStats{
		AttachmentPushBytes: NewIntStat(SubsystemReplicationPush, "attachment_push_bytes", labelKeys, labelVals, prometheus.CounterValue, 0),
		AttachmentPushCount: NewIntStat(SubsystemReplicationPush, "attachment_push_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		DocPushCount:        NewIntStat(SubsystemReplicationPush, "doc_push_count", labelKeys, labelVals, prometheus.GaugeValue, 0),
		DocPushErrorCount:   NewIntStat(SubsystemReplicationPush, "doc_push_error_count", labelKeys, labelVals, prometheus.GaugeValue, 0),
		ProposeChangeCount:  NewIntStat(SubsystemReplicationPush, "propose_change_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		ProposeChangeTime:   NewIntStat(SubsystemReplicationPush, "propose_change_time", labelKeys, labelVals, prometheus.CounterValue, 0),
		WriteProcessingTime: NewIntStat(SubsystemReplicationPush, "write_processing_time", labelKeys, labelVals, prometheus.GaugeValue, 0),
	}
}

func (d *DbStats) unregisterCBLReplicationPushStats() {
	prometheus.Unregister(d.CBLReplicationPushStats.AttachmentPushBytes)
	prometheus.Unregister(d.CBLReplicationPushStats.AttachmentPushCount)
	prometheus.Unregister(d.CBLReplicationPushStats.DocPushCount)
	prometheus.Unregister(d.CBLReplicationPushStats.DocPushErrorCount)
	prometheus.Unregister(d.CBLReplicationPushStats.ProposeChangeCount)
	prometheus.Unregister(d.CBLReplicationPushStats.ProposeChangeTime)
	prometheus.Unregister(d.CBLReplicationPushStats.WriteProcessingTime)
}

func (d *DbStats) CBLReplicationPush() *CBLReplicationPushStats {
	return d.CBLReplicationPushStats
}

func (d *DbStats) initDatabaseStats() {
	labelKeys := []string{DatabaseLabelKey}
	labelVals := []string{d.dbName}
	d.DatabaseStats = &DatabaseStats{
		CompactionAttachmentStartTime: NewIntStat(SubsystemDatabaseKey, "compaction_attachment_start_time", labelKeys, labelVals, prometheus.GaugeValue, 0),
		CompactionTombstoneStartTime:  NewIntStat(SubsystemDatabaseKey, "compaction_tombstone_start_time", labelKeys, labelVals, prometheus.GaugeValue, 0),
		ConflictWriteCount:            NewIntStat(SubsystemDatabaseKey, "conflict_write_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		Crc32MatchCount:               NewIntStat(SubsystemDatabaseKey, "crc32c_match_count", labelKeys, labelVals, prometheus.GaugeValue, 0),
		DCPCachingCount:               NewIntStat(SubsystemDatabaseKey, "dcp_caching_count", labelKeys, labelVals, prometheus.GaugeValue, 0),
		DCPCachingTime:                NewIntStat(SubsystemDatabaseKey, "dcp_caching_time", labelKeys, labelVals, prometheus.GaugeValue, 0),
		DCPReceivedCount:              NewIntStat(SubsystemDatabaseKey, "dcp_received_count", labelKeys, labelVals, prometheus.GaugeValue, 0),
		DCPReceivedTime:               NewIntStat(SubsystemDatabaseKey, "dcp_received_time", labelKeys, labelVals, prometheus.GaugeValue, 0),
		DocReadsBytesBlip:             NewIntStat(SubsystemDatabaseKey, "doc_reads_bytes_blip", labelKeys, labelVals, prometheus.CounterValue, 0),
		DocWritesBytes:                NewIntStat(SubsystemDatabaseKey, "doc_writes_bytes", labelKeys, labelVals, prometheus.CounterValue, 0),
		DocWritesXattrBytes:           NewIntStat(SubsystemDatabaseKey, "doc_writes_xattr_bytes", labelKeys, labelVals, prometheus.CounterValue, 0),
		HighSeqFeed:                   NewIntStat(SubsystemDatabaseKey, "high_seq_feed", labelKeys, labelVals, prometheus.CounterValue, 0),
		NumAttachmentsCompacted:       NewIntStat(SubsystemDatabaseKey, "num_attachments_compacted", labelKeys, labelVals, prometheus.CounterValue, 0),
		DocWritesBytesBlip:            NewIntStat(SubsystemDatabaseKey, "doc_writes_bytes_blip", labelKeys, labelVals, prometheus.CounterValue, 0),
		NumDocReadsBlip:               NewIntStat(SubsystemDatabaseKey, "num_doc_reads_blip", labelKeys, labelVals, prometheus.CounterValue, 0),
		NumDocReadsRest:               NewIntStat(SubsystemDatabaseKey, "num_doc_reads_rest", labelKeys, labelVals, prometheus.CounterValue, 0),
		NumDocWrites:                  NewIntStat(SubsystemDatabaseKey, "num_doc_writes", labelKeys, labelVals, prometheus.CounterValue, 0),
		NumReplicationsActive:         NewIntStat(SubsystemDatabaseKey, "num_replications_active", labelKeys, labelVals, prometheus.GaugeValue, 0),
		NumReplicationsTotal:          NewIntStat(SubsystemDatabaseKey, "num_replications_total", labelKeys, labelVals, prometheus.CounterValue, 0),
		NumTombstonesCompacted:        NewIntStat(SubsystemDatabaseKey, "num_tombstones_compacted", labelKeys, labelVals, prometheus.CounterValue, 0),
		SequenceAssignedCount:         NewIntStat(SubsystemDatabaseKey, "sequence_assigned_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		SequenceGetCount:              NewIntStat(SubsystemDatabaseKey, "sequence_get_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		SequenceIncrCount:             NewIntStat(SubsystemDatabaseKey, "sequence_incr_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		SequenceReleasedCount:         NewIntStat(SubsystemDatabaseKey, "sequence_released_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		SequenceReservedCount:         NewIntStat(SubsystemDatabaseKey, "sequence_reserved_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		WarnChannelNameSizeCount:      NewIntStat(SubsystemDatabaseKey, "warn_channel_name_size_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		WarnChannelsPerDocCount:       NewIntStat(SubsystemDatabaseKey, "warn_channels_per_doc_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		WarnGrantsPerDocCount:         NewIntStat(SubsystemDatabaseKey, "warn_grants_per_doc_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		WarnXattrSizeCount:            NewIntStat(SubsystemDatabaseKey, "warn_xattr_size_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		SyncFunctionCount:             NewIntStat(SubsystemDatabaseKey, "sync_function_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		SyncFunctionTime:              NewIntStat(SubsystemDatabaseKey, "sync_function_time", labelKeys, labelVals, prometheus.CounterValue, 0),
		ImportFeedMapStats:            &ExpVarMapWrapper{new(expvar.Map).Init()},
		CacheFeedMapStats:             &ExpVarMapWrapper{new(expvar.Map).Init()},
	}
}

func (d *DbStats) unregisterDatabaseStats() {
	prometheus.Unregister(d.DatabaseStats.CompactionAttachmentStartTime)
	prometheus.Unregister(d.DatabaseStats.CompactionTombstoneStartTime)
	prometheus.Unregister(d.DatabaseStats.ConflictWriteCount)
	prometheus.Unregister(d.DatabaseStats.Crc32MatchCount)
	prometheus.Unregister(d.DatabaseStats.DCPCachingCount)
	prometheus.Unregister(d.DatabaseStats.DCPCachingTime)
	prometheus.Unregister(d.DatabaseStats.DCPReceivedCount)
	prometheus.Unregister(d.DatabaseStats.DCPReceivedTime)
	prometheus.Unregister(d.DatabaseStats.DocReadsBytesBlip)
	prometheus.Unregister(d.DatabaseStats.DocWritesBytes)
	prometheus.Unregister(d.DatabaseStats.DocWritesXattrBytes)
	prometheus.Unregister(d.DatabaseStats.HighSeqFeed)
	prometheus.Unregister(d.DatabaseStats.DocWritesBytesBlip)
	prometheus.Unregister(d.DatabaseStats.NumAttachmentsCompacted)
	prometheus.Unregister(d.DatabaseStats.NumDocReadsBlip)
	prometheus.Unregister(d.DatabaseStats.NumDocReadsRest)
	prometheus.Unregister(d.DatabaseStats.NumDocWrites)
	prometheus.Unregister(d.DatabaseStats.NumReplicationsActive)
	prometheus.Unregister(d.DatabaseStats.NumReplicationsTotal)
	prometheus.Unregister(d.DatabaseStats.NumTombstonesCompacted)
	prometheus.Unregister(d.DatabaseStats.SequenceAssignedCount)
	prometheus.Unregister(d.DatabaseStats.SequenceGetCount)
	prometheus.Unregister(d.DatabaseStats.SequenceIncrCount)
	prometheus.Unregister(d.DatabaseStats.SequenceReleasedCount)
	prometheus.Unregister(d.DatabaseStats.SequenceReservedCount)
	prometheus.Unregister(d.DatabaseStats.WarnChannelNameSizeCount)
	prometheus.Unregister(d.DatabaseStats.WarnChannelsPerDocCount)
	prometheus.Unregister(d.DatabaseStats.WarnGrantsPerDocCount)
	prometheus.Unregister(d.DatabaseStats.WarnXattrSizeCount)
	prometheus.Unregister(d.DatabaseStats.SyncFunctionCount)
	prometheus.Unregister(d.DatabaseStats.SyncFunctionTime)
}

func (d *DbStats) Database() *DatabaseStats {
	return d.DatabaseStats
}

func (d *DbStats) InitDeltaSyncStats() {
	labelKeys := []string{DatabaseLabelKey}
	labelVals := []string{d.dbName}
	d.DeltaSyncStats = &DeltaSyncStats{
		DeltasRequested:           NewIntStat(SubsystemDeltaSyncKey, "deltas_requested", labelKeys, labelVals, prometheus.CounterValue, 0),
		DeltasSent:                NewIntStat(SubsystemDeltaSyncKey, "deltas_sent", labelKeys, labelVals, prometheus.CounterValue, 0),
		DeltaPullReplicationCount: NewIntStat(SubsystemDeltaSyncKey, "delta_pull_replication_count", labelKeys, labelVals, prometheus.CounterValue, 0),
		DeltaCacheHit:             NewIntStat(SubsystemDeltaSyncKey, "delta_cache_hit", labelKeys, labelVals, prometheus.CounterValue, 0),
		DeltaCacheMiss:            NewIntStat(SubsystemDeltaSyncKey, "delta_sync_miss", labelKeys, labelVals, prometheus.CounterValue, 0),
		DeltaPushDocCount:         NewIntStat(SubsystemDeltaSyncKey, "delta_push_doc_count", labelKeys, labelVals, prometheus.CounterValue, 0),
	}
}

func (d *DbStats) unregisterDeltaSyncStats() {
	prometheus.Unregister(d.DeltaSyncStats.DeltasRequested)
	prometheus.Unregister(d.DeltaSyncStats.DeltasSent)
	prometheus.Unregister(d.DeltaSyncStats.DeltaPullReplicationCount)
	prometheus.Unregister(d.DeltaSyncStats.DeltaCacheHit)
	prometheus.Unregister(d.DeltaSyncStats.DeltaCacheMiss)
	prometheus.Unregister(d.DeltaSyncStats.DeltaPushDocCount)
}

func (d *DbStats) DeltaSync() *DeltaSyncStats {
	return d.DeltaSyncStats
}

func (d *DbStats) initSecurityStats() {
	if d.SecurityStats == nil {
		labelKeys := []string{DatabaseLabelKey}
		labelVals := []string{d.dbName}
		d.SecurityStats = &SecurityStats{
			AuthFailedCount:  NewIntStat(SubsystemSecurity, "auth_failed_count", labelKeys, labelVals, prometheus.CounterValue, 0),
			AuthSuccessCount: NewIntStat(SubsystemSecurity, "auth_success_count", labelKeys, labelVals, prometheus.CounterValue, 0),
			NumAccessErrors:  NewIntStat(SubsystemSecurity, "num_access_errors", labelKeys, labelVals, prometheus.CounterValue, 0),
			NumDocsRejected:  NewIntStat(SubsystemSecurity, "num_docs_rejected", labelKeys, labelVals, prometheus.CounterValue, 0),
			TotalAuthTime:    NewIntStat(SubsystemSecurity, "total_auth_time", labelKeys, labelVals, prometheus.GaugeValue, 0),
		}
	}
}

func (d *DbStats) unregisterReplicationStats(replicationID string) {
	if d.DbReplicatorStats[replicationID] == nil {
		return
	}

	prometheus.Unregister(d.DbReplicatorStats[replicationID].NumAttachmentBytesPushed)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].NumAttachmentPushed)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].NumDocPushed)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].NumDocsFailedToPush)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].PushConflictCount)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].PushRejectedCount)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].PushDeltaSentCount)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].DocsCheckedSent)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].NumConnectAttemptsPush)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].NumReconnectsAbortedPush)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].NumAttachmentBytesPulled)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].NumAttachmentsPulled)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].PulledCount)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].PurgedCount)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].FailedToPullCount)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].DeltaReceivedCount)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].DeltaRequestedCount)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].DocsCheckedReceived)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].ConflictResolvedLocalCount)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].ConflictResolvedRemoteCount)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].ConflictResolvedMergedCount)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].NumConnectAttemptsPull)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].NumReconnectsAbortedPull)
}

func (d *DbStats) unregisterSecurityStats() {
	prometheus.Unregister(d.SecurityStats.AuthFailedCount)
	prometheus.Unregister(d.SecurityStats.AuthSuccessCount)
	prometheus.Unregister(d.SecurityStats.NumAccessErrors)
	prometheus.Unregister(d.SecurityStats.NumDocsRejected)
	prometheus.Unregister(d.SecurityStats.TotalAuthTime)
}

func (d *DbStats) DBReplicatorStats(replicationID string) *DbReplicatorStats {
	if d.DbReplicatorStats == nil {
		d.DbReplicatorStats = map[string]*DbReplicatorStats{}
	}

	if _, ok := d.DbReplicatorStats[replicationID]; !ok {
		labelKeys := []string{DatabaseLabelKey, ReplicationLabelKey}
		labelVals := []string{d.dbName, replicationID}
		d.DbReplicatorStats[replicationID] = &DbReplicatorStats{
			NumAttachmentBytesPushed:    NewIntStat(SubsystemReplication, "sgr_num_attachment_bytes_pushed", labelKeys, labelVals, prometheus.CounterValue, 0),
			NumAttachmentPushed:         NewIntStat(SubsystemReplication, "sgr_num_attachments_pushed", labelKeys, labelVals, prometheus.CounterValue, 0),
			NumDocPushed:                NewIntStat(SubsystemReplication, "sgr_num_docs_pushed", labelKeys, labelVals, prometheus.CounterValue, 0),
			NumDocsFailedToPush:         NewIntStat(SubsystemReplication, "sgr_num_docs_failed_to_push", labelKeys, labelVals, prometheus.CounterValue, 0),
			PushConflictCount:           NewIntStat(SubsystemReplication, "sgr_push_conflict_count", labelKeys, labelVals, prometheus.CounterValue, 0),
			PushRejectedCount:           NewIntStat(SubsystemReplication, "sgr_push_rejected_count", labelKeys, labelVals, prometheus.CounterValue, 0),
			PushDeltaSentCount:          NewIntStat(SubsystemReplication, "sgr_deltas_sent", labelKeys, labelVals, prometheus.CounterValue, 0),
			DocsCheckedSent:             NewIntStat(SubsystemReplication, "sgr_docs_checked_sent", labelKeys, labelVals, prometheus.CounterValue, 0),
			NumConnectAttemptsPush:      NewIntStat(SubsystemReplication, "sgr_num_connect_attempts_push", labelKeys, labelVals, prometheus.CounterValue, 0),
			NumReconnectsAbortedPush:    NewIntStat(SubsystemReplication, "sgr_num_reconnects_aborted_push", labelKeys, labelVals, prometheus.CounterValue, 0),
			NumAttachmentBytesPulled:    NewIntStat(SubsystemReplication, "sgr_num_attachment_bytes_pulled", labelKeys, labelVals, prometheus.CounterValue, 0),
			NumAttachmentsPulled:        NewIntStat(SubsystemReplication, "sgr_num_attachments_pulled", labelKeys, labelVals, prometheus.CounterValue, 0),
			PulledCount:                 NewIntStat(SubsystemReplication, "sgr_num_docs_pulled", labelKeys, labelVals, prometheus.CounterValue, 0),
			PurgedCount:                 NewIntStat(SubsystemReplication, "sgr_num_docs_purged", labelKeys, labelVals, prometheus.CounterValue, 0),
			FailedToPullCount:           NewIntStat(SubsystemReplication, "sgr_num_docs_failed_to_pull", labelKeys, labelVals, prometheus.CounterValue, 0),
			DeltaReceivedCount:          NewIntStat(SubsystemReplication, "sgr_deltas_recv", labelKeys, labelVals, prometheus.CounterValue, 0),
			DeltaRequestedCount:         NewIntStat(SubsystemReplication, "sgr_deltas_requested", labelKeys, labelVals, prometheus.CounterValue, 0),
			DocsCheckedReceived:         NewIntStat(SubsystemReplication, "sgr_docs_checked_recv", labelKeys, labelVals, prometheus.CounterValue, 0),
			ConflictResolvedLocalCount:  NewIntStat(SubsystemReplication, "sgr_conflict_resolved_local_count", labelKeys, labelVals, prometheus.CounterValue, 0),
			ConflictResolvedRemoteCount: NewIntStat(SubsystemReplication, "sgr_conflict_resolved_remote_count", labelKeys, labelVals, prometheus.CounterValue, 0),
			ConflictResolvedMergedCount: NewIntStat(SubsystemReplication, "sgr_conflict_resolved_merge_count", labelKeys, labelVals, prometheus.CounterValue, 0),
			NumConnectAttemptsPull:      NewIntStat(SubsystemReplication, "sgr_num_connect_attempts_pull", labelKeys, labelVals, prometheus.CounterValue, 0),
			NumReconnectsAbortedPull:    NewIntStat(SubsystemReplication, "sgr_num_reconnects_aborted_pull", labelKeys, labelVals, prometheus.CounterValue, 0),
		}
	}

	return d.DbReplicatorStats[replicationID]
}

// Reset replication stats to zero
func (dbr *DbReplicatorStats) Reset() {
	dbr.NumAttachmentBytesPushed.Set(0)
	dbr.NumAttachmentPushed.Set(0)
	dbr.NumDocPushed.Set(0)
	dbr.NumDocsFailedToPush.Set(0)
	dbr.PushConflictCount.Set(0)
	dbr.PushRejectedCount.Set(0)
	dbr.PushDeltaSentCount.Set(0)
	dbr.DocsCheckedSent.Set(0)
	dbr.NumAttachmentBytesPulled.Set(0)
	dbr.NumAttachmentsPulled.Set(0)
	dbr.PulledCount.Set(0)
	dbr.PurgedCount.Set(0)
	dbr.FailedToPullCount.Set(0)
	dbr.DeltaReceivedCount.Set(0)
	dbr.DeltaRequestedCount.Set(0)
	dbr.DocsCheckedReceived.Set(0)
	dbr.ConflictResolvedLocalCount.Set(0)
	dbr.ConflictResolvedRemoteCount.Set(0)
	dbr.ConflictResolvedMergedCount.Set(0)
}

func (d *DbStats) Security() *SecurityStats {
	return d.SecurityStats
}

func (d *DbStats) InitSharedBucketImportStats() {
	if d.SharedBucketImportStats == nil {
		labelKeys := []string{DatabaseLabelKey}
		labelVals := []string{d.dbName}
		d.SharedBucketImportStats = &SharedBucketImportStats{
			ImportCount:          NewIntStat(SubsystemSharedBucketImport, "import_count", labelKeys, labelVals, prometheus.CounterValue, 0),
			ImportCancelCAS:      NewIntStat(SubsystemSharedBucketImport, "import_cancel_cas", labelKeys, labelVals, prometheus.CounterValue, 0),
			ImportErrorCount:     NewIntStat(SubsystemSharedBucketImport, "import_error_count", labelKeys, labelVals, prometheus.CounterValue, 0),
			ImportProcessingTime: NewIntStat(SubsystemSharedBucketImport, "import_processing_time", labelKeys, labelVals, prometheus.GaugeValue, 0),
			ImportHighSeq:        NewIntStat(SubsystemSharedBucketImport, "import_high_seq", labelKeys, labelVals, prometheus.CounterValue, 0),
			ImportPartitions:     NewIntStat(SubsystemSharedBucketImport, "import_partitions", labelKeys, labelVals, prometheus.GaugeValue, 0),
		}
	}
}

func (d *DbStats) unregisterSharedBucketImportStats() {
	prometheus.Unregister(d.SharedBucketImportStats.ImportCount)
	prometheus.Unregister(d.SharedBucketImportStats.ImportCancelCAS)
	prometheus.Unregister(d.SharedBucketImportStats.ImportErrorCount)
	prometheus.Unregister(d.SharedBucketImportStats.ImportProcessingTime)
	prometheus.Unregister(d.SharedBucketImportStats.ImportHighSeq)
	prometheus.Unregister(d.SharedBucketImportStats.ImportPartitions)
}

func (d *DbStats) SharedBucketImport() *SharedBucketImportStats {
	return d.SharedBucketImportStats
}

func (d *DbStats) InitQueryStats(useViews bool, queryNames ...string) {
	d.QueryStats = &QueryStats{
		Stats: map[string]*QueryStat{},
	}
	d.QueryStats.mutex.Lock()
	for _, queryName := range queryNames {
		d._initQueryStat(useViews, queryName)
	}
	d.QueryStats.mutex.Unlock()
}

func (d *DbStats) _initQueryStat(useViews bool, queryName string) {
	if _, ok := d.QueryStats.Stats[queryName]; !ok {
		labelKeys := []string{DatabaseLabelKey}
		labelVals := []string{d.dbName}

		// Prometheus isn't happy with '.'s in the name and the '.'s come from the design doc version. Design doc isn't
		// reported to prometheus. Only the view name.
		prometheusKey := queryName
		if useViews {
			splitName := strings.Split(queryName, ".")
			prometheusKey = splitName[len(splitName)-1]
		}

		d.QueryStats.Stats[queryName] = &QueryStat{
			QueryCount:      NewIntStat(SubsystemGSIViews, prometheusKey+"_count", labelKeys, labelVals, prometheus.CounterValue, 0),
			QueryErrorCount: NewIntStat(SubsystemGSIViews, prometheusKey+"_error_count", labelKeys, labelVals, prometheus.CounterValue, 0),
			QueryTime:       NewIntStat(SubsystemGSIViews, prometheusKey+"_time", labelKeys, labelVals, prometheus.CounterValue, 0),
		}
	}
}

func (d *DbStats) unregisterQueryStats() {
	for _, stat := range d.QueryStats.Stats {
		prometheus.Unregister(stat.QueryCount)
		prometheus.Unregister(stat.QueryErrorCount)
		prometheus.Unregister(stat.QueryTime)
	}
}

func (d *DbStats) Query(queryName string) *QueryStat {
	return d.QueryStats.Stats[queryName]
}

func (g *QueryStats) MarshalJSON() ([]byte, error) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	ret := map[string]interface{}{}
	for queryName, queryMap := range g.Stats {
		ret[queryName+"_query_count"] = queryMap.QueryCount
		ret[queryName+"_query_error_count"] = queryMap.QueryErrorCount
		ret[queryName+"_query_time"] = queryMap.QueryTime
	}

	return JSONMarshalCanonical(ret)
}
