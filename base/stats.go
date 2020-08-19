package base

import (
	"expvar"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"

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
)

var (
	checkedSentDesc               *prometheus.Desc
	numAttachmentBytesTransferred *prometheus.Desc
	numAttachmentsTransferred     *prometheus.Desc
	numDocsFailedToPush           *prometheus.Desc
	numDocsPushed                 *prometheus.Desc
)

type SgwStats struct {
	GlobalStats     *GlobalStat         `json:"global"`
	DbStats         map[string]*DbStats `json:"per_db"`
	ReplicatorStats *ReplicatorStats    `json:"per_replication,omitempty"`

	dbStatsMapMutex sync.Mutex
}

func NewSyncGatewayStats() *SgwStats {
	sgwStats := SgwStats{
		GlobalStats: &GlobalStat{},
		DbStats:     map[string]*DbStats{},
	}

	checkedSentDesc = prometheus.NewDesc(prometheus.BuildFQName(NamespaceKey, SubsystemReplication, "sgr_docs_checked_sent"), "sgr_docs_checked_sent", []string{"replication"}, nil)
	numAttachmentBytesTransferred = prometheus.NewDesc(prometheus.BuildFQName(NamespaceKey, SubsystemReplication, "sgr_num_attachment_bytes_transferred"), "sgr_num_attachment_bytes_transferred", []string{"replication"}, nil)
	numAttachmentsTransferred = prometheus.NewDesc(prometheus.BuildFQName(NamespaceKey, SubsystemReplication, "sgr_num_attachments_transferred"), "sgr_num_attachments_transferred", []string{"replication"}, nil)
	numDocsFailedToPush = prometheus.NewDesc(prometheus.BuildFQName(NamespaceKey, SubsystemReplication, "sgr_num_docs_failed_to_push"), "sgr_num_docs_failed_to_push", []string{"replication"}, nil)
	numDocsPushed = prometheus.NewDesc(prometheus.BuildFQName(NamespaceKey, SubsystemReplication, "sgr_num_docs_pushed"), "sgr_num_docs_pushed", []string{"replication"}, nil)

	sgwStats.GlobalStats.initResourceUtilizationStats()
	sgwStats.initReplicationStats()

	return &sgwStats
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
		AdminNetworkInterfaceBytesReceived:  NewIntStat(ResourceUtilizationSubsystem, "admin_net_bytes_recv", "", prometheus.CounterValue, 0),
		AdminNetworkInterfaceBytesSent:      NewIntStat(ResourceUtilizationSubsystem, "admin_net_bytes_sent", "", prometheus.CounterValue, 0),
		ErrorCount:                          NewIntStat(ResourceUtilizationSubsystem, "error_count", "", prometheus.CounterValue, 0),
		GoMemstatsHeapAlloc:                 NewIntStat(ResourceUtilizationSubsystem, "go_memstats_heapalloc", "", prometheus.GaugeValue, 0),
		GoMemstatsHeapIdle:                  NewIntStat(ResourceUtilizationSubsystem, "go_memstats_heapidle", "", prometheus.GaugeValue, 0),
		GoMemstatsHeapInUse:                 NewIntStat(ResourceUtilizationSubsystem, "go_memstats_heapinuse", "", prometheus.GaugeValue, 0),
		GoMemstatsHeapReleased:              NewIntStat(ResourceUtilizationSubsystem, "go_memstats_heapreleased", "", prometheus.GaugeValue, 0),
		GoMemstatsPauseTotalNS:              NewIntStat(ResourceUtilizationSubsystem, "go_memstats_pausetotalns", "", prometheus.GaugeValue, 0),
		GoMemstatsStackInUse:                NewIntStat(ResourceUtilizationSubsystem, "go_memstats_stackinuse", "", prometheus.GaugeValue, 0),
		GoMemstatsStackSys:                  NewIntStat(ResourceUtilizationSubsystem, "go_memstats_stacksys", "", prometheus.GaugeValue, 0),
		GoMemstatsSys:                       NewIntStat(ResourceUtilizationSubsystem, "go_memstats_sys", "", prometheus.GaugeValue, 0),
		GoroutinesHighWatermark:             NewIntStat(ResourceUtilizationSubsystem, "goroutines_high_watermark", "", prometheus.GaugeValue, 0),
		NumGoroutines:                       NewIntStat(ResourceUtilizationSubsystem, "num_goroutines", "", prometheus.GaugeValue, 0),
		ProcessMemoryResident:               NewIntStat(ResourceUtilizationSubsystem, "process_memory_resident", "", prometheus.GaugeValue, 0),
		PublicNetworkInterfaceBytesReceived: NewIntStat(ResourceUtilizationSubsystem, "pub_net_bytes_recv", "", prometheus.CounterValue, 0),
		PublicNetworkInterfaceBytesSent:     NewIntStat(ResourceUtilizationSubsystem, "pub_net_bytes_sent", "", prometheus.CounterValue, 0),
		SystemMemoryTotal:                   NewIntStat(ResourceUtilizationSubsystem, "system_memory_total", "", prometheus.GaugeValue, 0),
		WarnCount:                           NewIntStat(ResourceUtilizationSubsystem, "warn_count", "", prometheus.CounterValue, 0),
		CpuPercentUtil:                      NewFloatStat(ResourceUtilizationSubsystem, "process_cpu_percent_utilization", "", prometheus.GaugeValue, 0),
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
	NumActiveChannels                   *SgwIntStat `json:"num_active_channels"`
	NumSkippedSeqs                      *SgwIntStat `json:"num_skipped_seqs"`
	PendingSeqLen                       *SgwIntStat `json:"pending_seq_len"`
	RevisionCacheBypass                 *SgwIntStat `json:"rev_cache_bypass"`
	RevisionCacheHits                   *SgwIntStat `json:"rev_cache_hits"`
	RevisionCacheMisses                 *SgwIntStat `json:"rev_cache_misses"`
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
	AbandonedSeqs           *SgwIntStat `json:"abandoned_seqs"`
	ConflictWriteCount      *SgwIntStat `json:"conflict_write_count"`
	Crc32MatchCount         *SgwIntStat `json:"crc32c_match_count"`
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

func (rs *ReplicatorStats) MarshalJSON() ([]byte, error) {
	return []byte(rs.String()), nil
}

func (rs *ReplicatorStats) Describe(ch chan<- *prometheus.Desc) {
	return
}

func (rs *ReplicatorStats) Collect(ch chan<- prometheus.Metric) {
	rs.Do(func(value expvar.KeyValue) {
		ch <- prometheus.MustNewConstMetric(
			checkedSentDesc,
			prometheus.GaugeValue,
			float64(value.Value.(*expvar.Map).Get("sgr_docs_checked_sent").(*expvar.Int).Value()),
			value.Key,
		)
		ch <- prometheus.MustNewConstMetric(
			numAttachmentBytesTransferred,
			prometheus.GaugeValue,
			float64(value.Value.(*expvar.Map).Get("sgr_num_attachment_bytes_transferred").(*expvar.Int).Value()),
			value.Key,
		)
		ch <- prometheus.MustNewConstMetric(
			numAttachmentsTransferred,
			prometheus.GaugeValue,
			float64(value.Value.(*expvar.Map).Get("sgr_num_attachments_transferred").(*expvar.Int).Value()),
			value.Key,
		)
		ch <- prometheus.MustNewConstMetric(
			numDocsFailedToPush,
			prometheus.GaugeValue,
			float64(value.Value.(*expvar.Map).Get("sgr_num_docs_failed_to_push").(*expvar.Int).Value()),
			value.Key,
		)
		ch <- prometheus.MustNewConstMetric(
			numDocsPushed,
			prometheus.GaugeValue,
			float64(value.Value.(*expvar.Map).Get("sgr_num_docs_pushed").(*expvar.Int).Value()),
			value.Key,
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

	NumAttachmentBytesPulled *SgwIntStat `json:"sgr_num_attachment_bytes_pulled"`
	NumAttachmentsPulled     *SgwIntStat `json:"sgr_num_attachments_pulled"`
	PulledCount              *SgwIntStat `json:"sgr_num_docs_pulled"`
	PurgedCount              *SgwIntStat `json:"sgr_num_docs_purged"`
	FailedToPullCount        *SgwIntStat `json:"sgr_num_docs_failed_to_pull"`
	DeltaReceivedCount       *SgwIntStat `json:"sgr_deltas_recv"`
	DeltaRequestedCount      *SgwIntStat `json:"sgr_deltas_requested"`
	DocsCheckedReceived      *SgwIntStat `json:"sgr_docs_checked_recv"`

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
	labels        map[string]string
	statValueType prometheus.ValueType
}

type SgwIntStat struct {
	SgwStat
	Val int64
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

func newSGWStat(subsystem string, key string, labels map[string]string, statValueType prometheus.ValueType) *SgwStat {
	name := prometheus.BuildFQName(NamespaceKey, subsystem, key)
	desc := prometheus.NewDesc(name, key, getKeys(labels), nil)

	return &SgwStat{
		statFQN:       name,
		statDesc:      desc,
		labels:        labels,
		statValueType: statValueType,
	}
}

func NewIntStat(subsystem string, key string, dbName string, statValueType prometheus.ValueType, initialValue int64) *SgwIntStat {
	var labels map[string]string
	if dbName != "" {
		// It would be possible to supply labels directly into the constructor but as we will only take dbName for now
		// we can do this to make the constructor more convenient to use.
		labels = map[string]string{"database": dbName}
	}

	stat := &SgwIntStat{
		SgwStat: *newSGWStat(subsystem, key, labels, statValueType),
		Val:     initialValue,
	}
	prometheus.MustRegister(stat)
	return stat
}

func (s *SgwIntStat) Describe(ch chan<- *prometheus.Desc) {
	return
}

func (s *SgwIntStat) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(s.statDesc, s.statValueType, float64(atomic.LoadInt64(&s.Val)), getValues(s.labels)...)
}

func (s *SgwIntStat) Set(newV int64) {
	atomic.StoreInt64(&s.Val, newV)
}

func (s *SgwIntStat) SetIfMax(newV int64) {
	for {
		cur := atomic.LoadInt64(&s.Val)
		nxtVal := cur + newV
		if atomic.CompareAndSwapInt64(&s.Val, cur, nxtVal) {
			return
		}
	}
}

func (s *SgwIntStat) Add(newV int64) {
	atomic.AddInt64(&s.Val, newV)
}

func (s *SgwIntStat) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%v", atomic.LoadInt64(&s.Val))), nil
}

func (s *SgwIntStat) String() string {
	return fmt.Sprintf("%v", s.Val)
}

func (s *SgwIntStat) Value() int64 {
	return atomic.LoadInt64(&s.Val)
}

func NewFloatStat(subsystem string, key string, dbName string, statValueType prometheus.ValueType, initialValue float64) *SgwFloatStat {
	var labels map[string]string
	if dbName != "" {
		// It would be possible to supply labels directly into the constructor but as we will only take dbName for now
		// we can do this to make the constructor more convenient to use.
		labels = map[string]string{"database": dbName}
	}

	stat := &SgwFloatStat{
		SgwStat: *newSGWStat(subsystem, key, labels, statValueType),
		Val:     math.Float64bits(initialValue),
	}
	prometheus.MustRegister(stat)
	return stat
}

func (s *SgwFloatStat) Describe(ch chan<- *prometheus.Desc) {
	return
}

func (s *SgwFloatStat) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(s.statDesc, s.statValueType, math.Float64frombits(atomic.LoadUint64(&s.Val)), getValues(s.labels)...)
}

func (s *SgwFloatStat) Set(newV float64) {
	atomic.StoreUint64(&s.Val, math.Float64bits(newV))
}

func (s *SgwFloatStat) SetIfMax(newV float64) {
	for {
		cur := atomic.LoadUint64(&s.Val)
		curVal := math.Float64frombits(cur)
		nxtVal := curVal + newV
		nxt := math.Float64bits(nxtVal)
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
	return []byte(fmt.Sprintf("%v", math.Float64frombits(atomic.LoadUint64(&s.Val)))), nil
}

func (s *SgwFloatStat) String() string {
	return fmt.Sprintf("%v", s.Val)
}

func (s *SgwFloatStat) Value() float64 {
	return math.Float64frombits(atomic.LoadUint64(&s.Val))
}

type QueryStat struct {
	QueryCount      *SgwIntStat
	QueryErrorCount *SgwIntStat
	QueryTime       *SgwIntStat
}

func (s *SgwStats) NewDBStats(name string) *DbStats {
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

	return s.DbStats[name]
}

func (s *SgwStats) ClearDBStats(name string) {
	s.dbStatsMapMutex.Lock()
	defer s.dbStatsMapMutex.Unlock()
	delete(s.DbStats, name)
}

func (d *DbStats) initCacheStats() {
	dbName := d.dbName
	d.CacheStats = &CacheStats{
		AbandonedSeqs:                       NewIntStat(SubsystemCacheKey, "abandoned_seqs", dbName, prometheus.CounterValue, 0),
		ChannelCacheRevsActive:              NewIntStat(SubsystemCacheKey, "chan_cache_active_revs", dbName, prometheus.GaugeValue, 0),
		ChannelCacheBypassCount:             NewIntStat(SubsystemCacheKey, "chan_cache_bypass_count", dbName, prometheus.CounterValue, 0),
		ChannelCacheChannelsAdded:           NewIntStat(SubsystemCacheKey, "chan_cache_channels_added", dbName, prometheus.CounterValue, 0),
		ChannelCacheChannelsEvictedInactive: NewIntStat(SubsystemCacheKey, "chan_cache_channels_evicted_inactive", dbName, prometheus.CounterValue, 0),
		ChannelCacheChannelsEvictedNRU:      NewIntStat(SubsystemCacheKey, "chan_cache_channels_evicted_nru", dbName, prometheus.CounterValue, 0),
		ChannelCacheCompactCount:            NewIntStat(SubsystemCacheKey, "chan_cache_compact_count", dbName, prometheus.CounterValue, 0),
		ChannelCacheCompactTime:             NewIntStat(SubsystemCacheKey, "chan_cache_compact_time", dbName, prometheus.CounterValue, 0),
		ChannelCacheHits:                    NewIntStat(SubsystemCacheKey, "chan_cache_hits", dbName, prometheus.CounterValue, 0),
		ChannelCacheMaxEntries:              NewIntStat(SubsystemCacheKey, "chan_cache_max_entries", dbName, prometheus.GaugeValue, 0),
		ChannelCacheMisses:                  NewIntStat(SubsystemCacheKey, "chan_cache_misses", dbName, prometheus.CounterValue, 0),
		ChannelCacheNumChannels:             NewIntStat(SubsystemCacheKey, "chan_cache_num_channels", dbName, prometheus.GaugeValue, 0),
		ChannelCachePendingQueries:          NewIntStat(SubsystemCacheKey, "chan_cache_pending_queries", dbName, prometheus.GaugeValue, 0),
		ChannelCacheRevsRemoval:             NewIntStat(SubsystemCacheKey, "chan_cache_removal_revs", dbName, prometheus.GaugeValue, 0),
		ChannelCacheRevsTombstone:           NewIntStat(SubsystemCacheKey, "chan_cache_tombstone_revs", dbName, prometheus.GaugeValue, 0),
		HighSeqCached:                       NewIntStat(SubsystemCacheKey, "high_seq_cached", dbName, prometheus.CounterValue, 0),
		HighSeqStable:                       NewIntStat(SubsystemCacheKey, "high_seq_stable", dbName, prometheus.CounterValue, 0),
		NumActiveChannels:                   NewIntStat(SubsystemCacheKey, "num_active_channels", dbName, prometheus.GaugeValue, 0),
		NumSkippedSeqs:                      NewIntStat(SubsystemCacheKey, "num_skipped_seqs", dbName, prometheus.CounterValue, 0),
		PendingSeqLen:                       NewIntStat(SubsystemCacheKey, "pending_seq_len", dbName, prometheus.GaugeValue, 0),
		RevisionCacheBypass:                 NewIntStat(SubsystemCacheKey, "rev_cache_bypass", dbName, prometheus.GaugeValue, 0),
		RevisionCacheHits:                   NewIntStat(SubsystemCacheKey, "rev_cache_hits", dbName, prometheus.CounterValue, 0),
		RevisionCacheMisses:                 NewIntStat(SubsystemCacheKey, "rev_cache_misses", dbName, prometheus.CounterValue, 0),
		SkippedSeqLen:                       NewIntStat(SubsystemCacheKey, "skipped_seq_len", dbName, prometheus.GaugeValue, 0),
	}
}

func (d *DbStats) Cache() *CacheStats {
	return d.CacheStats
}

func (d *DbStats) initCBLReplicationPullStats() {
	dbName := d.dbName
	d.CBLReplicationPullStats = &CBLReplicationPullStats{
		AttachmentPullBytes:         NewIntStat(SubsystemReplicationPull, "attachment_pull_bytes", dbName, prometheus.CounterValue, 0),
		AttachmentPullCount:         NewIntStat(SubsystemReplicationPull, "attachment_pull_count", dbName, prometheus.CounterValue, 0),
		MaxPending:                  NewIntStat(SubsystemReplicationPull, "max_pending", dbName, prometheus.GaugeValue, 0),
		NumReplicationsActive:       NewIntStat(SubsystemReplicationPull, "num_pull_repl_active_continuous", dbName, prometheus.GaugeValue, 0),
		NumPullReplActiveContinuous: NewIntStat(SubsystemReplicationPull, "num_pull_repl_active_one_shot", dbName, prometheus.GaugeValue, 0),
		NumPullReplActiveOneShot:    NewIntStat(SubsystemReplicationPull, "num_replications_active", dbName, prometheus.GaugeValue, 0),
		NumPullReplCaughtUp:         NewIntStat(SubsystemReplicationPull, "num_pull_repl_caught_up", dbName, prometheus.GaugeValue, 0),
		NumPullReplSinceZero:        NewIntStat(SubsystemReplicationPull, "num_pull_repl_since_zero", dbName, prometheus.CounterValue, 0),
		NumPullReplTotalContinuous:  NewIntStat(SubsystemReplicationPull, "num_pull_repl_total_continuous", dbName, prometheus.GaugeValue, 0),
		NumPullReplTotalOneShot:     NewIntStat(SubsystemReplicationPull, "num_pull_repl_total_one_shot", dbName, prometheus.GaugeValue, 0),
		RequestChangesCount:         NewIntStat(SubsystemReplicationPull, "request_changes_count", dbName, prometheus.CounterValue, 0),
		RequestChangesTime:          NewIntStat(SubsystemReplicationPull, "request_changes_time", dbName, prometheus.CounterValue, 0),
		RevProcessingTime:           NewIntStat(SubsystemReplicationPull, "rev_processing_time", dbName, prometheus.GaugeValue, 0),
		RevSendCount:                NewIntStat(SubsystemReplicationPull, "rev_send_count", dbName, prometheus.CounterValue, 0),
		RevSendLatency:              NewIntStat(SubsystemReplicationPull, "rev_send_latency", dbName, prometheus.CounterValue, 0),
	}
}

func (d *DbStats) CBLReplicationPull() *CBLReplicationPullStats {
	return d.CBLReplicationPullStats
}

func (d *DbStats) initCBLReplicationPushStats() {
	dbName := d.dbName
	d.CBLReplicationPushStats = &CBLReplicationPushStats{
		AttachmentPushBytes: NewIntStat(SubsystemReplicationPush, "attachment_push_bytes", dbName, prometheus.CounterValue, 0),
		AttachmentPushCount: NewIntStat(SubsystemReplicationPush, "attachment_push_count", dbName, prometheus.CounterValue, 0),
		DocPushCount:        NewIntStat(SubsystemReplicationPush, "doc_push_count", dbName, prometheus.GaugeValue, 0),
		ProposeChangeCount:  NewIntStat(SubsystemReplicationPush, "propose_change_count", dbName, prometheus.CounterValue, 0),
		ProposeChangeTime:   NewIntStat(SubsystemReplicationPush, "propose_change_time", dbName, prometheus.CounterValue, 0),
		SyncFunctionCount:   NewIntStat(SubsystemReplicationPush, "sync_function_count", dbName, prometheus.CounterValue, 0),
		SyncFunctionTime:    NewIntStat(SubsystemReplicationPush, "sync_function_time", dbName, prometheus.CounterValue, 0),
		WriteProcessingTime: NewIntStat(SubsystemReplicationPush, "write_processing_time", dbName, prometheus.GaugeValue, 0),
	}
}

func (d *DbStats) CBLReplicationPush() *CBLReplicationPushStats {
	return d.CBLReplicationPushStats
}

func (d *DbStats) initDatabaseStats() {
	dbName := d.dbName
	d.DatabaseStats = &DatabaseStats{
		AbandonedSeqs:           NewIntStat(SubsystemDatabaseKey, "abandoned_seqs", dbName, prometheus.CounterValue, 0),
		ConflictWriteCount:      NewIntStat(SubsystemDatabaseKey, "conflict_write_count", dbName, prometheus.CounterValue, 0),
		Crc32MatchCount:         NewIntStat(SubsystemDatabaseKey, "crc32c_match_count", dbName, prometheus.GaugeValue, 0),
		DCPCachingCount:         NewIntStat(SubsystemDatabaseKey, "dcp_caching_count", dbName, prometheus.GaugeValue, 0),
		DCPCachingTime:          NewIntStat(SubsystemDatabaseKey, "dcp_caching_time", dbName, prometheus.GaugeValue, 0),
		DCPReceivedCount:        NewIntStat(SubsystemDatabaseKey, "dcp_received_count", dbName, prometheus.GaugeValue, 0),
		DCPReceivedTime:         NewIntStat(SubsystemDatabaseKey, "dcp_received_time", dbName, prometheus.GaugeValue, 0),
		DocReadsBytesBlip:       NewIntStat(SubsystemDatabaseKey, "doc_reads_bytes_blip", dbName, prometheus.CounterValue, 0),
		DocWritesBytes:          NewIntStat(SubsystemDatabaseKey, "doc_writes_bytes", dbName, prometheus.CounterValue, 0),
		DocWritesXattrBytes:     NewIntStat(SubsystemDatabaseKey, "doc_writes_xattr_bytes", dbName, prometheus.CounterValue, 0),
		HighSeqFeed:             NewIntStat(SubsystemDatabaseKey, "high_seq_feed", dbName, prometheus.CounterValue, 0),
		DocWritesBytesBlip:      NewIntStat(SubsystemDatabaseKey, "doc_writes_bytes_blip", dbName, prometheus.CounterValue, 0),
		NumDocReadsBlip:         NewIntStat(SubsystemDatabaseKey, "num_doc_reads_blip", dbName, prometheus.CounterValue, 0),
		NumDocReadsRest:         NewIntStat(SubsystemDatabaseKey, "num_doc_reads_rest", dbName, prometheus.CounterValue, 0),
		NumDocWrites:            NewIntStat(SubsystemDatabaseKey, "num_doc_writes", dbName, prometheus.CounterValue, 0),
		NumReplicationsActive:   NewIntStat(SubsystemDatabaseKey, "num_replications_active", dbName, prometheus.GaugeValue, 0),
		NumReplicationsTotal:    NewIntStat(SubsystemDatabaseKey, "num_replications_total", dbName, prometheus.CounterValue, 0),
		NumTombstonesCompacted:  NewIntStat(SubsystemDatabaseKey, "num_tombstones_compacted", dbName, prometheus.CounterValue, 0),
		SequenceAssignedCount:   NewIntStat(SubsystemDatabaseKey, "sequence_assigned_count", dbName, prometheus.CounterValue, 0),
		SequenceGetCount:        NewIntStat(SubsystemDatabaseKey, "sequence_get_count", dbName, prometheus.CounterValue, 0),
		SequenceIncrCount:       NewIntStat(SubsystemDatabaseKey, "sequence_incr_count", dbName, prometheus.CounterValue, 0),
		SequenceReleasedCount:   NewIntStat(SubsystemDatabaseKey, "sequence_released_count", dbName, prometheus.CounterValue, 0),
		SequenceReservedCount:   NewIntStat(SubsystemDatabaseKey, "sequence_reserved_count", dbName, prometheus.CounterValue, 0),
		WarnChannelsPerDocCount: NewIntStat(SubsystemDatabaseKey, "warn_channels_per_doc_count", dbName, prometheus.CounterValue, 0),
		WarnGrantsPerDocCount:   NewIntStat(SubsystemDatabaseKey, "warn_grants_per_doc_count", dbName, prometheus.CounterValue, 0),
		WarnXattrSizeCount:      NewIntStat(SubsystemDatabaseKey, "warn_xattr_size_count", dbName, prometheus.CounterValue, 0),
		ImportFeedMapStats:      &ExpVarMapWrapper{new(expvar.Map).Init()},
		CacheFeedMapStats:       &ExpVarMapWrapper{new(expvar.Map).Init()},
	}
}

func (d *DbStats) Database() *DatabaseStats {
	return d.DatabaseStats
}

func (d *DbStats) InitDeltaSyncStats() {
	dbName := d.dbName
	d.DeltaSyncStats = &DeltaSyncStats{
		DeltasRequested:           NewIntStat(SubsystemDeltaSyncKey, "deltas_requested", dbName, prometheus.CounterValue, 0),
		DeltasSent:                NewIntStat(SubsystemDeltaSyncKey, "deltas_sent", dbName, prometheus.CounterValue, 0),
		DeltaPullReplicationCount: NewIntStat(SubsystemDeltaSyncKey, "delta_pull_replication_count", dbName, prometheus.CounterValue, 0),
		DeltaCacheHit:             NewIntStat(SubsystemDeltaSyncKey, "delta_cache_hit", dbName, prometheus.CounterValue, 0),
		DeltaCacheMiss:            NewIntStat(SubsystemDeltaSyncKey, "delta_sync_miss", dbName, prometheus.CounterValue, 0),
		DeltaPushDocCount:         NewIntStat(SubsystemDeltaSyncKey, "delta_push_doc_count", dbName, prometheus.CounterValue, 0),
	}
}

func (d *DbStats) DeltaSync() *DeltaSyncStats {
	return d.DeltaSyncStats
}

func (d *DbStats) initSecurityStats() {
	if d.SecurityStats == nil {
		dbName := d.dbName
		d.SecurityStats = &SecurityStats{
			AuthFailedCount:  NewIntStat(SubsystemSecurity, "auth_failed_count", dbName, prometheus.CounterValue, 0),
			AuthSuccessCount: NewIntStat(SubsystemSecurity, "auth_success_count", dbName, prometheus.CounterValue, 0),
			NumAccessErrors:  NewIntStat(SubsystemSecurity, "num_access_errors", dbName, prometheus.CounterValue, 0),
			NumDocsRejected:  NewIntStat(SubsystemSecurity, "num_docs_rejected", dbName, prometheus.CounterValue, 0),
			TotalAuthTime:    NewIntStat(SubsystemSecurity, "total_auth_time", dbName, prometheus.GaugeValue, 0),
		}
	}
}

func (d *DbStats) DBReplicatorStats(replicationID string) *DbReplicatorStats {
	if d.DbReplicatorStats == nil {
		d.DbReplicatorStats = map[string]*DbReplicatorStats{}
	}

	if _, ok := d.DbReplicatorStats[replicationID]; !ok {
		d.DbReplicatorStats[replicationID] = &DbReplicatorStats{
			NumAttachmentBytesPushed:    NewIntStat(SubsystemReplication, "sgr_num_attachment_bytes_pushed", d.dbName, prometheus.CounterValue, 0),
			NumAttachmentPushed:         NewIntStat(SubsystemReplication, "sgr_num_attachments_pushed", d.dbName, prometheus.CounterValue, 0),
			NumDocPushed:                NewIntStat(SubsystemReplication, "sgr_num_docs_pushed", d.dbName, prometheus.CounterValue, 0),
			NumDocsFailedToPush:         NewIntStat(SubsystemReplication, "sgr_num_docs_failed_to_push", d.dbName, prometheus.CounterValue, 0),
			PushConflictCount:           NewIntStat(SubsystemReplication, "sgr_push_conflict_count", d.dbName, prometheus.CounterValue, 0),
			PushRejectedCount:           NewIntStat(SubsystemReplication, "sgr_push_rejected_count", d.dbName, prometheus.CounterValue, 0),
			PushDeltaSentCount:          NewIntStat(SubsystemReplication, "sgr_deltas_sent", d.dbName, prometheus.CounterValue, 0),
			DocsCheckedSent:             NewIntStat(SubsystemReplication, "sgr_docs_checked_sent", d.dbName, prometheus.CounterValue, 0),
			NumAttachmentBytesPulled:    NewIntStat(SubsystemReplication, "sgr_num_attachment_bytes_pulled", d.dbName, prometheus.CounterValue, 0),
			NumAttachmentsPulled:        NewIntStat(SubsystemReplication, "sgr_num_attachments_pulled", d.dbName, prometheus.CounterValue, 0),
			PulledCount:                 NewIntStat(SubsystemReplication, "sgr_num_docs_pulled", d.dbName, prometheus.CounterValue, 0),
			PurgedCount:                 NewIntStat(SubsystemReplication, "sgr_num_docs_purged", d.dbName, prometheus.CounterValue, 0),
			FailedToPullCount:           NewIntStat(SubsystemReplication, "sgr_num_docs_failed_to_pull", d.dbName, prometheus.CounterValue, 0),
			DeltaReceivedCount:          NewIntStat(SubsystemReplication, "sgr_deltas_recv", d.dbName, prometheus.CounterValue, 0),
			DeltaRequestedCount:         NewIntStat(SubsystemReplication, "sgr_deltas_requested", d.dbName, prometheus.CounterValue, 0),
			DocsCheckedReceived:         NewIntStat(SubsystemReplication, "sgr_docs_checked_recv", d.dbName, prometheus.CounterValue, 0),
			ConflictResolvedLocalCount:  NewIntStat(SubsystemReplication, "sgr_conflict_resolved_local_count", d.dbName, prometheus.CounterValue, 0),
			ConflictResolvedRemoteCount: NewIntStat(SubsystemReplication, "sgr_conflict_resolved_remote_count", d.dbName, prometheus.CounterValue, 0),
			ConflictResolvedMergedCount: NewIntStat(SubsystemReplication, "sgr_conflict_resolved_merge_count", d.dbName, prometheus.CounterValue, 0),
		}
	}

	return d.DbReplicatorStats[replicationID]
}

func (d *DbStats) Security() *SecurityStats {
	return d.SecurityStats
}

func (d *DbStats) InitSharedBucketImportStats() {
	if d.SharedBucketImportStats == nil {
		dbName := d.dbName
		d.SharedBucketImportStats = &SharedBucketImportStats{
			ImportCount:          NewIntStat(SubsystemSharedBucketImport, "import_count", dbName, prometheus.CounterValue, 0),
			ImportCancelCAS:      NewIntStat(SubsystemSharedBucketImport, "import_cancel_cas", dbName, prometheus.CounterValue, 0),
			ImportErrorCount:     NewIntStat(SubsystemSharedBucketImport, "import_error_count", dbName, prometheus.CounterValue, 0),
			ImportProcessingTime: NewIntStat(SubsystemSharedBucketImport, "import_processing_time", dbName, prometheus.GaugeValue, 0),
			ImportHighSeq:        NewIntStat(SubsystemSharedBucketImport, "import_high_seq", dbName, prometheus.CounterValue, 0),
			ImportPartitions:     NewIntStat(SubsystemSharedBucketImport, "import_partitions", dbName, prometheus.GaugeValue, 0),
		}
	}
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
		dbName := d.dbName

		// Prometheus isn't happy with '.'s in the name and the '.'s come from the design doc version. Design doc isn't
		// reported to prometheus. Only the view name.
		prometheusKey := queryName
		if useViews {
			splitName := strings.Split(queryName, ".")
			prometheusKey = splitName[len(splitName)-1]
		}

		d.QueryStats.Stats[queryName] = &QueryStat{
			QueryCount:      NewIntStat(SubsystemGSIViews, prometheusKey+"_count", dbName, prometheus.CounterValue, 0),
			QueryErrorCount: NewIntStat(SubsystemGSIViews, prometheusKey+"_error_count", dbName, prometheus.CounterValue, 0),
			QueryTime:       NewIntStat(SubsystemGSIViews, prometheusKey+"_time", dbName, prometheus.CounterValue, 0),
		}
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

func getKeys(myMap map[string]string) []string {
	keys := make([]string, 0, len(myMap))
	for k := range myMap {
		keys = append(keys, k)
	}
	return keys
}

func getValues(myMap map[string]string) []string {
	vals := make([]string, 0, len(myMap))
	for _, v := range myMap {
		vals = append(vals, v)
	}
	return vals
}
