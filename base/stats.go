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
	"context"
	"expvar"
	"fmt"
	"log"
	"math"
	"slices"
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
	ConfigSubsystem              = "config"
	AuditSubsystem               = "audit"

	SubsystemCacheKey           = "cache"
	SubsystemDatabaseKey        = "database"
	SubsystemDeltaSyncKey       = "delta_sync"
	SubsystemGSIViews           = "gsi_views"
	SubsystemReplication        = "replication"
	SubsystemCollection         = "collection"
	SubsystemReplicationPull    = "replication_pull"
	SubsystemReplicationPush    = "replication_push"
	SubsystemSecurity           = "security"
	SubsystemSharedBucketImport = "shared_bucket_import"

	DatabaseLabelKey    = "database"
	ReplicationLabelKey = "replication"
	CollectionLabelKey  = "collection"
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

const (
	PrometheusValueTypeGauge   = "gauge"
	PrometheusValueTypeCounter = "counter"

	StatUnitNoUnits       = ""
	StatUnitPercent       = "percent"
	StatUnitBytes         = "bytes"
	StatUnitNanoseconds   = "nanoseconds"
	StatUnitSeconds       = "seconds"
	StatUnitUnixTimestamp = "unix timestamp"

	StatFormatInt      = "int"
	StatFormatFloat    = "float"
	StatFormatDuration = "duration"
	StatFormatBool     = "bool"

	StatAddedVersion3dot0dot0     = "3.0.0"
	StatAddedVersion3dot1dot0     = "3.1.0"
	StatAddedVersion3dot1dot2     = "3.1.2"
	StatAddedVersion3dot1dot3dot1 = "3.1.3.1"
	StatAddedVersion3dot1dot4     = "3.1.4"
	StatAddedVersion3dot2dot0     = "3.2.0"
	StatAddedVersion3dot2dot1     = "3.2.1"
	StatAddedVersion3dot2dot2     = "3.2.2"
	StatAddedVersion3dot2dot3     = "3.2.3"
	StatAddedVersion3dot2dot4     = "3.2.4"
	StatAddedVersion3dot3dot0     = "3.3.0"

	StatDeprecatedVersionNotDeprecated = ""
	StatDeprecatedVersion3dot2dot0     = "3.2.0"
	StatDeprecatedVersion3dot2dot2     = "3.2.2"
	StatDeprecatedVersion3dot3dot0     = "3.3.0"

	StatStabilityCommitted = "committed"
	StatStabilityVolatile  = "volatile"
	StatStabilityInternal  = "internal"
)

type SgwStats struct {
	GlobalStats     *GlobalStat         `json:"global"`
	DbStats         map[string]*DbStats `json:"per_db"`
	ReplicatorStats *ReplicatorStats    `json:"per_replication,omitempty"`

	dbStatsMapMutex sync.Mutex
}

var SyncGatewayStats *SgwStats

// SkipPrometheusStatsRegistration used to avoid registering a stat with prometheus
// Defaults to false - Only intended for test use
// Note that due to NewSyncGatewayStats() being ran as part of init the initial stats will still be registered and so
// global stats are unaffected
var SkipPrometheusStatsRegistration bool

func NewSyncGatewayStats() (*SgwStats, error) {
	globalStats, err := newGlobalStat()
	if err != nil {
		return nil, err
	}
	sgwStats := SgwStats{
		GlobalStats: globalStats,
		DbStats:     map[string]*DbStats{},
	}

	err = sgwStats.initReplicationStats()
	if err != nil {
		return nil, err
	}

	// This provides a stat for sgw_up where the value will be fixed to one. This is to allow backwards compatibility
	// where the standalone exporter would export a value of 1 if it has contact with SGW.
	_, err = NewIntStat("", "up", StatUnitNoUnits, SGWUpDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.GaugeValue, 1)
	if err != nil {
		return nil, err
	}

	return &sgwStats, nil
}

func init() {
	// Initialize Sync Gateway Stats

	// All stats will be stored as part of this struct. Global variable accessible everywhere. To add stats see stats.go
	var err error
	SyncGatewayStats, err = NewSyncGatewayStats()
	if err != nil {
		log.Fatalf("Fatal error attempting to instantiate sync gateway stats: %v", err)
	}

	// Publish our stats to expvars. This will run String method on SyncGatewayStats ( type SgwStats ) which will
	// marshal the stats to JSON
	expvar.Publish(StatsGroupKeySyncGateway, SyncGatewayStats)
}

// This String() is to satisfy the expvar.Var interface which is used to produce the expvar endpoint output.
func (s *SgwStats) String() string {
	s.dbStatsMapMutex.Lock()
	bytes, err := JSONMarshalCanonical(s)
	s.dbStatsMapMutex.Unlock()
	if err != nil {
		ErrorfCtx(context.Background(), "Unable to Marshal SgwStats: %v", err)
		return "null"
	}
	return string(bytes)
}

type GlobalStat struct {
	ResourceUtilization *ResourceUtilization `json:"resource_utilization"`
	ConfigStat          *ConfigStat          `json:"config"`
	AuditStat           *AuditStat           `json:"audit"`
}

func newGlobalStat() (*GlobalStat, error) {
	g := &GlobalStat{}
	err := g.initResourceUtilizationStats()
	if err != nil {
		return nil, err
	}
	err = g.initConfigStats()
	if err != nil {
		return nil, err
	}
	err = g.initAuditStats()
	if err != nil {
		return nil, err
	}
	return g, nil
}

func (g *GlobalStat) initConfigStats() error {
	configStat := &ConfigStat{}
	var err error
	configStat.DatabaseBucketMismatches, err = NewIntStat(ConfigSubsystem, "database_config_bucket_mismatches", StatUnitBytes, DatabaseBucketMismatchesDesc, StatAddedVersion3dot1dot2, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	configStat.DatabaseRollbackCollectionCollisions, err = NewIntStat(ConfigSubsystem, "database_config_collection_conflicts", StatUnitBytes, DatabaseCollectionConflictDesc, StatAddedVersion3dot1dot4, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	configStat.XattrFormatMismatches, err = NewIntStat(ConfigSubsystem, "xattr_format_mismatches", StatUnitBytes, XattrFormatMismatchesDesc, StatAddedVersion3dot2dot4, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	g.ConfigStat = configStat
	return nil
}

func (g *GlobalStat) initAuditStats() error {
	auditStat := &AuditStat{}
	var err error
	auditStat.NumAuditsLogged, err = NewIntStat(AuditSubsystem, "num_audits_logged", StatUnitNoUnits, NumAuditsLoggedDesc, StatAddedVersion3dot2dot1, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	auditStat.NumAuditsFilteredByUser, err = NewIntStat(AuditSubsystem, "num_audits_filtered_by_user", StatUnitNoUnits, NumAuditsFilteredByUserDesc, StatAddedVersion3dot2dot1, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	auditStat.NumAuditsFilteredByRole, err = NewIntStat(AuditSubsystem, "num_audits_filtered_by_role", StatUnitNoUnits, NumAuditsFilteredByRoleDesc, StatAddedVersion3dot2dot1, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	g.AuditStat = auditStat
	return nil
}

func (g *GlobalStat) initResourceUtilizationStats() error {
	var err error
	resUtil := &ResourceUtilization{}

	resUtil.AdminNetworkInterfaceBytesReceived, err = NewIntStat(ResourceUtilizationSubsystem, "admin_net_bytes_recv", StatUnitBytes, AdminNetBytesRecDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.AdminNetworkInterfaceBytesSent, err = NewIntStat(ResourceUtilizationSubsystem, "admin_net_bytes_sent", StatUnitBytes, AdminNetBytesSentDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ErrorCount, err = NewIntStat(ResourceUtilizationSubsystem, "error_count", StatUnitNoUnits, ErrorCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.GoMemstatsHeapAlloc, err = NewIntStat(ResourceUtilizationSubsystem, "go_memstats_heapalloc", StatUnitBytes, GoMemHeapAllocDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.GoMemstatsHeapIdle, err = NewIntStat(ResourceUtilizationSubsystem, "go_memstats_heapidle", StatUnitBytes, GoMemHeapIdleDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.GoMemstatsHeapInUse, err = NewIntStat(ResourceUtilizationSubsystem, "go_memstats_heapinuse", StatUnitBytes, GoMemHeapInUseDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.GoMemstatsHeapReleased, err = NewIntStat(ResourceUtilizationSubsystem, "go_memstats_heapreleased", StatUnitBytes, GoMemHeapReleasedDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.GoMemstatsPauseTotalNS, err = NewIntStat(ResourceUtilizationSubsystem, "go_memstats_pausetotalns", StatUnitNanoseconds, GoMemPauseTotalNSDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.GoMemstatsStackInUse, err = NewIntStat(ResourceUtilizationSubsystem, "go_memstats_stackinuse", StatUnitBytes, GoMemStackInUseDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.GoMemstatsStackSys, err = NewIntStat(ResourceUtilizationSubsystem, "go_memstats_stacksys", StatUnitBytes, GoMemStackSysDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.GoMemstatsSys, err = NewIntStat(ResourceUtilizationSubsystem, "go_memstats_sys", StatUnitBytes, GoMemSysDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.GoroutinesHighWatermark, err = NewIntStat(ResourceUtilizationSubsystem, "goroutines_high_watermark", StatUnitNoUnits, GoroutinesHighWatermarkDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumGoroutines, err = NewIntStat(ResourceUtilizationSubsystem, "num_goroutines", StatUnitNoUnits, NumGoroutinesDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.ProcessMemoryResident, err = NewIntStat(ResourceUtilizationSubsystem, "process_memory_resident", StatUnitBytes, ProcessMemoryResidentDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.PublicNetworkInterfaceBytesReceived, err = NewIntStat(ResourceUtilizationSubsystem, "pub_net_bytes_recv", StatUnitBytes, PublicNetBytesRecvDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.PublicNetworkInterfaceBytesSent, err = NewIntStat(ResourceUtilizationSubsystem, "pub_net_bytes_sent", StatUnitBytes, PublicNetBytesSentDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.SystemMemoryTotal, err = NewIntStat(ResourceUtilizationSubsystem, "system_memory_total", StatUnitBytes, SystemMemoryTotalDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.WarnCount, err = NewIntStat(ResourceUtilizationSubsystem, "warn_count", StatUnitNoUnits, WarnCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.AssertionFailCount, err = NewIntStat(ResourceUtilizationSubsystem, "assertion_fail_count", StatUnitNoUnits, AssertionFailCountDesc, StatAddedVersion3dot2dot4, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.CpuPercentUtil, err = NewFloatStat(ResourceUtilizationSubsystem, "process_cpu_percent_utilization", StatUnitPercent, ProcessCPUPercentUtilDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersion3dot2dot0, StatStabilityCommitted, nil, nil, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}

	resUtil.NodeCpuPercentUtil, err = NewFloatStat(ResourceUtilizationSubsystem, "node_cpu_percent_utilization", StatUnitPercent, NodeCPUPercentUtilDesc, StatAddedVersion3dot2dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumIdleKvOps, err = NewIntStat(SubsystemDatabaseKey, "num_idle_kv_ops", StatUnitNoUnits, NumIdleKvOpsDesc, StatAddedVersion3dot1dot3dot1, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumIdleQueryOps, err = NewIntStat(SubsystemDatabaseKey, "num_idle_query_ops", StatUnitNoUnits, NumIdleQueryOpsDesc, StatAddedVersion3dot2dot2, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}

	resUtil.Uptime, err = NewDurStat(ResourceUtilizationSubsystem, "uptime", StatUnitNanoseconds, UptimeDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, nil, nil, prometheus.CounterValue, time.Now())
	if err != nil {
		return err
	}

	g.ResourceUtilization = resUtil

	return nil
}

func (g *GlobalStat) ResourceUtilizationStats() *ResourceUtilization {
	return g.ResourceUtilization
}

func (s *SgwStats) initReplicationStats() error {
	s.ReplicatorStats = &ReplicatorStats{
		new(expvar.Map).Init(),
	}
	return prometheus.Register(s.ReplicatorStats)
}

func (s *SgwStats) ReplicationStats() *expvar.Map {
	return s.ReplicatorStats.Map
}

type ResourceUtilization struct {
	// The total number of bytes received (since node start-up) on the network interface to which the Sync Gateway api.admin_interface is bound.
	AdminNetworkInterfaceBytesReceived *SgwIntStat `json:"admin_net_bytes_recv"`
	// The total number of bytes sent (since node start-up) on the network interface to which the Sync Gateway api.admin_interface is bound.
	AdminNetworkInterfaceBytesSent *SgwIntStat `json:"admin_net_bytes_sent"`
	// The total number of errors logged.
	ErrorCount             *SgwIntStat `json:"error_count"`
	GoMemstatsHeapAlloc    *SgwIntStat `json:"go_memstats_heapalloc"`
	GoMemstatsHeapIdle     *SgwIntStat `json:"go_memstats_heapidle"`
	GoMemstatsHeapInUse    *SgwIntStat `json:"go_memstats_heapinuse"`
	GoMemstatsHeapReleased *SgwIntStat `json:"go_memstats_heapreleased"`
	GoMemstatsPauseTotalNS *SgwIntStat `json:"go_memstats_pausetotalns"`
	GoMemstatsStackInUse   *SgwIntStat `json:"go_memstats_stackinuse"`
	GoMemstatsStackSys     *SgwIntStat `json:"go_memstats_stacksys"`
	GoMemstatsSys          *SgwIntStat `json:"go_memstats_sys"`
	// Peak number of go routines since process start.
	GoroutinesHighWatermark *SgwIntStat `json:"goroutines_high_watermark"`
	// The total number of goroutines.
	NumGoroutines *SgwIntStat `json:"num_goroutines"`
	// The CPU’s utilization as percentage value.
	//
	// The CPU usage calculation is performed based on user and system CPU time, but it doesn’t include components such as iowait.
	// The derivation means that the values of process_cpu_percent_utilization and %Cpu, returned when running the top command, will differ.
	CpuPercentUtil *SgwFloatStat `json:"process_cpu_percent_utilization"`

	// The node CPU usage calculation based values from /proc of user + system since the last time this function was called.
	NodeCpuPercentUtil *SgwFloatStat `json:"node_cpu_percent_utilization"`

	// The number of background kv/query operations.
	NumIdleKvOps    *SgwIntStat `json:"idle_kv_ops"`
	NumIdleQueryOps *SgwIntStat `json:"idle_query_ops"`

	// The memory utilization (Resident Set Size) for the process, in bytes.
	ProcessMemoryResident *SgwIntStat `json:"process_memory_resident"`
	// The total number of bytes received (since node start-up) on the network interface to which the Sync Gateway api.public_interface is bound.
	PublicNetworkInterfaceBytesReceived *SgwIntStat `json:"pub_net_bytes_recv"`
	// The total number of bytes sent (since node start-up) on the network interface to which Sync Gateway api.public_interface is bound.
	PublicNetworkInterfaceBytesSent *SgwIntStat `json:"pub_net_bytes_sent"`
	// The total memory available on the system in bytes.
	SystemMemoryTotal *SgwIntStat `json:"system_memory_total"`
	// The total number of warnings logged.
	WarnCount *SgwIntStat `json:"warn_count"`
	// The total number of assertion failures logged. This is a good indicator of a bug and should be reported.
	AssertionFailCount *SgwIntStat `json:"assertion_fail_count"`
	// The total uptime.
	Uptime *SgwDurStat `json:"uptime"`
}

type ConfigStat struct {
	// The number of times the bucket specified in a database config doesn't match the bucket it's found in.
	DatabaseBucketMismatches *SgwIntStat `json:"database_config_bucket_mismatches"`
	// The number of times the config was rolled back to an invalid state (conflicting collections)
	DatabaseRollbackCollectionCollisions *SgwIntStat `json:"database_config_rollback_collection_collisions"`
	// The number of times a non-xattr config or registry document was loaded in xattr mode
	XattrFormatMismatches *SgwIntStat `json:"xattr_format_mismatches"`
}

type AuditStat struct {
	// The number of times an audit event was created/emitted/logged.
	NumAuditsLogged *SgwIntStat `json:"num_audits_logged"`
	// The number of times an audit event was filtered by username.
	NumAuditsFilteredByUser *SgwIntStat `json:"num_audits_filtered_by_user"`
	// The number of times an audit event was filtered by role.
	NumAuditsFilteredByRole *SgwIntStat `json:"num_audits_filtered_by_role"`
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
	CollectionStats         map[string]*CollectionStats   `json:"per_collection,omitempty"`
	dbReplicatorStatsMutex  sync.Mutex
}

type CacheStats struct {
	// The total number of skipped sequences that were not found after 60 minutes and were abandoned.
	AbandonedSeqs *SgwIntStat `json:"abandoned_seqs"`
	// The total number of active revisions in the channel cache.
	ChannelCacheRevsActive *SgwIntStat `json:"chan_cache_active_revs"`
	// The total number of transient bypass channel caches created to serve requests when the channel cache was at capacity.
	ChannelCacheBypassCount *SgwIntStat `json:"chan_cache_bypass_count"`
	// The total number of channel caches added.
	//
	// The metric doesn't decrease when a channel is removed. That is, it is similar to chan_cache_num_channels but doesn’t track removals.
	ChannelCacheChannelsAdded *SgwIntStat `json:"chan_cache_channels_added"`
	// The total number of channel cache channels evicted due to inactivity.
	ChannelCacheChannelsEvictedInactive *SgwIntStat `json:"chan_cache_channels_evicted_inactive"`
	// The total number of active channel cache channels evicted, based on ‘not recently used’ criteria.
	ChannelCacheChannelsEvictedNRU *SgwIntStat `json:"chan_cache_channels_evicted_nru"`
	// The total number of channel cache compaction runs.
	ChannelCacheCompactCount *SgwIntStat `json:"chan_cache_compact_count"`
	// The total amount of time taken by channel cache compaction across all compaction runs.
	ChannelCacheCompactTime *SgwIntStat `json:"chan_cache_compact_time"`
	// The total number of channel cache requests fully served by the cache.
	ChannelCacheHits *SgwIntStat `json:"chan_cache_hits"`
	// The total size of the largest channel cache.
	ChannelCacheMaxEntries *SgwIntStat `json:"chan_cache_max_entries"`
	// The total number of channel cache requests not fully served by the cache.
	ChannelCacheMisses *SgwIntStat `json:"chan_cache_misses"`
	// The total number of channels being cached.
	ChannelCacheNumChannels *SgwIntStat `json:"chan_cache_num_channels"`
	// The total number of channel cache pending queries.
	ChannelCachePendingQueries *SgwIntStat `json:"chan_cache_pending_queries"`
	// The total number of removal revisions in the channel cache.
	ChannelCacheRevsRemoval *SgwIntStat `json:"chan_cache_removal_revs"`
	// The total number of tombstone revisions in the channel cache.
	ChannelCacheRevsTombstone *SgwIntStat `json:"chan_cache_tombstone_revs"`
	// The highest sequence number cached.
	//
	// There may be skipped sequences lower than high_seq_cached.
	HighSeqCached *SgwIntStat `json:"high_seq_cached"`
	// The highest contiguous sequence number that has been cached.
	HighSeqStable         *SgwIntStat `json:"high_seq_stable"`
	NonMobileIgnoredCount *SgwIntStat `json:"non_mobile_ignored_count"`
	// The total number of active channels.
	NumActiveChannels *SgwIntStat `json:"num_active_channels"`
	// The total number of skipped sequences. This is a cumulative value.
	NumSkippedSeqs *SgwIntStat `json:"num_skipped_seqs"`
	// The total number of pending sequences. These are out-of-sequence entries waiting to be cached.
	PendingSeqLen *SgwIntStat `json:"pending_seq_len"`
	// Total number of items in the rev cache
	RevisionCacheNumItems *SgwIntStat `json:"revision_cache_num_items"`
	// The total number of revision cache bypass operations performed.
	RevisionCacheBypass *SgwIntStat `json:"rev_cache_bypass"`
	// The total number of revision cache hits.
	RevisionCacheHits *SgwIntStat `json:"rev_cache_hits"`
	// The total number of revision cache misses.
	RevisionCacheMisses *SgwIntStat `json:"rev_cache_misses"`
	// Total memory used by the rev cache
	RevisionCacheTotalMemory *SgwIntStat `json:"revision_cache_total_memory"`
	// Deprecated: DeprecatedSkippedSeqLen UNUSED
	DeprecatedSkippedSeqLen *SgwIntStat `json:"skipped_seq_len"`
	// Deprecated: DeprecatedSkippedSeqCap UNUSED
	DeprecatedSkippedSeqCap *SgwIntStat `json:"skipped_seq_cap"`
	// The number of nodes in skipped sequence skiplist data structure
	SkippedSequenceSkiplistNodes *SgwIntStat `json:"skipped_sequence_skip_list_nodes"`
	// The number of sequences currently in the skipped sequence slice
	NumCurrentSeqsSkipped *SgwIntStat `json:"current_skipped_seq_count"`
	// The total view_queries.
	ViewQueries *SgwIntStat `json:"view_queries"`
}

type CBLReplicationPullStats struct {
	// The total size of attachments pulled. This is the pre-compressed size.
	AttachmentPullBytes *SgwIntStat `json:"attachment_pull_bytes"`
	// The total number of attachments pulled.
	AttachmentPullCount *SgwIntStat `json:"attachment_pull_count"`
	// The high watermark for the number of documents buffered during feed processing, waiting on a missing earlier sequence.
	MaxPending *SgwIntStat `json:"max_pending"`
	// The total number of active replications. This metric only counts continuous pull replications.
	NumReplicationsActive *SgwIntStat `json:"num_replications_active"`
	// The total number of continuous pull replications in the active state.
	NumPullReplActiveContinuous *SgwIntStat `json:"num_pull_repl_active_continuous"`
	// The total number of one-shot pull replications in the active state.
	NumPullReplActiveOneShot *SgwIntStat `json:"num_pull_repl_active_one_shot"`
	// The total number of replications which have caught up to the latest changes.
	NumPullReplCaughtUp      *SgwIntStat `json:"num_pull_repl_caught_up"`
	NumPullReplTotalCaughtUp *SgwIntStat `json:"num_pull_repl_total_caught_up"`
	// The total number of new replications started (/_changes?since=0).
	NumPullReplSinceZero *SgwIntStat `json:"num_pull_repl_since_zero"`
	// The total number of continuous pull replications.
	NumPullReplTotalContinuous *SgwIntStat `json:"num_pull_repl_total_continuous"`
	// The total number of one-shot pull replications.
	NumPullReplTotalOneShot *SgwIntStat `json:"num_pull_repl_total_one_shot"`
	// The total number of changes requested.
	RequestChangesCount *SgwIntStat `json:"request_changes_count"`
	RequestChangesTime  *SgwIntStat `json:"request_changes_time"`
	// The total amount of time processing rev messages (revisions) during pull revision.
	RevProcessingTime *SgwIntStat `json:"rev_processing_time"`
	// The total number of rev messages processed during replication.
	RevSendCount *SgwIntStat `json:"rev_send_count"`
	// The total number of norev messages sent during replication.
	NoRevSendCount *SgwIntStat `json:"norev_send_count"`
	// The total number of replacement revs sent during replication.
	ReplacementRevSendCount *SgwIntStat `json:"replacement_rev_send_count"`
	// The total number of errors in response to sending a rev message.
	RevErrorCount *SgwIntStat `json:"rev_error_count"`
	// The total amount of time between Sync Gateway receiving a request for a revision and that revision being sent.
	//
	// In a pull replication, Sync Gateway sends a /_changes request to the client and the client responds with the list of revisions it wants to receive.
	//
	// So, rev_send_latency measures the time between the client asking for those revisions and Sync Gateway sending them to the client.
	RevSendLatency *SgwIntStat `json:"rev_send_latency"`
}

type CBLReplicationPushStats struct {
	// The total number of attachment bytes pushed.
	AttachmentPushBytes *SgwIntStat `json:"attachment_push_bytes"`
	// The total number of attachments pushed.
	AttachmentPushCount *SgwIntStat `json:"attachment_push_count"`
	// The total number of documents pushed.
	DocPushCount *SgwIntStat `json:"doc_push_count"`
	// The total number of documents that failed to push.
	DocPushErrorCount *SgwIntStat `json:"doc_push_error_count"`
	// The total number of changes and-or proposeChanges messages processed since node start-up.
	ProposeChangeCount *SgwIntStat `json:"propose_change_count"`
	// The total time spent processing changes and/or proposeChanges messages.
	//
	// The propose_change_time is not included in the write_processing_time.
	ProposeChangeTime *SgwIntStat `json:"propose_change_time"`
	// Total time spent processing writes. Measures complete request-to-response time for a write.
	WriteProcessingTime *SgwIntStat `json:"write_processing_time"`
	// WriteThrottledCount is the cumulative number of writes that were throttled.
	WriteThrottledCount *SgwIntStat `json:"write_throttled_count"`
	// WriteThrottledTime is the cumulative time spent throttling writes.
	WriteThrottledTime *SgwIntStat `json:"write_throttled_time"`
}

// CollectionStats are stats that are tracked on a per-collection basis.
type CollectionStats struct {
	// The total number of times that the sync_function is evaluated for this collection.
	SyncFunctionCount *SgwIntStat `json:"sync_function_count"`
	// The total time spent evaluating the sync_function for this keyspace.
	SyncFunctionTime *SgwIntStat `json:"sync_function_time"`
	// The total number of documents rejected by the sync_function for this collection.
	SyncFunctionRejectCount *SgwIntStat `json:"sync_function_reject_count"`
	// The total number of documents rejected by write access functions (requireAccess, requireRole, requireUser) for this collection.
	SyncFunctionRejectAccessCount *SgwIntStat `json:"sync_function_reject_access_count"`
	// The total number of times the sync function encountered an exception for this collection.
	SyncFunctionExceptionCount *SgwIntStat `json:"sync_function_exception_count"`

	// The total number of documents imported to this collection since Sync Gateway node startup.
	ImportCount *SgwIntStat `json:"import_count"`

	// The total number of documents read from this collection since Sync Gateway node startup (i.e. sending to a client)
	NumDocReads *SgwIntStat `json:"num_doc_reads"`
	// The total number of bytes read from this collection as part of document writes since Sync Gateway node startup.
	DocReadsBytes *SgwIntStat `json:"doc_reads_bytes"`

	// The total number of documents written to this collection since Sync Gateway node startup (i.e. receiving from a client)
	NumDocWrites *SgwIntStat `json:"num_doc_writes"`
	// The total number of bytes written to this collection as part of document writes since Sync Gateway node startup.
	DocWritesBytes *SgwIntStat `json:"doc_writes_bytes"`

	// The total number of processed documents for resync on this collection.
	ResyncNumProcessed *SgwIntStat `json:"resync_num_processed"`
	// The total number of changed documents for resync on this collection.
	ResyncNumChanged *SgwIntStat `json:"resync_num_changed"`
}

type DatabaseStats struct {
	ReplicationBytesReceived *SgwIntStat `json:"replication_bytes_received"`
	ReplicationBytesSent     *SgwIntStat `json:"replication_bytes_sent"`
	// The compaction_attachment_start_time.
	CompactionAttachmentStartTime *SgwIntStat `json:"compaction_attachment_start_time"`
	// The compaction_tombstone_start_time.
	CompactionTombstoneStartTime *SgwIntStat `json:"compaction_tombstone_start_time"`
	// The total number of writes that left the document in a conflicted state. Includes new conflicts, and mutations that don’t resolve existing conflicts.
	ConflictWriteCount *SgwIntStat `json:"conflict_write_count"`
	// The total number of instances during import when the document cas had changed, but the document was not imported because the document body had not changed.
	Crc32MatchCount *SgwIntStat `json:"crc32c_match_count"`
	// The total number of DCP mutations added to Sync Gateway’s channel cache.
	DCPCachingCount *SgwIntStat `json:"dcp_caching_count"`
	// The total time between a DCP mutation arriving at Sync Gateway and being added to channel cache.
	DCPCachingTime *SgwIntStat `json:"dcp_caching_time"`
	// The total number of document mutations received by Sync Gateway over DCP.
	DCPReceivedCount *SgwIntStat `json:"dcp_received_count"`
	// The time between a document write and that document being received by Sync Gateway over DCP. If the document was written prior to Sync Gateway starting the feed, it is recorded as the time since the feed was started.
	DCPReceivedTime *SgwIntStat `json:"dcp_received_time"`
	// The total number of bytes read via Couchbase Lite 2.x replication since Sync Gateway node startup.
	DocReadsBytesBlip *SgwIntStat `json:"doc_reads_bytes_blip"`
	// The total number of bytes written as part of document writes since Sync Gateway node startup.
	DocWritesBytes *SgwIntStat `json:"doc_writes_bytes"`
	// The total number of bytes written as part of Couchbase Lite document writes since Sync Gateway node startup.
	DocWritesBytesBlip *SgwIntStat `json:"doc_writes_bytes_blip"`
	// The total size of xattrs written (in bytes).
	DocWritesXattrBytes *SgwIntStat `json:"doc_writes_xattr_bytes"`
	// Highest sequence number seen on the caching DCP feed.
	HighSeqFeed *SgwIntStat `json:"high_seq_feed"`
	// The number of attachments compacted
	NumAttachmentsCompacted *SgwIntStat `json:"num_attachments_compacted"`
	// The total number of documents read via Couchbase Lite 2.x replication since Sync Gateway node startup.
	NumDocReadsBlip *SgwIntStat `json:"num_doc_reads_blip"`
	// The total number of documents read via the REST API since Sync Gateway node startup. Includes Couchbase Lite 1.x replication.
	NumDocReadsRest *SgwIntStat `json:"num_doc_reads_rest"`
	// The total number of documents written by any means (replication, rest API interaction or imports) since Sync Gateway node startup.
	NumDocWrites *SgwIntStat `json:"num_doc_writes"`
	// NumDocWritesRejected is the total number of document writes that were rejected by Sync Gateway.
	NumDocWritesRejected *SgwIntStat `json:"num_doc_writes_rejected"`
	// The total number of requests sent over the public REST api
	NumPublicRestRequests *SgwIntStat `json:"num_public_rest_requests"`
	// The total number of active replications.
	NumReplicationsActive *SgwIntStat `json:"num_replications_active"`
	// The total number of replications created since Sync Gateway node startup.
	NumReplicationsTotal   *SgwIntStat `json:"num_replications_total"`
	NumTombstonesCompacted *SgwIntStat `json:"num_tombstones_compacted"`
	// Number of bytes written over public interface for REST api
	PublicRestBytesWritten *SgwIntStat `json:"public_rest_bytes_written"`
	// The total amount of bytes read over the public REST api
	PublicRestBytesRead *SgwIntStat `json:"public_rest_bytes_read"`
	// The value of the last sequence number assigned. Callers using Set should be holding a mutex or ensure concurrent updates to this value are otherwise safe.
	LastSequenceAssignedValue *SgwIntStat `json:"last_sequence_assigned_value"` // TODO: CBG-4579 - Replace with SgwUintStat stat
	// The total number of sequence numbers assigned.
	SequenceAssignedCount *SgwIntStat `json:"sequence_assigned_count"`
	// The total number of high sequence lookups.
	SequenceGetCount *SgwIntStat `json:"sequence_get_count"`
	// The total number of times the sequence counter document has been incremented.
	SequenceIncrCount *SgwIntStat `json:"sequence_incr_count"`
	// The total number of unused, reserved sequences released by Sync Gateway.
	SequenceReleasedCount *SgwIntStat `json:"sequence_released_count"`
	// The value of the last sequence number reserved (which may not yet be assigned). Callers using Set should be holding a mutex or ensure concurrent updates to this value are otherwise safe.
	LastSequenceReservedValue *SgwIntStat `json:"last_sequence_reserved_value"` // TODO: CBG-4579 - Replace with SgwUintStat stat
	// The total number of sequences reserved by Sync Gateway.
	SequenceReservedCount *SgwIntStat `json:"sequence_reserved_count"`
	// The total number of corrupt sequences above the MaxSequencesToRelease threshold seen at the sequence allocator
	CorruptSequenceCount *SgwIntStat `json:"corrupt_sequence_count"`
	// The total number of warnings relating to the channel name size.
	WarnChannelNameSizeCount *SgwIntStat `json:"warn_channel_name_size_count"`
	// The total number of warnings relating to the channel count exceeding the channel count threshold.
	WarnChannelsPerDocCount *SgwIntStat `json:"warn_channels_per_doc_count"`
	// The total number of warnings relating to the grant count exceeding the grant count threshold.
	WarnGrantsPerDocCount *SgwIntStat `json:"warn_grants_per_doc_count"`
	// The total number of warnings relating to the xattr sync data being larger than a configured threshold.
	WarnXattrSizeCount *SgwIntStat `json:"warn_xattr_size_count"`
	// The total number of times that a sync function was evaluated for the database (across all collections).
	SyncFunctionCount *SgwIntStat `json:"sync_function_count"`
	// The total time spent evaluating a sync function (across all collections).
	SyncFunctionTime *SgwIntStat `json:"sync_function_time"`
	// The total sync time is a proxy for websocket connections. Tracking long lived and potentially idle connections.
	// This stat represents the continually growing number of connections per sec.
	TotalSyncTime *SgwIntStat `json:"total_sync_time"`
	// The total number of times that a sync function encountered an exception (across all collections).
	SyncFunctionExceptionCount *SgwIntStat `json:"sync_function_exception_count"`
	// The total number of times a replication connection is rejected due ot it being over the threshold
	NumReplicationsRejectedLimit *SgwIntStat `json:"num_replications_rejected_limit"`
	// The total number of processed documents for resync on this database.
	ResyncNumProcessed *SgwIntStat `json:"resync_num_processed"`
	// The total number of changed documents for resync on this database.
	ResyncNumChanged *SgwIntStat `json:"resync_num_changed"`

	// These can be cleaned up in future versions of SGW, implemented as maps to reduce amount of potential risk
	// prior to Hydrogen release. These are not exported as part of prometheus and only exposed through expvars
	CacheFeedMapStats  *ExpVarMapWrapper `json:"cache_feed"`
	ImportFeedMapStats *ExpVarMapWrapper `json:"import_feed"`

	// The total number of errors that occurred that prevented the database from being initialized.
	TotalInitFatalErrors *SgwIntStat `json:"total_init_fatal_errors"`
	// The total number of errors that occurred that prevented the database from being brought online.
	TotalOnlineFatalErrors *SgwIntStat `json:"total_online_fatal_errors"`
	// NumPublicAllDocsRequests is the total number of requests to /_all_docs on the public interface.
	NumPublicAllDocsRequests *SgwIntStat `json:"num_public_all_docs_requests"`
	// NumDocsPostFilterPublicAllDocs is the total number of documents returned after filtering for /_all_docs on the public interface.
	NumDocsPostFilterPublicAllDocs *SgwIntStat `json:"num_docs_post_filter_public_all_docs"`
	// NumDocsPreFilterPublicAllDocs is the total number of documents returned before filtering for /_all_docs on the public interface.
	NumDocsPreFilterPublicAllDocs *SgwIntStat `json:"num_docs_pre_filter_public_all_docs"`
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
	// The total number of requested deltas that were available in the revision cache.
	DeltaCacheHit *SgwIntStat `json:"delta_cache_hit"`
	// The total number of requested deltas that were not available in the revision cache.
	DeltaCacheMiss *SgwIntStat `json:"delta_cache_miss"`
	// The number of delta replications that have been run.
	DeltaPullReplicationCount *SgwIntStat `json:"delta_pull_replication_count"`
	// The total number of documents pushed as a delta from a previous revision.
	DeltaPushDocCount *SgwIntStat `json:"delta_push_doc_count"`
	// The total number of times a revision is sent as delta from a previous revision.
	DeltasRequested *SgwIntStat `json:"deltas_requested"`
	// The total number of revisions sent to clients as deltas.
	DeltasSent *SgwIntStat `json:"deltas_sent"`
}

type QueryStats struct {
	Stats map[string]*QueryStat
	mutex sync.Mutex
}

type DbReplicatorStats struct {
	// The total number of bytes in all the attachments that were pushed since replication started.
	NumAttachmentBytesPushed *SgwIntStat `json:"sgr_num_attachment_bytes_pushed"`
	// The total number of attachments that were pushed since replication started.
	NumAttachmentPushed *SgwIntStat `json:"sgr_num_attachments_pushed"`
	// The total number of documents that were pushed since replication started.
	//
	// Used by Inter-Sync Gateway and SG Replicate.
	NumDocPushed *SgwIntStat `json:"sgr_num_docs_pushed"`
	// The total number of documents that failed to be pushed since replication started.
	//
	// Used by Inter-Sync Gateway and SG Replicate
	NumDocsFailedToPush *SgwIntStat `json:"sgr_num_docs_failed_to_push"`
	// The total number of pushed documents that conflicted since replication started.
	PushConflictCount *SgwIntStat `json:"sgr_push_conflict_count"`
	// The total number of pushed documents that were rejected since replication started.
	PushRejectedCount *SgwIntStat `json:"sgr_push_rejected_count"`
	// The total number of deltas sent
	PushDeltaSentCount *SgwIntStat `json:"sgr_deltas_sent"`
	// The total number of documents checked for changes since replication started. This represents the number of potential change notifications pushed by Sync Gateway.
	//
	// This is not necessarily the number of documents pushed, as a given target might already have the change. Used by Inter-Sync Gateway and SG Replicate
	DocsCheckedSent          *SgwIntStat `json:"sgr_docs_checked_sent" `
	NumConnectAttemptsPull   *SgwIntStat `json:"sgr_num_connect_attempts_pull"`
	NumReconnectsAbortedPull *SgwIntStat `json:"sgr_num_reconnects_aborted_pull"`

	// The total number of bytes in all the attachments that were pulled since replication started.
	NumAttachmentBytesPulled *SgwIntStat `json:"sgr_num_attachment_bytes_pulled"`
	// The total number of attachments that were pulled since replication started.
	NumAttachmentsPulled *SgwIntStat `json:"sgr_num_attachments_pulled"`
	// The total number of documents that were pulled since replication started.
	PulledCount *SgwIntStat `json:"sgr_num_docs_pulled"`
	// The total number of documents that were purged since replication started.
	PurgedCount *SgwIntStat `json:"sgr_num_docs_purged"`
	// The total number of document pulls that failed since replication started.
	FailedToPullCount *SgwIntStat `json:"sgr_num_docs_failed_to_pull"`
	// The total number of documents that were purged since replication started.
	DeltaReceivedCount *SgwIntStat `json:"sgr_deltas_recv"`
	// The total number of deltas requested
	DeltaRequestedCount *SgwIntStat `json:"sgr_deltas_requested"`
	// The total number of documents that were purged since replication started.
	DocsCheckedReceived      *SgwIntStat `json:"sgr_docs_checked_recv"`
	NumConnectAttemptsPush   *SgwIntStat `json:"sgr_num_connect_attempts_push"`
	NumReconnectsAbortedPush *SgwIntStat `json:"sgr_num_reconnects_aborted_push"`

	// The total number of conflicting documents that were resolved successfully locally (by the active replicator).
	ConflictResolvedLocalCount *SgwIntStat `json:"sgr_conflict_resolved_local_count"`
	// The total number of conflicting documents that were resolved successfully remotely (by the active replicator).
	ConflictResolvedRemoteCount *SgwIntStat `json:"sgr_conflict_resolved_remote_count"`
	// The total number of conflicting documents that were resolved successfully by a merge action (by the active replicator)
	ConflictResolvedMergedCount *SgwIntStat `json:"sgr_conflict_resolved_merge_count"`

	// The number of times a handler panicked and didn't know how to recover from it.
	NumHandlersPanicked *SgwIntStat `json:"-"`
	// Internal stats for the lengths of expectedSeqs/processedSeqs lists in the ISGR checkpointer.
	ExpectedSequenceLen             *SgwIntStat `json:"expected_seq_len,omitempty"`
	ExpectedSequenceLenPostCleanup  *SgwIntStat `json:"expected_seq_len_post_cleanup,omitempty"`
	ProcessedSequenceLen            *SgwIntStat `json:"processed_seq_len,omitempty"`
	ProcessedSequenceLenPostCleanup *SgwIntStat `json:"processed_seq_len_post_cleanup,omitempty"`
}

type SecurityStats struct {
	// The total number of unsuccessful authentications.
	AuthFailedCount *SgwIntStat `json:"auth_failed_count"`
	// The total number of successful authentications.
	AuthSuccessCount *SgwIntStat `json:"auth_success_count"`
	// The total number of documents rejected by write access functions (requireAccess, requireRole, requireUser).
	NumAccessErrors *SgwIntStat `json:"num_access_errors"`
	// The total number of documents rejected by the sync_function.
	NumDocsRejected *SgwIntStat `json:"num_docs_rejected"`
	// The total time spent in authenticating all requests.
	TotalAuthTime *SgwIntStat `json:"total_auth_time"`
}

type SharedBucketImportStats struct {
	// The total number of docs imported.
	ImportCount *SgwIntStat `json:"import_count"`
	// The total number of imports cancelled due to cas failure.
	ImportCancelCAS *SgwIntStat `json:"import_cancel_cas"`
	// The total number of errors arising as a result of a document import.
	ImportErrorCount *SgwIntStat `json:"import_error_count"`
	// The total time taken to process a document import.
	ImportProcessingTime *SgwIntStat `json:"import_processing_time"`
	// The highest sequence number value imported.
	ImportHighSeq *SgwIntStat `json:"import_high_seq"`
	// The total number of import partitions.
	ImportPartitions *SgwIntStat `json:"import_partitions"`
	// The total number of documents processed by the import feed.
	ImportFeedProcessedCount *SgwIntStat `json:"import_feed_processed_count"`
}

type SgwStatWrapper interface {
	FormatString() string
	Name() string
	Unit() string
	Help() string
	AddedVersion() string
	DeprecatedVersion() string
	Stability() string
	LabelKeys() []string
	ValueTypeString() string
}

type SgwStat struct {
	statFQN           string
	unit              string
	help              string
	addedVersion      string
	deprecatedVersion string
	stability         string
	labels            map[string]string
	statValueType     prometheus.ValueType
	statDesc          *prometheus.Desc
}

// Name returns the fully qualified name of the stat.
// Currently only used for the stat metadata exporter tool.
func (s SgwStat) Name() string {
	return s.statFQN
}

// Unit returns the units the stat uses for example, seconds.
// Currently only used for the stat metadata exporter tool.
func (s SgwStat) Unit() string {
	return s.unit
}

// Help returns the help text for the stat.
// Currently only used for the stat metadata exporter tool.
func (s SgwStat) Help() string {
	return s.help
}

// AddedVersion returns the version of Sync Gateway this stat was added.
// Currently only used for the stat metadata exporter tool.
func (s SgwStat) AddedVersion() string {
	return s.addedVersion
}

// DeprecatedVersion returns the version of Sync Gateway this stat was deprecated.
// Currently only used for the stat metadata exporter tool.
func (s SgwStat) DeprecatedVersion() string {
	return s.deprecatedVersion
}

// Stability returns if there is a commitment to keep this stat stable.
// Currently only used for the stat metadata exporter tool.
func (s SgwStat) Stability() string {
	return s.stability
}

// LabelKeys returns the label keys for the stat in deterministic order.
// Currently only used for the stat metadata exporter tool.
func (s SgwStat) LabelKeys() []string {
	labelKeys := make([]string, 0, len(s.labels))
	for labelKey := range s.labels {
		labelKeys = append(labelKeys, labelKey)
	}

	// Sort the label keys so that the order is deterministic
	slices.Sort(labelKeys)

	return labelKeys
}

// ValueTypeString returns the string representation of the prometheus.ValueType
// Currently only used for the stat metadata exporter tool.
func (s SgwStat) ValueTypeString() string {
	switch s.statValueType {
	case prometheus.CounterValue:
		return PrometheusValueTypeCounter
	case prometheus.GaugeValue:
		return PrometheusValueTypeGauge
	}
	return ""
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

func (s *SgwBoolStat) FormatString() string {
	return StatFormatBool
}

func newSGWStat(subsystem, key, unit, description, addedVersion, deprecatedVersion, stability string, labelKeys, labelVals []string, statValueType prometheus.ValueType) (*SgwStat, error) {
	// Validate required fields have been specified
	if description == "" {
		return nil, fmt.Errorf("attempting to register stat with no description specified")
	}
	if addedVersion == "" {
		return nil, fmt.Errorf("attempting to register stat with no added version specified")
	}
	if stability == "" {
		return nil, fmt.Errorf("attempting to register stat with no stability specified")
	}

	// Build stat
	name := prometheus.BuildFQName(NamespaceKey, subsystem, key)

	constLabels := make(prometheus.Labels)
	for i, labelKey := range labelKeys {
		constLabels[labelKey] = labelVals[i]
	}

	desc := prometheus.NewDesc(name, description, nil, constLabels)

	stat := &SgwStat{
		statFQN:           name,
		unit:              unit,
		help:              description,
		addedVersion:      addedVersion,
		deprecatedVersion: deprecatedVersion,
		stability:         stability,
		labels:            constLabels,
		statValueType:     statValueType,
		statDesc:          desc,
	}
	return stat, nil
}

func NewIntStat(subsystem, key, unit, description, addedVersion, deprecatedVersion, stability string, labelKeys, labelVals []string, statValueType prometheus.ValueType, initialValue int64) (*SgwIntStat, error) {
	stat, err := newSGWStat(subsystem, key, unit, description, addedVersion, deprecatedVersion, stability, labelKeys, labelVals, statValueType)
	if err != nil {
		return nil, err
	}

	wrappedStat := &SgwIntStat{
		SgwStat: *stat,
	}

	wrappedStat.Set(initialValue)

	if !SkipPrometheusStatsRegistration {
		err := prometheus.Register(wrappedStat)
		if err != nil {
			return nil, err
		}
	}

	return wrappedStat, nil
}

func (s *SgwIntStat) FormatString() string {
	return StatFormatInt
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

func NewFloatStat(subsystem, key, unit, description, addedVersion, deprecatedVersion, stability string, labelKeys, labelVals []string, statValueType prometheus.ValueType, initialValue float64) (*SgwFloatStat, error) {
	stat, err := newSGWStat(subsystem, key, unit, description, addedVersion, deprecatedVersion, stability, labelKeys, labelVals, statValueType)
	if err != nil {
		return nil, err
	}

	wrappedStat := &SgwFloatStat{
		SgwStat: *stat,
		Val:     math.Float64bits(initialValue),
	}

	if !SkipPrometheusStatsRegistration {
		err := prometheus.Register(wrappedStat)
		if err != nil {
			return nil, err
		}
	}

	return wrappedStat, nil
}

func (s *SgwFloatStat) FormatString() string {
	return StatFormatFloat
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
func NewDurStat(subsystem, key, unit, description, addedVersion, deprecatedVersion, stability string, labelKeys, labelVals []string, statValueType prometheus.ValueType, initialValue time.Time) (*SgwDurStat, error) {
	stat, err := newSGWStat(subsystem, key, unit, description, addedVersion, deprecatedVersion, stability, labelKeys, labelVals, statValueType)
	if err != nil {
		return nil, err
	}

	wrappedStat := &SgwDurStat{
		SgwStat:   *stat,
		StartTime: initialValue,
	}

	if !SkipPrometheusStatsRegistration {
		err := prometheus.Register(wrappedStat)
		if err != nil {
			return nil, err
		}
	}

	return wrappedStat, nil
}

func (s *SgwDurStat) FormatString() string {
	return StatFormatDuration
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

func (s *SgwStats) NewDBStats(name string, deltaSyncEnabled bool, importEnabled bool, viewsEnabled bool, queryNames []string, collections []string) (*DbStats, error) {
	s.dbStatsMapMutex.Lock()
	defer s.dbStatsMapMutex.Unlock()
	dbStats := &DbStats{
		dbName:            name,
		DbReplicatorStats: make(map[string]*DbReplicatorStats),
	}

	// These have a pretty good chance of being used so we'll initialise these for every database stat struct created
	err := dbStats.initCacheStats()
	if err != nil {
		return nil, err
	}
	err = dbStats.initCBLReplicationPullStats()
	if err != nil {
		return nil, err
	}
	err = dbStats.initCBLReplicationPushStats()
	if err != nil {
		return nil, err
	}
	err = dbStats.initDatabaseStats()
	if err != nil {
		return nil, err
	}
	err = dbStats.initSecurityStats()
	if err != nil {
		return nil, err
	}

	err = dbStats.InitCollectionStats(collections...)
	if err != nil {
		return nil, err
	}

	if deltaSyncEnabled {
		err = dbStats.InitDeltaSyncStats()
		if err != nil {
			return nil, err
		}
	}

	if importEnabled {
		err = dbStats.InitSharedBucketImportStats()
		if err != nil {
			return nil, err
		}
	}

	if viewsEnabled {
		err = dbStats.InitQueryStats(
			true,
			queryNames...,
		)
	} else {
		err = dbStats.InitQueryStats(
			false,
			queryNames...,
		)
	}
	if err != nil {
		return nil, err
	}

	s.DbStats[name] = dbStats
	return dbStats, nil
}

func (s *SgwStats) ClearDBStats(name string) {
	s.dbStatsMapMutex.Lock()
	defer s.dbStatsMapMutex.Unlock()

	if _, ok := s.DbStats[name]; !ok {
		return
	}

	for scopeAndCollectionName := range s.DbStats[name].CollectionStats {
		s.DbStats[name].unregisterCollectionStats(scopeAndCollectionName)
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

func (d *DbStats) initCacheStats() error {
	var err error
	resUtil := &CacheStats{}
	labelKeys := []string{DatabaseLabelKey}
	labelVals := []string{d.dbName}

	resUtil.AbandonedSeqs, err = NewIntStat(SubsystemCacheKey, "abandoned_seqs", StatUnitNoUnits, AbandonedSeqsDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ChannelCacheRevsActive, err = NewIntStat(SubsystemCacheKey, "chan_cache_active_revs", StatUnitNoUnits, ChanCacheActiveRevsDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.ChannelCacheBypassCount, err = NewIntStat(SubsystemCacheKey, "chan_cache_bypass_count", StatUnitNoUnits, ChanCacheBypassCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ChannelCacheChannelsAdded, err = NewIntStat(SubsystemCacheKey, "chan_cache_channels_added", StatUnitNoUnits, ChanCacheChannelsAddedDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ChannelCacheChannelsEvictedInactive, err = NewIntStat(SubsystemCacheKey, "chan_cache_channels_evicted_inactive", StatUnitNoUnits, ChanCacheChannelsEvictedInactiveDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ChannelCacheChannelsEvictedNRU, err = NewIntStat(SubsystemCacheKey, "chan_cache_channels_evicted_nru", StatUnitNoUnits, ChanCacheChannelsEvictedNRUDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ChannelCacheCompactCount, err = NewIntStat(SubsystemCacheKey, "chan_cache_compact_count", StatUnitNoUnits, ChanCacheCompactCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ChannelCacheCompactTime, err = NewIntStat(SubsystemCacheKey, "chan_cache_compact_time", StatUnitNanoseconds, ChanCacheCompactTimeDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ChannelCacheHits, err = NewIntStat(SubsystemCacheKey, "chan_cache_hits", StatUnitNoUnits, ChanCacheHitsDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ChannelCacheMaxEntries, err = NewIntStat(SubsystemCacheKey, "chan_cache_max_entries", StatUnitNoUnits, ChanCacheMaxEntriesDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.ChannelCacheMisses, err = NewIntStat(SubsystemCacheKey, "chan_cache_misses", StatUnitNoUnits, ChanCacheMissesDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ChannelCacheNumChannels, err = NewIntStat(SubsystemCacheKey, "chan_cache_num_channels", StatUnitNoUnits, ChanCacheNumChannelsDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.ChannelCachePendingQueries, err = NewIntStat(SubsystemCacheKey, "chan_cache_pending_queries", StatUnitNoUnits, ChanCachePendingQueriesDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.ChannelCacheRevsRemoval, err = NewIntStat(SubsystemCacheKey, "chan_cache_removal_revs", StatUnitNoUnits, ChanCacheRemovalRevsDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.ChannelCacheRevsTombstone, err = NewIntStat(SubsystemCacheKey, "chan_cache_tombstone_revs", StatUnitNoUnits, ChanCacheTombstoneRevsDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.HighSeqCached, err = NewIntStat(SubsystemCacheKey, "high_seq_cached", StatUnitNoUnits, HighSeqCachedDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.HighSeqStable, err = NewIntStat(SubsystemCacheKey, "high_seq_stable", StatUnitNoUnits, HighStableSeqCachedDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NonMobileIgnoredCount, err = NewIntStat(SubsystemCacheKey, "non_mobile_ignored_count", StatUnitNoUnits, NonMobileIgnoredCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumActiveChannels, err = NewIntStat(SubsystemCacheKey, "num_active_channels", StatUnitNoUnits, NumActiveChannelsDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumSkippedSeqs, err = NewIntStat(SubsystemCacheKey, "num_skipped_seqs", StatUnitNoUnits, NumSkippedSeqsDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.PendingSeqLen, err = NewIntStat(SubsystemCacheKey, "pending_seq_len", StatUnitNoUnits, PendingSeqLengthDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.RevisionCacheNumItems, err = NewIntStat(SubsystemCacheKey, "revision_cache_num_items", StatUnitNoUnits, RevCacheNumItemsDesc, StatAddedVersion3dot2dot1, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.RevisionCacheBypass, err = NewIntStat(SubsystemCacheKey, "rev_cache_bypass", StatUnitNoUnits, RevCacheBypassDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.RevisionCacheHits, err = NewIntStat(SubsystemCacheKey, "rev_cache_hits", StatUnitNoUnits, RevCacheHitsDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.RevisionCacheMisses, err = NewIntStat(SubsystemCacheKey, "rev_cache_misses", StatUnitNoUnits, RevCacheMissesDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.RevisionCacheTotalMemory, err = NewIntStat(SubsystemCacheKey, "revision_cache_total_memory", StatUnitNoUnits, RevCacheMemoryDesc, StatAddedVersion3dot2dot1, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.DeprecatedSkippedSeqLen, err = NewIntStat(SubsystemCacheKey, "skipped_seq_len", StatUnitNoUnits, SkippedSeqLengthDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersion3dot2dot2, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.DeprecatedSkippedSeqCap, err = NewIntStat(SubsystemCacheKey, "skipped_seq_cap", StatUnitNoUnits, SkippedSeqCapDesc, StatAddedVersion3dot2dot0, StatDeprecatedVersion3dot2dot2, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.SkippedSequenceSkiplistNodes, err = NewIntStat(SubsystemCacheKey, "skipped_sequence_skip_list_nodes", StatUnitNoUnits, SkippedSequenceNodesDesc, StatAddedVersion3dot3dot0, StatDeprecatedVersionNotDeprecated, StatStabilityInternal, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumCurrentSeqsSkipped, err = NewIntStat(SubsystemCacheKey, "current_skipped_seq_count", StatUnitNoUnits, NumCurrentSkippedSeq, StatAddedVersion3dot2dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ViewQueries, err = NewIntStat(SubsystemCacheKey, "view_queries", StatUnitNoUnits, ViewQueriesDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}

	d.CacheStats = resUtil
	return nil
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
	prometheus.Unregister(d.CacheStats.DeprecatedSkippedSeqCap)
	prometheus.Unregister(d.CacheStats.NumCurrentSeqsSkipped)
	prometheus.Unregister(d.CacheStats.PendingSeqLen)
	prometheus.Unregister(d.CacheStats.RevisionCacheNumItems)
	prometheus.Unregister(d.CacheStats.RevisionCacheBypass)
	prometheus.Unregister(d.CacheStats.RevisionCacheHits)
	prometheus.Unregister(d.CacheStats.RevisionCacheMisses)
	prometheus.Unregister(d.CacheStats.RevisionCacheTotalMemory)
	prometheus.Unregister(d.CacheStats.DeprecatedSkippedSeqLen)
	prometheus.Unregister(d.CacheStats.SkippedSequenceSkiplistNodes)
	prometheus.Unregister(d.CacheStats.ViewQueries)
}

func (d *DbStats) Cache() *CacheStats {
	return d.CacheStats
}

func (d *DbStats) initCBLReplicationPullStats() error {
	var err error
	resUtil := &CBLReplicationPullStats{}
	labelKeys := []string{DatabaseLabelKey}
	labelVals := []string{d.dbName}

	resUtil.AttachmentPullBytes, err = NewIntStat(SubsystemReplicationPull, "attachment_pull_bytes", StatUnitBytes, AttachmentPullBytesDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.AttachmentPullCount, err = NewIntStat(SubsystemReplicationPull, "attachment_pull_count", StatUnitNoUnits, AttachmentPullCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.MaxPending, err = NewIntStat(SubsystemReplicationPull, "max_pending", StatUnitNoUnits, MaxPendingDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumReplicationsActive, err = NewIntStat(SubsystemReplicationPull, "num_replications_active", StatUnitNoUnits, NumReplicationsActiveDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumPullReplActiveContinuous, err = NewIntStat(SubsystemReplicationPull, "num_pull_repl_active_continuous", StatUnitNoUnits, NumPullRepliActiveContinuousDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumPullReplActiveOneShot, err = NewIntStat(SubsystemReplicationPull, "num_pull_repl_active_one_shot", StatUnitNoUnits, NumPullRepliActiveOneShotDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumPullReplCaughtUp, err = NewIntStat(SubsystemReplicationPull, "num_pull_repl_caught_up", StatUnitNoUnits, NumPullRepliCaughtUpDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumPullReplTotalCaughtUp, err = NewIntStat(SubsystemReplicationPull, "num_pull_repl_total_caught_up", StatUnitNoUnits, NumPullRepliTotalCaughtUpDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumPullReplSinceZero, err = NewIntStat(SubsystemReplicationPull, "num_pull_repl_since_zero", StatUnitNoUnits, NumPullRepliSinceZeroDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumPullReplTotalContinuous, err = NewIntStat(SubsystemReplicationPull, "num_pull_repl_total_continuous", StatUnitNoUnits, NumPullRepliContinuousDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumPullReplTotalOneShot, err = NewIntStat(SubsystemReplicationPull, "num_pull_repl_total_one_shot", StatUnitNoUnits, NumPullRepliTotalOneshotDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.RequestChangesCount, err = NewIntStat(SubsystemReplicationPull, "request_changes_count", StatUnitNoUnits, RequestChangesCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.RequestChangesTime, err = NewIntStat(SubsystemReplicationPull, "request_changes_time", StatUnitNanoseconds, RequestChangesTimeDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.RevProcessingTime, err = NewIntStat(SubsystemReplicationPull, "rev_processing_time", StatUnitNanoseconds, RevProcessingTimeDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.RevSendCount, err = NewIntStat(SubsystemReplicationPull, "rev_send_count", StatUnitNoUnits, RevSendCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NoRevSendCount, err = NewIntStat(SubsystemReplicationPull, "norev_send_count", StatUnitNoUnits, NoRevSendCountDesc, StatAddedVersion3dot2dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ReplacementRevSendCount, err = NewIntStat(SubsystemReplicationPull, "replacement_rev_send_count", StatUnitNoUnits, ReplacementRevSendCountDesc, StatAddedVersion3dot2dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.RevErrorCount, err = NewIntStat(SubsystemReplicationPull, "rev_error_count", StatUnitNoUnits, RevErrorCountDesc, StatAddedVersion3dot2dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.RevSendLatency, err = NewIntStat(SubsystemReplicationPull, "rev_send_latency", StatUnitNanoseconds, RevSendLatencyDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}

	d.CBLReplicationPullStats = resUtil
	return nil
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
	prometheus.Unregister(d.CBLReplicationPullStats.NoRevSendCount)
	prometheus.Unregister(d.CBLReplicationPullStats.ReplacementRevSendCount)
	prometheus.Unregister(d.CBLReplicationPullStats.RevErrorCount)
	prometheus.Unregister(d.CBLReplicationPullStats.RevSendLatency)
}

func (d *DbStats) CBLReplicationPull() *CBLReplicationPullStats {
	return d.CBLReplicationPullStats
}

func (d *DbStats) initCBLReplicationPushStats() error {
	var err error
	resUtil := &CBLReplicationPushStats{}
	labelKeys := []string{DatabaseLabelKey}
	labelVals := []string{d.dbName}

	resUtil.AttachmentPushBytes, err = NewIntStat(SubsystemReplicationPush, "attachment_push_bytes", StatUnitBytes, AttachmentPushBytesDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.AttachmentPushCount, err = NewIntStat(SubsystemReplicationPush, "attachment_push_count", StatUnitNoUnits, AttachmentPushCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.DocPushCount, err = NewIntStat(SubsystemReplicationPush, "doc_push_count", StatUnitNoUnits, DocPushCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.DocPushErrorCount, err = NewIntStat(SubsystemReplicationPush, "doc_push_error_count", StatUnitNoUnits, DocPushErrorCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.ProposeChangeCount, err = NewIntStat(SubsystemReplicationPush, "propose_change_count", StatUnitNoUnits, ProposeChangeCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ProposeChangeTime, err = NewIntStat(SubsystemReplicationPush, "propose_change_time", StatUnitNanoseconds, ProposeChangeTimeDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.WriteProcessingTime, err = NewIntStat(SubsystemReplicationPush, "write_processing_time", StatUnitNanoseconds, WriteProcessingTimeDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.WriteThrottledCount, err = NewIntStat(SubsystemReplicationPush, "write_throttled_count", StatUnitNoUnits, WriteThrottledCountDesc, StatAddedVersion3dot1dot4, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.WriteThrottledTime, err = NewIntStat(SubsystemReplicationPush, "write_throttled_time", StatUnitNanoseconds, WriteThrottledTimeDesc, StatAddedVersion3dot1dot4, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}

	d.CBLReplicationPushStats = resUtil
	return nil
}

func (d *DbStats) unregisterCBLReplicationPushStats() {
	prometheus.Unregister(d.CBLReplicationPushStats.AttachmentPushBytes)
	prometheus.Unregister(d.CBLReplicationPushStats.AttachmentPushCount)
	prometheus.Unregister(d.CBLReplicationPushStats.DocPushCount)
	prometheus.Unregister(d.CBLReplicationPushStats.DocPushErrorCount)
	prometheus.Unregister(d.CBLReplicationPushStats.ProposeChangeCount)
	prometheus.Unregister(d.CBLReplicationPushStats.ProposeChangeTime)
	prometheus.Unregister(d.CBLReplicationPushStats.WriteProcessingTime)
	prometheus.Unregister(d.CBLReplicationPushStats.WriteThrottledCount)
	prometheus.Unregister(d.CBLReplicationPushStats.WriteThrottledTime)
}

func (d *DbStats) CBLReplicationPush() *CBLReplicationPushStats {
	return d.CBLReplicationPushStats
}

func (d *DbStats) initDatabaseStats() error {
	var err error
	resUtil := &DatabaseStats{}
	labelKeys := []string{DatabaseLabelKey}
	labelVals := []string{d.dbName}

	resUtil.ReplicationBytesReceived, err = NewIntStat(SubsystemDatabaseKey, "replication_bytes_received", StatUnitBytes, ReplicationBytesReceivedDesc, StatAddedVersion3dot2dot0, StatDeprecatedVersionNotDeprecated, StatStabilityVolatile, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ReplicationBytesSent, err = NewIntStat(SubsystemDatabaseKey, "replication_bytes_sent", StatUnitBytes, ReplicationBytesSentDesc, StatAddedVersion3dot2dot0, StatDeprecatedVersionNotDeprecated, StatStabilityVolatile, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.CompactionAttachmentStartTime, err = NewIntStat(SubsystemDatabaseKey, "compaction_attachment_start_time", StatUnitUnixTimestamp, CompactionAttachmentStartTimeDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.CompactionTombstoneStartTime, err = NewIntStat(SubsystemDatabaseKey, "compaction_tombstone_start_time", StatUnitUnixTimestamp, CompactionTombstoneStartTimeDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.ConflictWriteCount, err = NewIntStat(SubsystemDatabaseKey, "conflict_write_count", StatUnitNoUnits, ConflictWriteCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.Crc32MatchCount, err = NewIntStat(SubsystemDatabaseKey, "crc32c_match_count", StatUnitNoUnits, Crc32MatchCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.DCPCachingCount, err = NewIntStat(SubsystemDatabaseKey, "dcp_caching_count", StatUnitNoUnits, DCPCachingCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.DCPCachingTime, err = NewIntStat(SubsystemDatabaseKey, "dcp_caching_time", StatUnitNanoseconds, DCPCachingTimeDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.DCPReceivedCount, err = NewIntStat(SubsystemDatabaseKey, "dcp_received_count", StatUnitNoUnits, DCPReceivedCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.DCPReceivedTime, err = NewIntStat(SubsystemDatabaseKey, "dcp_received_time", StatUnitNanoseconds, DCPReceivedTimeDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.DocReadsBytesBlip, err = NewIntStat(SubsystemDatabaseKey, "doc_reads_bytes_blip", StatUnitBytes, DocReadsBytesBlipDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.DocWritesBytes, err = NewIntStat(SubsystemDatabaseKey, "doc_writes_bytes", StatUnitBytes, DocWritesBytesDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.DocWritesXattrBytes, err = NewIntStat(SubsystemDatabaseKey, "doc_writes_xattr_bytes", StatUnitBytes, DocWritesXattrBytesDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.HighSeqFeed, err = NewIntStat(SubsystemDatabaseKey, "high_seq_feed", StatUnitNoUnits, HighSeqFeedDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.PublicRestBytesWritten, err = NewIntStat(SubsystemDatabaseKey, "http_bytes_written", StatUnitBytes, PublicRestBytesWrittenDesc, StatAddedVersion3dot2dot0, StatDeprecatedVersionNotDeprecated, StatStabilityVolatile, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumAttachmentsCompacted, err = NewIntStat(SubsystemDatabaseKey, "num_attachments_compacted", StatUnitNoUnits, NumAttachmentsCompactedDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.DocWritesBytesBlip, err = NewIntStat(SubsystemDatabaseKey, "doc_writes_bytes_blip", StatUnitBytes, DocWritesBytesBlipDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumDocReadsBlip, err = NewIntStat(SubsystemDatabaseKey, "num_doc_reads_blip", StatUnitNoUnits, NumDocsReadsBlipDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumDocReadsRest, err = NewIntStat(SubsystemDatabaseKey, "num_doc_reads_rest", StatUnitNoUnits, NumDocReadsRestDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumDocWrites, err = NewIntStat(SubsystemDatabaseKey, "num_doc_writes", StatUnitNoUnits, NumDocWritesDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumDocWritesRejected, err = NewIntStat(SubsystemDatabaseKey, "num_doc_writes_rejected", StatUnitNoUnits, NumDocWritesRejectedDesc, StatAddedVersion3dot3dot0, StatDeprecatedVersionNotDeprecated, StatStabilityInternal, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumReplicationsActive, err = NewIntStat(SubsystemDatabaseKey, "num_replications_active", StatUnitNoUnits, NumReplicationsActiveDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumReplicationsTotal, err = NewIntStat(SubsystemDatabaseKey, "num_replications_total", StatUnitNoUnits, NumReplicationsTotalDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumTombstonesCompacted, err = NewIntStat(SubsystemDatabaseKey, "num_tombstones_compacted", StatUnitNoUnits, NumTombstonesCompactedDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.PublicRestBytesRead, err = NewIntStat(SubsystemDatabaseKey, "public_rest_bytes_read", StatUnitBytes, PublicRestBytesReadDesc, StatAddedVersion3dot2dot0, StatDeprecatedVersionNotDeprecated, StatStabilityVolatile, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.SequenceAssignedCount, err = NewIntStat(SubsystemDatabaseKey, "sequence_assigned_count", StatUnitNoUnits, SequenceAssignedCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.LastSequenceAssignedValue, err = NewIntStat(SubsystemDatabaseKey, "last_sequence_assigned_value", StatUnitNoUnits, LastSequenceAssignedValueDesc, StatAddedVersion3dot2dot4, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.SequenceGetCount, err = NewIntStat(SubsystemDatabaseKey, "sequence_get_count", StatUnitNoUnits, SequenceGetCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.SequenceIncrCount, err = NewIntStat(SubsystemDatabaseKey, "sequence_incr_count", StatUnitNoUnits, SequenceIncrCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.SequenceReleasedCount, err = NewIntStat(SubsystemDatabaseKey, "sequence_released_count", StatUnitNoUnits, SequenceReleasedCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.SequenceReservedCount, err = NewIntStat(SubsystemDatabaseKey, "sequence_reserved_count", StatUnitNoUnits, SequenceReservedCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.LastSequenceReservedValue, err = NewIntStat(SubsystemDatabaseKey, "last_sequence_reserved_value", StatUnitNoUnits, LastSequenceReservedValueDesc, StatAddedVersion3dot2dot4, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.CorruptSequenceCount, err = NewIntStat(SubsystemDatabaseKey, "corrupt_sequence_count", StatUnitNoUnits, CorruptSequenceCountDesc, StatAddedVersion3dot2dot4, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.WarnChannelNameSizeCount, err = NewIntStat(SubsystemDatabaseKey, "warn_channel_name_size_count", StatUnitNoUnits, WarnChannelNameSizeCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.WarnChannelsPerDocCount, err = NewIntStat(SubsystemDatabaseKey, "warn_channels_per_doc_count", StatUnitNoUnits, WarnChannelsPerDocCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.WarnGrantsPerDocCount, err = NewIntStat(SubsystemDatabaseKey, "warn_grants_per_doc_count", StatUnitNoUnits, WarnGrantsPerDocCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.WarnXattrSizeCount, err = NewIntStat(SubsystemDatabaseKey, "warn_xattr_size_count", StatUnitNoUnits, WarnsXattrSizeCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.SyncFunctionCount, err = NewIntStat(SubsystemDatabaseKey, "sync_function_count", StatUnitNoUnits, SyncFunctionCountDesc, StatAddedVersion3dot1dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.SyncFunctionTime, err = NewIntStat(SubsystemDatabaseKey, "sync_function_time", StatUnitNanoseconds, SyncFunctionTimeDesc, StatAddedVersion3dot1dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.SyncFunctionExceptionCount, err = NewIntStat(SubsystemDatabaseKey, "sync_function_exception_count", StatUnitNoUnits, SyncFunctionExceptionCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumReplicationsRejectedLimit, err = NewIntStat(SubsystemDatabaseKey, "num_replications_rejected_limit", StatUnitNoUnits, NumReplicationsRejectedLimitDesc, StatAddedVersion3dot2dot0, StatDeprecatedVersionNotDeprecated, StatStabilityVolatile, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ResyncNumProcessed, err = NewIntStat(SubsystemDatabaseKey, "resync_num_processed", StatUnitNoUnits, ResyncNumProcessedDesc, StatAddedVersion3dot3dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ResyncNumChanged, err = NewIntStat(SubsystemDatabaseKey, "resync_num_changed", StatUnitNoUnits, ResyncNumChangedDesc, StatAddedVersion3dot3dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumPublicRestRequests, err = NewIntStat(SubsystemDatabaseKey, "num_public_rest_requests", StatUnitNoUnits, NumPublicRestRequestsDesc, StatAddedVersion3dot2dot0, StatDeprecatedVersionNotDeprecated, StatStabilityVolatile, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.TotalSyncTime, err = NewIntStat(SubsystemDatabaseKey, "total_sync_time", StatUnitSeconds, TotalSyncTimeDesc, StatAddedVersion3dot2dot0, StatDeprecatedVersionNotDeprecated, StatStabilityVolatile, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.TotalInitFatalErrors, err = NewIntStat(SubsystemDatabaseKey, "total_init_fatal_errors", StatUnitNoUnits, TotalInitFatalErrorsDesc, StatAddedVersion3dot2dot3, StatDeprecatedVersionNotDeprecated, StatStabilityVolatile, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.TotalOnlineFatalErrors, err = NewIntStat(SubsystemDatabaseKey, "total_online_fatal_errors", StatUnitNoUnits, TotalOnlineFatalErrorsDesc, StatAddedVersion3dot2dot3, StatDeprecatedVersionNotDeprecated, StatStabilityVolatile, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumPublicAllDocsRequests, err = NewIntStat(SubsystemDatabaseKey, "num_public_all_docs_requests", StatUnitNoUnits, NumPublicAllDocsRequestsDesc, StatAddedVersion3dot3dot0, StatDeprecatedVersionNotDeprecated, StatStabilityInternal, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumDocsPreFilterPublicAllDocs, err = NewIntStat(SubsystemDatabaseKey, "num_docs_pre_filter_public_all_docs", StatUnitNoUnits, NumDocsPreFilterPublicAllDocsDesc, StatAddedVersion3dot3dot0, StatDeprecatedVersionNotDeprecated, StatStabilityInternal, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.NumDocsPostFilterPublicAllDocs, err = NewIntStat(SubsystemDatabaseKey, "num_docs_post_filter_public_all_docs", StatUnitNoUnits, NumDocsPostFilterPublicAllDocsDesc, StatAddedVersion3dot3dot0, StatDeprecatedVersionNotDeprecated, StatStabilityInternal, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.ImportFeedMapStats = &ExpVarMapWrapper{new(expvar.Map).Init()}

	resUtil.CacheFeedMapStats = &ExpVarMapWrapper{new(expvar.Map).Init()}

	d.DatabaseStats = resUtil
	return nil
}

func (d *DbStats) unregisterDatabaseStats() {
	prometheus.Unregister(d.DatabaseStats.ReplicationBytesReceived)
	prometheus.Unregister(d.DatabaseStats.ReplicationBytesSent)
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
	prometheus.Unregister(d.DatabaseStats.NumDocWritesRejected)
	prometheus.Unregister(d.DatabaseStats.NumReplicationsActive)
	prometheus.Unregister(d.DatabaseStats.NumReplicationsTotal)
	prometheus.Unregister(d.DatabaseStats.NumTombstonesCompacted)
	prometheus.Unregister(d.DatabaseStats.PublicRestBytesWritten)
	prometheus.Unregister(d.DatabaseStats.SequenceAssignedCount)
	prometheus.Unregister(d.DatabaseStats.LastSequenceAssignedValue)
	prometheus.Unregister(d.DatabaseStats.SequenceGetCount)
	prometheus.Unregister(d.DatabaseStats.SequenceIncrCount)
	prometheus.Unregister(d.DatabaseStats.SequenceReleasedCount)
	prometheus.Unregister(d.DatabaseStats.SequenceReservedCount)
	prometheus.Unregister(d.DatabaseStats.LastSequenceReservedValue)
	prometheus.Unregister(d.DatabaseStats.CorruptSequenceCount)
	prometheus.Unregister(d.DatabaseStats.WarnChannelNameSizeCount)
	prometheus.Unregister(d.DatabaseStats.WarnChannelsPerDocCount)
	prometheus.Unregister(d.DatabaseStats.WarnGrantsPerDocCount)
	prometheus.Unregister(d.DatabaseStats.WarnXattrSizeCount)
	prometheus.Unregister(d.DatabaseStats.SyncFunctionCount)
	prometheus.Unregister(d.DatabaseStats.SyncFunctionTime)
	prometheus.Unregister(d.DatabaseStats.SyncFunctionExceptionCount)
	prometheus.Unregister(d.DatabaseStats.NumReplicationsRejectedLimit)
	prometheus.Unregister(d.DatabaseStats.ResyncNumProcessed)
	prometheus.Unregister(d.DatabaseStats.ResyncNumChanged)
	prometheus.Unregister(d.DatabaseStats.NumPublicRestRequests)
	prometheus.Unregister(d.DatabaseStats.TotalSyncTime)
	prometheus.Unregister(d.DatabaseStats.PublicRestBytesRead)
	prometheus.Unregister(d.DatabaseStats.TotalInitFatalErrors)
	prometheus.Unregister(d.DatabaseStats.TotalOnlineFatalErrors)
	prometheus.Unregister(d.DatabaseStats.NumPublicAllDocsRequests)
	prometheus.Unregister(d.DatabaseStats.NumDocsPreFilterPublicAllDocs)
	prometheus.Unregister(d.DatabaseStats.NumDocsPostFilterPublicAllDocs)
}

func (d *DbStats) CollectionStat(scopeName, collectionName string) (*CollectionStats, error) {
	scopeAndCollectionName := scopeName + "." + collectionName
	if _, ok := d.CollectionStats[scopeAndCollectionName]; !ok {
		// DbStats was not initialised with this collection upfront in NewDBStats for some reason - not something we'd expect to happen
		return nil, fmt.Errorf("stats for collection %q not found", scopeAndCollectionName)
	}
	return d.CollectionStats[scopeAndCollectionName], nil
}

func (d *DbStats) Database() *DatabaseStats {
	return d.DatabaseStats
}

func (d *DbStats) InitDeltaSyncStats() error {
	var err error
	resUtil := &DeltaSyncStats{}
	labelKeys := []string{DatabaseLabelKey}
	labelVals := []string{d.dbName}

	resUtil.DeltasRequested, err = NewIntStat(SubsystemDeltaSyncKey, "deltas_requested", StatUnitNoUnits, DeltasRequestedDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.DeltasSent, err = NewIntStat(SubsystemDeltaSyncKey, "deltas_sent", StatUnitNoUnits, DeltasSentDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.DeltaPullReplicationCount, err = NewIntStat(SubsystemDeltaSyncKey, "delta_pull_replication_count", StatUnitNoUnits, DeltaPullReplicationCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.DeltaCacheHit, err = NewIntStat(SubsystemDeltaSyncKey, "delta_cache_hit", StatUnitNoUnits, DeltaCacheHitDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.DeltaCacheMiss, err = NewIntStat(SubsystemDeltaSyncKey, "delta_sync_miss", StatUnitNoUnits, DeltaCacheMissDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}
	resUtil.DeltaPushDocCount, err = NewIntStat(SubsystemDeltaSyncKey, "delta_push_doc_count", StatUnitNoUnits, DeltaPushDocCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return err
	}

	d.DeltaSyncStats = resUtil
	return nil
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

func (d *DbStats) initSecurityStats() error {
	var err error
	resUtil := &SecurityStats{}
	if d.SecurityStats == nil {
		labelKeys := []string{DatabaseLabelKey}
		labelVals := []string{d.dbName}

		resUtil.AuthFailedCount, err = NewIntStat(SubsystemSecurity, "auth_failed_count", StatUnitNoUnits, AuthFailedCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
		if err != nil {
			return err
		}
		resUtil.AuthSuccessCount, err = NewIntStat(SubsystemSecurity, "auth_success_count", StatUnitNoUnits, AuthSuccessCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
		if err != nil {
			return err
		}
		resUtil.NumAccessErrors, err = NewIntStat(SubsystemSecurity, "num_access_errors", StatUnitNoUnits, NumAccessErrorsDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
		if err != nil {
			return err
		}
		resUtil.NumDocsRejected, err = NewIntStat(SubsystemSecurity, "num_docs_rejected", StatUnitNoUnits, NumDocsRejectedDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
		if err != nil {
			return err
		}
		resUtil.TotalAuthTime, err = NewIntStat(SubsystemSecurity, "total_auth_time", StatUnitNanoseconds, TotalAuthTimeDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
		if err != nil {
			return err
		}

		d.SecurityStats = resUtil
	}
	return nil
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
	prometheus.Unregister(d.DbReplicatorStats[replicationID].NumHandlersPanicked)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].ExpectedSequenceLen)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].ExpectedSequenceLenPostCleanup)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].ProcessedSequenceLen)
	prometheus.Unregister(d.DbReplicatorStats[replicationID].ProcessedSequenceLenPostCleanup)
}

func (d *DbStats) unregisterCollectionStats(scopeAndCollectionName string) {
	if d.CollectionStats[scopeAndCollectionName] == nil {
		return
	}

	prometheus.Unregister(d.CollectionStats[scopeAndCollectionName].SyncFunctionCount)
	prometheus.Unregister(d.CollectionStats[scopeAndCollectionName].SyncFunctionTime)
	prometheus.Unregister(d.CollectionStats[scopeAndCollectionName].SyncFunctionRejectCount)
	prometheus.Unregister(d.CollectionStats[scopeAndCollectionName].SyncFunctionRejectAccessCount)
	prometheus.Unregister(d.CollectionStats[scopeAndCollectionName].SyncFunctionExceptionCount)

	prometheus.Unregister(d.CollectionStats[scopeAndCollectionName].ImportCount)

	prometheus.Unregister(d.CollectionStats[scopeAndCollectionName].NumDocReads)
	prometheus.Unregister(d.CollectionStats[scopeAndCollectionName].DocReadsBytes)

	prometheus.Unregister(d.CollectionStats[scopeAndCollectionName].NumDocWrites)
	prometheus.Unregister(d.CollectionStats[scopeAndCollectionName].DocWritesBytes)
	prometheus.Unregister(d.CollectionStats[scopeAndCollectionName].ResyncNumProcessed)
	prometheus.Unregister(d.CollectionStats[scopeAndCollectionName].ResyncNumChanged)
}

func (d *DbStats) unregisterSecurityStats() {
	prometheus.Unregister(d.SecurityStats.AuthFailedCount)
	prometheus.Unregister(d.SecurityStats.AuthSuccessCount)
	prometheus.Unregister(d.SecurityStats.NumAccessErrors)
	prometheus.Unregister(d.SecurityStats.NumDocsRejected)
	prometheus.Unregister(d.SecurityStats.TotalAuthTime)
}

func NewCollectionStats(dbName, scopeAndCollectionName string) (stats *CollectionStats, err error) {
	labelKeys := []string{DatabaseLabelKey, CollectionLabelKey}
	labelVals := []string{dbName, scopeAndCollectionName}

	stats = &CollectionStats{}

	stats.SyncFunctionCount, err = NewIntStat(SubsystemCollection, "sync_function_count", StatUnitNoUnits, SyncFunctionCountCollDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	stats.SyncFunctionTime, err = NewIntStat(SubsystemCollection, "sync_function_time", StatUnitNanoseconds, SyncFunctionTimeCollDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	stats.SyncFunctionRejectCount, err = NewIntStat(SubsystemCollection, "sync_function_reject_count", StatUnitNoUnits, SyncFunctionRejectCountCollDesc, StatAddedVersion3dot1dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	stats.SyncFunctionRejectAccessCount, err = NewIntStat(SubsystemCollection, "sync_function_reject_access_count", StatUnitNoUnits, SyncFunctionRejectAccessCountCollDesc, StatAddedVersion3dot1dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	stats.SyncFunctionExceptionCount, err = NewIntStat(SubsystemCollection, "sync_function_exception_count", StatUnitNoUnits, SyncFunctionExceptionCountCollDesc, StatAddedVersion3dot1dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}

	stats.ImportCount, err = NewIntStat(SubsystemCollection, "import_count", StatUnitNoUnits, ImportCountCollDesc, StatAddedVersion3dot1dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}

	stats.NumDocReads, err = NewIntStat(SubsystemCollection, "num_doc_reads", StatUnitNoUnits, NumDocReadsCollDesc, StatAddedVersion3dot1dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	stats.DocReadsBytes, err = NewIntStat(SubsystemCollection, "doc_reads_bytes", StatUnitBytes, DocReadsBytesCollDesc, StatAddedVersion3dot1dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}

	stats.NumDocWrites, err = NewIntStat(SubsystemCollection, "num_doc_writes", StatUnitNoUnits, NumDocWritesCollDesc, StatAddedVersion3dot1dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	stats.DocWritesBytes, err = NewIntStat(SubsystemCollection, "doc_writes_bytes", StatUnitBytes, DocWritesBytesCollDesc, StatAddedVersion3dot1dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	stats.ResyncNumProcessed, err = NewIntStat(SubsystemCollection, "resync_num_processed", StatUnitBytes, ResyncNumProcessedCollDesc, StatAddedVersion3dot3dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	stats.ResyncNumChanged, err = NewIntStat(SubsystemCollection, "resync_num_changed", StatUnitBytes, ResyncNumChangedCollDesc, StatAddedVersion3dot3dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}

	return stats, nil
}

func (d *DbStats) InitCollectionStats(scopeAndCollectionNames ...string) error {
	if d.CollectionStats == nil || len(scopeAndCollectionNames) == 0 {
		d.CollectionStats = make(map[string]*CollectionStats, len(scopeAndCollectionNames))
	}

	for _, scopeAndCollectionName := range scopeAndCollectionNames {
		if _, ok := d.CollectionStats[scopeAndCollectionName]; !ok {
			stats, err := NewCollectionStats(d.dbName, scopeAndCollectionName)
			if err != nil {
				return err
			}
			d.CollectionStats[scopeAndCollectionName] = stats
		}
	}

	return nil
}

func (d *DbStats) DBReplicatorStats(replicationID string) (*DbReplicatorStats, error) {
	d.dbReplicatorStatsMutex.Lock()
	defer d.dbReplicatorStatsMutex.Unlock()

	if _, ok := d.DbReplicatorStats[replicationID]; ok {
		return d.DbReplicatorStats[replicationID], nil
	}
	var err error
	resUtil := &DbReplicatorStats{}
	labelKeys := []string{DatabaseLabelKey, ReplicationLabelKey}
	labelVals := []string{d.dbName, replicationID}

	resUtil.NumAttachmentBytesPushed, err = NewIntStat(SubsystemReplication, "sgr_num_attachment_bytes_pushed", StatUnitBytes, SGRNumAttachmentBytesPushedDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.NumAttachmentPushed, err = NewIntStat(SubsystemReplication, "sgr_num_attachments_pushed", StatUnitNoUnits, SGRNumAttachmentsPushedDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.NumDocPushed, err = NewIntStat(SubsystemReplication, "sgr_num_docs_pushed", StatUnitNoUnits, SGRNumDocsPushedDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.NumDocsFailedToPush, err = NewIntStat(SubsystemReplication, "sgr_num_docs_failed_to_push", StatUnitNoUnits, SGRNumDocsFailedToPushDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.PushConflictCount, err = NewIntStat(SubsystemReplication, "sgr_push_conflict_count", StatUnitNoUnits, SGRPushConflictCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.PushRejectedCount, err = NewIntStat(SubsystemReplication, "sgr_push_rejected_count", StatUnitNoUnits, SGRPushRejectedCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.PushDeltaSentCount, err = NewIntStat(SubsystemReplication, "sgr_deltas_sent", StatUnitNoUnits, SGRDeltasSentDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.DocsCheckedSent, err = NewIntStat(SubsystemReplication, "sgr_docs_checked_sent", StatUnitNoUnits, SGRDocsCheckedSentDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.NumConnectAttemptsPush, err = NewIntStat(SubsystemReplication, "sgr_num_connect_attempts_push", StatUnitNoUnits, SGRNumConnectAttemptsPushDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.NumReconnectsAbortedPush, err = NewIntStat(SubsystemReplication, "sgr_num_reconnects_aborted_push", StatUnitNoUnits, SGRNumReconnectsAbortedPushDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.NumAttachmentBytesPulled, err = NewIntStat(SubsystemReplication, "sgr_num_attachment_bytes_pulled", StatUnitBytes, SGRNumAttachmentBytesPulledDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.NumAttachmentsPulled, err = NewIntStat(SubsystemReplication, "sgr_num_attachments_pulled", StatUnitNoUnits, SGRNumAttachmentsPulledDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.PulledCount, err = NewIntStat(SubsystemReplication, "sgr_num_docs_pulled", StatUnitNoUnits, SGRNumDocsPulledDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.PurgedCount, err = NewIntStat(SubsystemReplication, "sgr_num_docs_purged", StatUnitNoUnits, SGRNumDocsPurgedDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.FailedToPullCount, err = NewIntStat(SubsystemReplication, "sgr_num_docs_failed_to_pull", StatUnitNoUnits, SGRNumDocsFailedToPullDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.DeltaReceivedCount, err = NewIntStat(SubsystemReplication, "sgr_deltas_recv", StatUnitNoUnits, SGRDeltasRecvDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.DeltaRequestedCount, err = NewIntStat(SubsystemReplication, "sgr_deltas_requested", StatUnitNoUnits, SGRDeltasRequestedDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.DocsCheckedReceived, err = NewIntStat(SubsystemReplication, "sgr_docs_checked_recv", StatUnitNoUnits, SGRDocsCheckedRecvDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.ConflictResolvedLocalCount, err = NewIntStat(SubsystemReplication, "sgr_conflict_resolved_local_count", StatUnitNoUnits, SGRConflictResolvedLocalCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.ConflictResolvedRemoteCount, err = NewIntStat(SubsystemReplication, "sgr_conflict_resolved_remote_count", StatUnitNoUnits, SGRConflictResolvedRemoteCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.ConflictResolvedMergedCount, err = NewIntStat(SubsystemReplication, "sgr_conflict_resolved_merge_count", StatUnitNoUnits, SGRConflictResolvedMergeCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.NumConnectAttemptsPull, err = NewIntStat(SubsystemReplication, "sgr_num_connect_attempts_pull", StatUnitNoUnits, SGRNumConnectAttemptsPullDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.NumReconnectsAbortedPull, err = NewIntStat(SubsystemReplication, "sgr_num_reconnects_aborted_pull", StatUnitNoUnits, SGRNumReconnectsAbortedPullDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.NumHandlersPanicked, err = NewIntStat(SubsystemReplication, "sgr_num_handlers_panicked", StatUnitNoUnits, SGRNumHandlersPanickedDesc, StatAddedVersion3dot1dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.ExpectedSequenceLen, err = NewIntStat(SubsystemReplication, "expected_sequence_len", StatUnitNoUnits, SGRExpectedSequenceLengthDesc, StatAddedVersion3dot1dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.ExpectedSequenceLenPostCleanup, err = NewIntStat(SubsystemReplication, "expected_sequence_len_post_cleanup", StatUnitNoUnits, SGRExpectedSequenceLengthPostCleanupDesc, StatAddedVersion3dot1dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.ProcessedSequenceLen, err = NewIntStat(SubsystemReplication, "processed_sequence_len", StatUnitNoUnits, SGRProcessedSequenceLength, StatAddedVersion3dot1dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}
	resUtil.ProcessedSequenceLenPostCleanup, err = NewIntStat(SubsystemReplication, "processed_sequence_len_post_cleanup", StatUnitNoUnits, SGRProcessedSequenceLengthPostCleanupDesc, StatAddedVersion3dot1dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
	if err != nil {
		return nil, err
	}

	d.DbReplicatorStats[replicationID] = resUtil

	return d.DbReplicatorStats[replicationID], nil
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
	dbr.ExpectedSequenceLen.Set(0)
	dbr.ExpectedSequenceLenPostCleanup.Set(0)
	dbr.ProcessedSequenceLen.Set(0)
	dbr.ProcessedSequenceLenPostCleanup.Set(0)

}

func (d *DbStats) Security() *SecurityStats {
	return d.SecurityStats
}

func (d *DbStats) InitSharedBucketImportStats() error {
	var err error
	resUtil := &SharedBucketImportStats{}
	if d.SharedBucketImportStats == nil {
		labelKeys := []string{DatabaseLabelKey}
		labelVals := []string{d.dbName}

		resUtil.ImportCount, err = NewIntStat(SubsystemSharedBucketImport, "import_count", StatUnitNoUnits, ImportCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
		if err != nil {
			return err
		}
		resUtil.ImportCancelCAS, err = NewIntStat(SubsystemSharedBucketImport, "import_cancel_cas", StatUnitNoUnits, ImportCancelCASDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
		if err != nil {
			return err
		}
		resUtil.ImportErrorCount, err = NewIntStat(SubsystemSharedBucketImport, "import_error_count", StatUnitNoUnits, ImportErrorCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
		if err != nil {
			return err
		}
		resUtil.ImportProcessingTime, err = NewIntStat(SubsystemSharedBucketImport, "import_processing_time", StatUnitNanoseconds, ImportProcessingTimeDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
		if err != nil {
			return err
		}
		resUtil.ImportHighSeq, err = NewIntStat(SubsystemSharedBucketImport, "import_high_seq", StatUnitNoUnits, ImportHighSeqDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
		if err != nil {
			return err
		}
		resUtil.ImportPartitions, err = NewIntStat(SubsystemSharedBucketImport, "import_partitions", StatUnitNoUnits, ImportPartitionsDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.GaugeValue, 0)
		if err != nil {
			return err
		}
		resUtil.ImportFeedProcessedCount, err = NewIntStat(SubsystemSharedBucketImport, "import_processed_count", StatUnitNoUnits, ImportFeedProcessedCountDesc, StatAddedVersion3dot3dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
		if err != nil {
			return err
		}
		d.SharedBucketImportStats = resUtil
	}
	return nil
}

func (d *DbStats) unregisterSharedBucketImportStats() {
	prometheus.Unregister(d.SharedBucketImportStats.ImportCount)
	prometheus.Unregister(d.SharedBucketImportStats.ImportCancelCAS)
	prometheus.Unregister(d.SharedBucketImportStats.ImportErrorCount)
	prometheus.Unregister(d.SharedBucketImportStats.ImportProcessingTime)
	prometheus.Unregister(d.SharedBucketImportStats.ImportHighSeq)
	prometheus.Unregister(d.SharedBucketImportStats.ImportPartitions)
	prometheus.Unregister(d.SharedBucketImportStats.ImportFeedProcessedCount)
}

func (d *DbStats) SharedBucketImport() *SharedBucketImportStats {
	return d.SharedBucketImportStats
}

func (d *DbStats) InitQueryStats(useViews bool, queryNames ...string) error {
	d.QueryStats = &QueryStats{
		Stats: map[string]*QueryStat{},
	}
	d.QueryStats.mutex.Lock()
	for _, queryName := range queryNames {
		err := d._initQueryStat(useViews, queryName)
		if err != nil {
			return err
		}
	}
	d.QueryStats.mutex.Unlock()
	return nil
}

func (d *DbStats) _initQueryStat(useViews bool, queryName string) error {
	var err error
	resUtil := &QueryStat{}
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
		resUtil.QueryCount, err = NewIntStat(SubsystemGSIViews, prometheusKey+"_count", StatUnitNoUnits, QueryNameCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
		if err != nil {
			return err
		}
		resUtil.QueryErrorCount, err = NewIntStat(SubsystemGSIViews, prometheusKey+"_error_count", StatUnitNoUnits, QueryNameErrorCountDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
		if err != nil {
			return err
		}
		resUtil.QueryTime, err = NewIntStat(SubsystemGSIViews, prometheusKey+"_time", StatUnitNanoseconds, QueryNameTimeDesc, StatAddedVersion3dot0dot0, StatDeprecatedVersionNotDeprecated, StatStabilityCommitted, labelKeys, labelVals, prometheus.CounterValue, 0)
		if err != nil {
			return err
		}

		d.QueryStats.Stats[queryName] = resUtil
	}
	return nil
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
