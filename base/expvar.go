//  Copyright (c) 2017 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"expvar"
	"fmt"
	"strconv"
	"sync"
	"time"
)

const (
	exportDebugExpvars = true
)

var TimingExpvarsEnabled = false

var (

	// Top level stats expvar map
	Stats *expvar.Map

	// Global Stats
	GlobalStats *expvar.Map

	// Per-database stats
	PerDbStats *expvar.Map

	// Per-replication (sg-replicate) stats
	PerReplicationStats *expvar.Map
)

const (
	PerDb          = "per_db"
	PerReplication = "per_replication"
	Global         = "global"
)

const (

	// StatsResourceUtilization
	StatKeyProcessCpuPercentUtilization   = "process_cpu_percent_utilization"
	StatKeyProcessMemoryResident          = "process_memory_resident"
	StatKeySystemMemoryTotal              = "system_memory_total"
	StatKeyPubNetworkInterfaceBytesSent   = "pub_net_bytes_sent"
	StatKeyPubNetworkInterfaceBytesRecv   = "pub_net_bytes_recv"
	StatKeyAdminNetworkInterfaceBytesSent = "admin_net_bytes_sent"
	StatKeyAdminNetworkInterfaceBytesRecv = "admin_net_bytes_recv"
	StatKeyNumGoroutines                  = "num_goroutines"
	StatKeyGoroutinesHighWatermark        = "goroutines_high_watermark"
	StatKeyGoMemstatsSys                  = "go_memstats_sys"
	StatKeyGoMemstatsHeapAlloc            = "go_memstats_heapalloc"
	StatKeyGoMemstatsHeapIdle             = "go_memstats_heapidle"
	StatKeyGoMemstatsHeapInUse            = "go_memstats_heapinuse"
	StatKeyGoMemstatsHeapReleased         = "go_memstats_heapreleased"
	StatKeyGoMemstatsStackInUse           = "go_memstats_stackinuse"
	StatKeyGoMemstatsStackSys             = "go_memstats_stacksys"
	StatKeyGoMemstatsPauseTotalNs         = "go_memstats_pausetotalns"
	StatKeyErrorCount                     = "error_count"
	StatKeyWarnCount                      = "warn_count"

	// StatsCache
	StatKeyRevisionCacheHits         = "rev_cache_hits"
	StatKeyRevisionCacheMisses       = "rev_cache_misses"
	StatKeyChannelCacheHits          = "chan_cache_hits"
	StatKeyChannelCacheMisses        = "chan_cache_misses"
	StatKeyChannelCacheRevsActive    = "chan_cache_active_revs"
	StatKeyChannelCacheRevsTombstone = "chan_cache_tombstone_revs"
	StatKeyChannelCacheRevsRemoval   = "chan_cache_removal_revs"
	StatKeyChannelCacheNumChannels   = "chan_cache_num_channels"
	StatKeyChannelCacheMaxEntries    = "chan_cache_max_entries"
	StatKeyNumSkippedSeqs            = "num_skipped_seqs"
	StatKeyAbandonedSeqs             = "abandoned_seqs"

	// StatsDatabase
	StatKeySequenceGets          = "sequence_gets"
	StatKeySequenceReserves      = "sequence_reserves"
	StatKeyCrc32cMatchCount      = "crc32c_match_count"
	StatKeyNumReplicationsActive = "num_replications_active"
	StatKeyNumReplicationsTotal  = "num_replications_total"
	StatKeyNumDocWrites          = "num_doc_writes"
	StatKeyDocWritesBytes        = "doc_writes_bytes"
	StatKeyNumDocReadsRest       = "num_doc_reads_rest"
	StatKeyNumDocReadsBlip       = "num_doc_reads_blip"
	StatKeyDocReadsBytesBlip     = "doc_reads_bytes_blip"

	// StatsDeltaSync
	StatKeyNetBandwidthSavings       = "net_bandwidth_savings"
	StatKeyDeltasRequested           = "deltas_requested"
	StatKeyDeltasSent                = "deltas_sent"
	StatKeyDeltaPullReplicationCount = "delta_pull_replication_count"
	StatKeyDeltaCacheHits            = "delta_cache_hit"
	StatKeyDeltaCacheMisses          = "delta_cache_miss"
	StatKeyDeltaPushDocCount         = "delta_push_doc_count"

	// StatsSharedBucketImport
	StatKeyImportBacklog    = "import_backlog"
	StatKeyImportCount      = "import_count"
	StatKeyImportErrorCount = "import_error_count"

	// StatsCBLReplicationPush
	StatKeyDocPushCount        = "doc_push_count"
	StatKeyWriteProcessingTime = "write_processing_time"
	StatKeySyncFunctionTime    = "sync_function_time"
	StatKeySyncFunctionCount   = "sync_function_count"
	StatKeyProposeChangeTime   = "propose_change_time"
	StatKeyProposeChangeCount  = "propose_change_count"
	StatKeyAttachmentPushCount = "attachment_push_count"
	StatKeyAttachmentPushBytes = "attachment_push_bytes"
	StatKeyConflictWriteCount  = "conflict_write_count"

	// StatsCBLReplicationPull
	StatKeyPullReplicationsActiveOneShot    = "num_pull_repl_active_one_shot"
	StatKeyPullReplicationsActiveContinuous = "num_pull_repl_active_continuous"
	StatKeyPullReplicationsTotalOneShot     = "num_pull_repl_total_one_shot"
	StatKeyPullReplicationsTotalContinuous  = "num_pull_repl_total_continuous"
	StatKeyPullReplicationsSinceZero        = "num_pull_repl_since_zero"
	StatKeyPullReplicationsCaughtUp         = "num_pull_repl_caught_up"
	StatKeyRequestChangesCount              = "request_changes_count"
	StatKeyRequestChangesTime               = "request_changes_time"
	StatKeyDcpCachingCount                  = "dcp_caching_count"
	StatKeyDcpCachingTime                   = "dcp_caching_time"
	StatKeyRevSendCount                     = "rev_send_count"
	StatKeyRevSendTime                      = "rev_send_time"
	StatKeyMaxPending                       = "max_pending"
	StatKeyAttachmentPullCount              = "attachment_pull_count"
	StatKeyAttachmentPullBytes              = "attachment_pull_bytes"

	// StatsSecurity
	StatKeyNumDocsRejected  = "num_docs_rejected"
	StatKeyNumAccessErrors  = "num_access_errors"
	StatKeyAuthSuccessCount = "auth_success_count"
	StatKeyAuthFailedCount  = "auth_failed_count"
	StatKeyTotalAuthTime    = "total_auth_time"

	// StatsGsiViews
	StatKeyTotalQueriesPerSec      = "total_queries_per_sec"
	StatKeyChannelQueriesPerSec    = "channel_queries_per_sec"
	StatKeyRoleAccessQueriesPerSec = "role_access_queries_per_sec"
	StatKeyQueryProcessingTime     = "query_processing_time"

	// StatsReplication
	StatKeySgrNumDocsPushed              = "sgr_num_docs_pushed"
	StatKeySgrNumDocsFailedToPush        = "sgr_num_docs_failed_to_push"
	StatKeySgrNumAttachmentsTransferred  = "sgr_num_attachments_transferred"
	StatKeySgrAttachmentBytesTransferred = "sgr_num_attachment_bytes_transferred"
	StatKeySgrDocsCheckedSent            = "sgr_docs_checked_sent"
)

const (
	StatsGroupKeySyncGateway         = "syncgateway"
	StatsGroupKeyResourceUtilization = "resource_utilization"
	StatsGroupKeyCache               = "cache"
	StatsGroupKeyDatabase            = "database"
	StatsGroupKeyDeltaSync           = "delta_sync"
	StatsGroupKeySharedBucketImport  = "shared_bucket_import"
	StatsGroupKeyCblReplicationPush  = "cbl_replication_push"
	StatsGroupKeyCblReplicationPull  = "cbl_replication_pull"
	StatsGroupKeySecurity            = "security"
	StatsGroupKeyGsiViews            = "gsi_views"
)

func init() {

	// Create the expvars structure:
	//
	// {
	//    "syncgateway": {
	//      "global": {..}
	//      "per_db": {
	//         "db1": {..}
	//      }
	//      "per_replication": {
	//         "repl1": {..}
	//      }
	// }

	// All stats will be stored in expvars under the "syncgateway" key.
	Stats = expvar.NewMap(StatsGroupKeySyncGateway)

	GlobalStats = new(expvar.Map).Init()
	Stats.Set(Global, GlobalStats)

	PerDbStats = new(expvar.Map).Init()
	Stats.Set(PerDb, PerDbStats)

	PerReplicationStats = new(expvar.Map).Init()
	Stats.Set(PerReplication, PerReplicationStats)

	// Add StatsResourceUtilization under GlobalStats
	GlobalStats.Set(StatsGroupKeyResourceUtilization, NewStatsResourceUtilization())

}

func StatsResourceUtilization() *expvar.Map {
	statsResourceUtilizationVar := GlobalStats.Get(StatsGroupKeyResourceUtilization)
	statsResourceUtilization := statsResourceUtilizationVar.(*expvar.Map)
	return statsResourceUtilization
}

func NewStatsResourceUtilization() *expvar.Map {
	stats := new(expvar.Map).Init()
	stats.Set(StatKeyProcessCpuPercentUtilization, ExpvarFloatVal(0))
	stats.Set(StatKeyProcessMemoryResident, ExpvarIntVal(0))
	stats.Set(StatKeySystemMemoryTotal, ExpvarIntVal(0))
	stats.Set(StatKeyPubNetworkInterfaceBytesSent, ExpvarIntVal(0))
	stats.Set(StatKeyPubNetworkInterfaceBytesRecv, ExpvarIntVal(0))
	stats.Set(StatKeyAdminNetworkInterfaceBytesSent, ExpvarIntVal(0))
	stats.Set(StatKeyAdminNetworkInterfaceBytesRecv, ExpvarIntVal(0))
	stats.Set(StatKeyNumGoroutines, ExpvarIntVal(0))
	stats.Set(StatKeyGoroutinesHighWatermark, ExpvarIntVal(0))
	stats.Set(StatKeyGoMemstatsSys, ExpvarIntVal(0))
	stats.Set(StatKeyGoMemstatsHeapAlloc, ExpvarIntVal(0))
	stats.Set(StatKeyGoMemstatsHeapIdle, ExpvarIntVal(0))
	stats.Set(StatKeyGoMemstatsHeapInUse, ExpvarIntVal(0))
	stats.Set(StatKeyGoMemstatsHeapReleased, ExpvarIntVal(0))
	stats.Set(StatKeyGoMemstatsStackInUse, ExpvarIntVal(0))
	stats.Set(StatKeyGoMemstatsStackSys, ExpvarIntVal(0))
	stats.Set(StatKeyGoMemstatsPauseTotalNs, ExpvarIntVal(0))
	stats.Set(StatKeyErrorCount, ExpvarIntVal(0))
	stats.Set(StatKeyWarnCount, ExpvarIntVal(0))
	return stats
}

// Removes the per-replication stats for this replication id by
// regenerating a new expvar map without that particular replicationUuid
func RemovePerReplicationStats(replicationUuid string) {

	// Clear out the stats for this replication since they will no longer be updated.
	PerReplicationStats.Set(replicationUuid, new(expvar.Map).Init())

}

// Removes the per-database stats for this database by
// regenerating a new expvar map without that particular dbname
func RemovePerDbStats(dbName string) {

	// Clear out the stats for this db since they will no longer be updated.
	PerDbStats.Set(dbName, new(expvar.Map).Init())

}

// SequenceTimingExpvarMap attempts to track timing information for targeted sequences as they move through the system.
// Creates a map that looks like the following, where Indexed, Polled, Changes are the incoming stages, the values are
// nanosecond timestamps, and the sequences are the target sequences, based on the specified vb and frequency (in the example
// frequency=1000).  Since we won't necessarily see every vb sequence, we track the first sequence we see higher than the
// target frequency.  (e.g. if our last sequence was 1000 and frequency is 1000, it will track the first sequence seen higher than
// 2000).
// Note: Frequency needs to be high enough that a sequence can move through the system before the next sequence is seen, otherwise
// earlier stages could be updating current before the later stages have processed it.
/*
{
	"timingMap": {
		"seq1000.Indexed" :  4738432432,
		"seq1000.Polled" : 5743785947,
		"seq1000.Changes" :
		"seq2002.Indexed" :  4738432432,
		"seq2002.Polled" : 5743785947,
		"seq2002.Changes" :
	}
}
*/
type SequenceTimingExpvar struct {
	frequency        uint64
	currentTargetSeq uint64
	currentActualSeq uint64
	nextTargetSeq    uint64
	vbNo             uint16
	lock             sync.RWMutex
	timingMap        *expvar.Map
}

func NewSequenceTimingExpvar(frequency uint64, targetVbNo uint16, name string) SequenceTimingExpvar {

	storageMap := expvar.Map{}
	storageMap.Init()

	return SequenceTimingExpvar{
		currentTargetSeq: 0,
		nextTargetSeq:    0,
		frequency:        frequency,
		vbNo:             targetVbNo,
		timingMap:        &storageMap,
	}
}

type TimingStatus int

const (
	TimingStatusCurrent TimingStatus = iota
	TimingStatusNext
	TimingStatusNone
	TimingStatusInit
)

func (s SequenceTimingExpvar) String() string {
	return s.timingMap.String()
}

func (s *SequenceTimingExpvar) UpdateBySequence(stage string, vbNo uint16, seq uint64) {

	if !TimingExpvarsEnabled {
		return
	}
	timingStatus := s.isCurrentOrNext(vbNo, seq)
	switch timingStatus {
	case TimingStatusNone:
		return
	case TimingStatusInit:
		s.initTiming(seq)
	case TimingStatusCurrent:
		s.setActual(seq)
		s.writeCurrentSeq(stage, time.Now())
	case TimingStatusNext:
		s.updateNext(stage, seq, time.Now())
	}
	return
}

func (s *SequenceTimingExpvar) UpdateBySequenceAt(stage string, vbNo uint16, seq uint64, time time.Time) {

	if !TimingExpvarsEnabled {
		return
	}
	timingStatus := s.isCurrentOrNext(vbNo, seq)
	switch timingStatus {
	case TimingStatusNone:
		return
	case TimingStatusInit:
		s.initTiming(seq)
	case TimingStatusCurrent:
		s.setActual(seq)
		s.writeCurrentSeq(stage, time)
	case TimingStatusNext:
		s.updateNext(stage, seq, time)
	}
	return
}

// Update by sequence range is used for events (like clock polling) that don't see
// every sequence.  Writes when current target sequence is in range.  Assumes callers
// don't report overlapping ranges
func (s *SequenceTimingExpvar) UpdateBySequenceRange(stage string, vbNo uint16, startSeq uint64, endSeq uint64) {

	if !TimingExpvarsEnabled {
		return
	}
	timingStatus := s.isCurrentOrNextRange(vbNo, startSeq, endSeq)
	switch timingStatus {
	case TimingStatusNone:
		return
	case TimingStatusInit:
		s.initTiming(endSeq)
	case TimingStatusCurrent:
		s.writeCurrentRange(stage)
	case TimingStatusNext:
		s.updateNextRange(stage, startSeq, endSeq)
	}
}

// Initializes based on the first sequence seen
func (s *SequenceTimingExpvar) initTiming(startSeq uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.nextTargetSeq == 0 {
		s.nextTargetSeq = ((startSeq / s.frequency) + 1) * s.frequency
	}
}

func (s *SequenceTimingExpvar) setActual(seq uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.currentActualSeq == 0 || s.currentActualSeq < s.currentTargetSeq {
		s.currentActualSeq = seq
	}
}

func (s *SequenceTimingExpvar) writeCurrentSeq(stage string, time time.Time) {

	key := fmt.Sprintf("seq%d:%s", s.currentTargetSeq, stage)
	value := expvar.Int{}
	value.Set(time.UnixNano())
	s.timingMap.Set(key, &value)
}

func (s *SequenceTimingExpvar) writeCurrentRange(stage string) {

	key := fmt.Sprintf("seq%d:%s", s.currentTargetSeq, stage)
	value := expvar.Int{}
	value.Set(time.Now().UnixNano())
	s.timingMap.Set(key, &value)
}

func (s *SequenceTimingExpvar) updateNext(stage string, seq uint64, time time.Time) {

	s.currentTargetSeq = s.nextTargetSeq
	s.currentActualSeq = seq
	s.nextTargetSeq = s.currentTargetSeq + s.frequency
	s.writeCurrentSeq(stage, time)
}

// UpdateNextRange updates the target values, but not actual
func (s *SequenceTimingExpvar) updateNextRange(stage string, fromSeq, toSeq uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.currentTargetSeq = s.nextTargetSeq
	s.nextTargetSeq = s.currentTargetSeq + s.frequency

	s.writeCurrentRange(stage)
}

func (s *SequenceTimingExpvar) isCurrentOrNextRange(vbNo uint16, startSeq uint64, endSeq uint64) TimingStatus {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if vbNo != s.vbNo {
		return TimingStatusNone
	}

	if s.nextTargetSeq == 0 {
		return TimingStatusInit
	}

	if startSeq <= s.nextTargetSeq && endSeq >= s.nextTargetSeq {
		return TimingStatusNext
	}

	if s.currentTargetSeq > 0 && startSeq <= s.currentTargetSeq && endSeq >= s.currentTargetSeq {
		return TimingStatusCurrent
	}

	return TimingStatusNone
}

func (s *SequenceTimingExpvar) isCurrentOrNext(vbNo uint16, seq uint64) TimingStatus {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if vbNo != s.vbNo {
		return TimingStatusNone
	}

	if s.nextTargetSeq == 0 {
		return TimingStatusInit
	}

	if seq > 0 {
		if seq > s.nextTargetSeq {
			return TimingStatusNext
		}
		// If matches actual
		if seq == s.currentActualSeq {
			return TimingStatusCurrent
		}
		// If actual hasn't been set yet
		if s.currentActualSeq < s.currentTargetSeq && seq >= s.currentTargetSeq {
			return TimingStatusCurrent
		}
	}

	return TimingStatusNone
}

// IntMax is an expvar.Value that tracks the maximum value it's given.
type IntMax struct {
	i  int64
	mu sync.RWMutex
}

func (v *IntMax) String() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return strconv.FormatInt(v.i, 10)
}

func (v *IntMax) SetIfMax(value int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if value > v.i {
		v.i = value
	}
}

func SetIfMax(expvarMap *expvar.Map, key string, val int64) {
	if expvarMap == nil {
		return
	}
	mapVar := expvarMap.Get(key)
	if intMaxVar, ok := mapVar.(*IntMax); ok {
		intMaxVar.SetIfMax(val)
	}
}

// IntMean is an expvar.Value that returns the mean of all values that
// are sent via AddValue or AddSince.
type IntMeanVar struct {
	count int64 // number of values seen
	mean  int64 // average value
	mu    sync.RWMutex
}

func (v *IntMeanVar) String() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return strconv.FormatInt(v.mean, 10)
}

// Adds value.  Calculates new mean as iterative mean (avoids int overflow)
func (v *IntMeanVar) AddValue(value int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.count++
	v.mean = v.mean + int64((value-v.mean)/v.count)
}

func (v *IntMeanVar) AddSince(start time.Time) {
	v.AddValue(time.Since(start).Nanoseconds())
}

type DebugIntMeanVar struct {
	v IntMeanVar
}

func (d *DebugIntMeanVar) String() string {
	if exportDebugExpvars {
		return d.v.String()
	}
	return ""
}

func (d *DebugIntMeanVar) AddValue(value int64) {
	if exportDebugExpvars {
		d.v.AddValue(value)
	}
}

func (d *DebugIntMeanVar) AddSince(start time.Time) {
	if exportDebugExpvars {
		d.v.AddSince(start)
	}
}

// IntRollingMean is an expvar.Value that returns the mean of the [size] latest
// values sent via AddValue.  Uses a slice to track values, so setting a large
// size has memory implications
type IntRollingMeanVar struct {
	mean     float64 // average value
	mu       sync.RWMutex
	entries  []int64
	capacity int
	position int
}

func NewIntRollingMeanVar(capacity int) IntRollingMeanVar {
	return IntRollingMeanVar{
		capacity: capacity,
		entries:  make([]int64, 0, capacity),
	}
}
func (v *IntRollingMeanVar) String() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return strconv.FormatInt(int64(v.mean), 10)
}

// Adds value
func (v *IntRollingMeanVar) AddValue(value int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.entries) < v.capacity {
		v.addValue(value)
	} else {
		v.replaceValue(value)
	}
}
func (v *IntRollingMeanVar) AddSince(start time.Time) {
	v.AddValue(time.Since(start).Nanoseconds())
}

func (v *IntRollingMeanVar) AddSincePerItem(start time.Time, numItems int) {

	// avoid divide by zero errors
	if numItems == 0 {
		numItems = 1
	}

	// calculate per-item time delta
	timeDelta := time.Since(start).Nanoseconds()
	timeDeltaPerItem := timeDelta / int64(numItems)

	v.AddValue(timeDeltaPerItem)

}

// If we have fewer entries than capacity, regular mean calculation
func (v *IntRollingMeanVar) addValue(value int64) {
	v.entries = append(v.entries, value)
	v.mean = v.mean + (float64(value)-v.mean)/float64(len(v.entries))
}

// If we have filled the ring buffer, replace value at position and recalculate mean
func (v *IntRollingMeanVar) replaceValue(value int64) {
	oldValue := v.entries[v.position]
	v.entries[v.position] = value
	v.mean = v.mean + float64(value-oldValue)/float64(v.capacity)
	v.position++
	if v.position > v.capacity-1 {
		v.position = 0
	}
}
