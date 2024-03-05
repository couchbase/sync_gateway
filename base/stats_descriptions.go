// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

const SGWUpDesc = "This provides a stat for sgw_up where the value will be fixed to one."

// ResourceUtilization descriptions
const (
	AdminNetBytesRecDesc = "The total number of bytes received (since node start-up) on the network interface to which the Sync Gateway api.admin_interface is bound. " +
		"By default, that is the number of bytes received on 127.0.0.1:4985 since node start-up."

	AdminNetBytesSentDesc = "The total number of bytes sent (since node start-up) on the network interface to which the Sync Gateway api.admin_interface is bound." +
		"By default, that is the number of bytes sent on 127.0.0.1:4985 since node start-up."

	ErrorCountDesc = "The total number of errors logged."

	GoMemHeapAllocDesc = "HeapAlloc is bytes of allocated heap objects. \"Allocated\" heap objects include all reachable objects, as well as unreachable objects that the garbage collector has " +
		"not yet freed. Specifically, HeapAlloc increases as heap objects are allocated and decreases as the heap is swept and unreachable objects are freed. Sweeping occurs " +
		"incrementally between GC cycles, so these two processes occur simultaneously, and as a result HeapAlloc tends to change smoothly (in contrast with the sawtooth that is" +
		"typical of stop-the-world garbage collectors)."

	GoMemHeapIdleDesc = "HeapIdle is bytes in idle (unused) spans. Idle spans have no objects in them. These spans could be (and may already have been) returned to the OS, or they can" +
		" be reused for heap allocations, or they can be reused as stack memory. HeapIdle minus HeapReleased estimates the amount of memory that could be returned to the OS, but is being retained by " +
		"the runtime so it can grow the heap without requesting more memory from the OS. If this difference is significantly larger than the heap size, it indicates there was a recent " +
		"transient spike in live heap size."

	GoMemHeapInUseDesc = "HeapInuse is bytes in in-use spans. In-use spans have at least one object in them. These spans an only be used for other objects of roughly the same size. " +
		"HeapInuse minus HeapAlloc estimates the amount of memory that has been dedicated to particular size classes, but is not currently being used. This is an upper bound on " +
		"fragmentation, but in general this memory can be reused efficiently."

	GoMemHeapReleasedDesc = "HeapReleased is bytes of physical memory returned to the OS. This counts heap memory from idle spans that was returned to the OS and has not yet been reacquired for the heap."

	GoMemPauseTotalNSDesc = "PauseTotalNs is the cumulative nanoseconds in GC stop-the-world pauses since the program started. During a stop-the-world pause, all goroutines are paused and only the garbage collector can run."

	GoMemStackInUseDesc = "StackInuse is bytes in stack spans. In-use stack spans have at least one stack in them. These spans can only be used for other stacks of the same size. There is no StackIdle because unused stack spans are " +
		"returned to the heap (and hence counted toward HeapIdle)."

	GoMemStackSysDesc = "StackSys is bytes of stack memory obtained from the OS. StackSys is StackInuse, plus any memory obtained directly from the OS for OS thread stacks (which should be minimal)."

	GoMemSysDesc = "Sys is the total bytes of memory obtained from the OS. Sys is the sum of the XSys fields below. Sys measures the virtual address space reserved by the Go runtime for the " +
		"heap, stacks, and other internal data structures. It's likely that not all of the virtual address space is backed by physical memory at any given moment, though in general " +
		"it all was at some point."

	GoroutinesHighWatermarkDesc = "Peak number of go routines since process start."

	NumGoroutinesDesc = "The total number of goroutines."

	ProcessCPUPercentUtilDesc = "The CPU's utilization as percentage value * 10. The extra 10 multiplier is a mistake left for backwards compatibility. Please consider using node_cpu_percent_utilization. The CPU usage calculation is performed based on user and system CPU time, but it does not include components such as iowait. The derivation means that the values of " +
		"process_cpu_percent_utilization and %Cpu, returned when running the top command, will differ"

	NodeCPUPercentUtilDesc = "The node CPU utilization as percentage value, since the last time this stat was called. The CPU usage calculation is performed based on user and system CPU time, but it does not include components such as iowait."

	ProcessMemoryResidentDesc = "The memory utilization (Resident Set Size) for the process, in bytes."

	PublicNetBytesRecvDesc = "The total number of bytes received (since node start-up) on the network interface to which the Sync Gateway api.public_interface is bound. By default, that is the number of bytes received on 127.0.0.1:4984 since node start-up"

	PublicNetBytesSentDesc = "The total number of bytes sent (since node start-up) on the network interface to which Sync Gateway api.public_interface is bound. By default, that is the number of bytes sent on 127.0.0.1:4984 since node start-up."

	SystemMemoryTotalDesc = "The total memory available on the system in bytes."

	WarnCountDesc = "The total number of warnings logged."

	UptimeDesc = "The total uptime."
)

// error stat
const (
	DatabaseBucketMismatchesDesc   = "The total number of times a database config is polled from a bucket that doesn't match the bucket specified in the database config."
	DatabaseCollectionConflictDesc = "The total number of times a database config is rolled back to an invalid state (collection conflicts)."
)

// cache stats descriptions
const (
	AbandonedSequencesDesc = "The total number of skipped sequences that were not found after 60 minutes and were abandoned."

	ChanCacheActiveRevsDesc = "The total number of active revisions in the channel cache."

	ChanCacheBypassCountDesc = "The total number of transient bypass channel caches created to serve requests when the channel cache was at capacity."

	ChanCacheChannelsAddedDesc = "The total number of channel caches added. The metric doesn't decrease when a channel is removed. That is, it is similar to chan_cache_num_channels but doesn't track removals."

	ChanCacheChannelsEvictedInactiveDesc = "The total number of channel cache channels evicted due to inactivity."

	ChanCacheChannelsEvictedNRUDesc = "The total number of active channel cache channels evicted, based on 'not recently used' criteria."

	ChanCacheCompactCountDesc = "The total number of channel cache compaction runs."

	ChanCacheCompactTimeDesc = "The total amount of time taken by channel cache compaction across all compaction runs."

	ChanCacheHitsDesc = "The total number of channel cache requests fully served by the cache. This metric is useful in calculating the channel cache hit ratio: channel cache hit ratio = chan_cache_hits / (chan_cache_hits + chan_cache_misses)"

	ChanCacheMaxEntriesDesc = "The total size of the largest channel cache. This metric helps with channel cache tuning, and provides a hint on cache size variation (when compared to average cache size)."

	ChanCacheMissesDesc = "The total number of channel cache requests not fully served by the cache. This metric is useful when calculating the channel cache hit ratio: channel cache hit ratio = chan_cache_hits / (chan_cache_hits + chan_cache_misses)"

	ChanCacheNumChannelsDesc = "The total number of channels being cached. The total number of channels being cached provides insight into potential max cache size requirements and also node usage (for example, chan_cache_num_channels * max_cache_size)."

	ChanCachePendingQueriesDesc = "The total number of channel cache pending queries."

	ChanCacheRemovalRevsDesc = "The total number of removal revisions in the channel cache. This metric acts as a reminder that removals must be considered when tuning the channel cache size and also helps users understand whether they should be tuning " +
		"tombstone retention policy (metadata purge interval) and running compact."

	ChanCacheTombstoneRevsDesc = "The total number of tombstone revisions in the channel cache. This metric acts as a reminder that tombstones and removals must be considered when tuning the channel cache size and also helps users understand whether they should " +
		"be tuning tombstone retention policy (metadata purge interval), and running compact."

	HighSeqCachedDesc = "The highest sequence number cached. Note: There may be skipped sequences lower than high_seq_cached."

	HighStableSeqCachedDesc = "The highest contiguous sequence number that has been cached."

	NumActiveChannelsDesc = "The total number of active channels."

	NumSkippedSeqsDesc = "The total number of skipped sequences."

	PendingSeqLengthDesc = "The total number of pending sequences. These are out-of-sequence entries waiting to be cached."

	RevCacheBypassDesc = "The total number of revision cache bypass operations performed."

	RevCacheHitsDesc = "The total number of revision cache hits. This metric can be used to calculate the ratio of revision cache hits: " +
		"Rev Cache Hit Ratio = rev_cache_hits / (rev_cache_hits + rev_cache_misses)"

	RevCacheMissesDesc = "The total number of revision cache misses. This metric can be used to calculate the ratio of revision cache misses: " +
		"Rev Cache Miss Ratio = rev_cache_misses / (rev_cache_hits + rev_cache_misses)"

	SkippedSeqLengthDesc = "The current length of the pending skipped sequence queue."

	ViewQueriesDesc = "The total view_queries."

	NonMobileIgnoredCountDesc = "Number of non mobile documents that were ignored off the cache feed."
)

// CBL Replication pull stats descriptions
const (
	AttachmentPullBytesDesc = "The total size of attachments pulled. This is the pre-compressed size."

	AttachmentPullCountDesc = "The total number of attachments pulled."

	MaxPendingDesc = "The high watermark for the number of documents buffered during feed processing, waiting on a missing earlier sequence."

	NumPullRepliActiveContinuousDesc = "The total number of continuous pull replications in the active state."

	NumPullRepliActiveOneShotDesc = "The total number of one-shot pull replications in the active state."

	NumPullRepliCaughtUpDesc = "The total number of replications which have caught up to the latest changes."

	NumPullRepliSinceZeroDesc = "The total number of new replications started (/_changes?since=0)."

	NumPullRepliContinuousDesc = "The total number of continuous pull replications."

	NumPullRepliTotalOneshotDesc = "The total number of one-shot pull replications."

	RequestChangesCountDesc = "The total number of changes requested. This metric can be used to calculate the latency of requested changes: " +
		"changes request latency = request_changes_time / request_changes_count"

	RequestChangesTimeDesc = "Total time taken to handle changes request response. This metric can be used to calculate the latency of requested changes: " +
		"changes request latency = request_changes_time / request_changes_count"

	RevProcessingTimeDesc = "The total amount of time processing rev messages (revisions) during pull revision. This metric can be used with rev_send_count to calculate " +
		"the average processing time per revision: average processing time per revision = rev_processing_time / rev_send_count"

	RevSendCountDesc = "The total number of rev messages processed during replication. This metric can be used with rev_processing_time to calculate the average processing time per revision: " +
		"average processing time per revision = rev_processing_time / rev_send_count"

	RevSendLatencyDesc = "The total amount of time between Sync Gateway receiving a request for a revision and that revision being sent. In a pull replication, Sync Gateway sends a /_changes request to " +
		"the client and the client responds with the list of revisions it wants to receive. So, rev_send_latency measures the time between the client asking for those revisions and Sync Gateway sending them to the client. " +
		"Note: Measuring time from the /_changes response means that this stat will vary significantly depending on the changes batch size A larger batch size will result in a spike of this stat, even if the processing time per revision is unchanged. " +
		"A more useful stat might be the average processing time per revision: average processing time per revision = rev_processing_time] / rev_send_count"

	NumPullRepliTotalCaughtUpDesc = "The total number of pull replications which have caught up to the latest changes across all replications."

	RevErrorCountDesc = "The total number of rev messages that were failed to be processed during replication."
)

// CBL replication push stats descriptions
const (
	AttachmentPushBytesDesc = "The total number of attachment bytes pushed."

	CompactionAttachmentStartTimeDesc = "The compaction_attachment_start_time"

	CompactionTombstoneStartTimeDesc = "The compaction_tombstone_start_time."

	AttachmentPushCountDesc = "The total number of attachments pushed."

	ConflictWriteCountDesc = "The total number of writes that left the document in a conflicted state. Includes new conflicts, and mutations that don't resolve existing conflicts."

	DocPushCountDesc = "The total number of documents pushed."

	ProposeChangeCountDesc = "The total number of changes and-or proposeChanges messages processed since node start-up. The propose_change_count stat can be useful when: (a). Assessing the number of redundant requested changes being pushed by the client. " +
		"Do this by comparing the propose_change_count value with the number of actual writes num_doc_writes, which could indicate that clients are pushing changes already known to Sync Gateway. " +
		"(b). Identifying situations where push replications are unexpectedly being restarted from zero."

	ProposeChangeTimeDesc = "The total time spent processing changes and/or proposeChanges messages. The propose_change_time stat can be useful in diagnosing push replication issues arising from potential bottlenecks changes and-or proposeChanges processing. " +
		"Note: The propose_change_time is not included in the write_processing_time"

	SyncFunctionCountDesc = "The total number of times that the sync_function is evaluated. The {sync_function_count_ stat is useful in assessing the usage of the sync_function, when used in conjunction with the sync_function_time."

	SyncFunctionTimeDesc = "The total time spent evaluating the sync_function. The sync_function_time stat can be useful when: (a). Troubleshooting excessively long push times, where it can help identify potential sync_function bottlenecks (for example, those arising from complex, " +
		"or inefficient, sync_function design. (b). Assessing the overall contribution of the sync_function processing to overall push replication write times."

	WriteProcessingTimeDesc = "Total time spent processing writes. Measures complete request-to-response time for a write. The write_processing_time stat can be useful when: (a). Determining the average time per write: average time per write = write_processing_time / num_doc_writes stat value " +
		"(b). Assessing the benefit of adding additional Sync Gateway nodes, as it can point to Sync Gateway being a bottleneck (c). Troubleshooting slow push replication, in which case it ought to be considered in conjunction with sync_function_time"

	WriteThrottledCountDesc = "The total number of times a revision was throttled during push replication from clients. The write_throttled_count stat can be useful to to determine an appropriate limit of concurrent revisions for each client. There's a direct tradeoff with memory and CPU usage for replicating clients and large amounts of concurrent revisions."
	WriteThrottledTimeDesc  = "The total time (in nanoseconds) waiting for an available slot to handle a pushed revision after being throttled. The write_throttled_time stat can be useful to determine whether clients are waiting too long for an available slot to push a revision. There's a direct tradeoff with memory and CPU usage for replicating clients and large amounts of concurrent revisions."

	DocPushErrorCountDesc = "The total number of documents that failed to push."
)

// Database specific stats descriptions
const (
	AbandonedSeqsDesc = "The total number of skipped sequences abandoned, based on cache.channel_cache.max_wait_skipped."

	CacheFeedDesc = "Contains low level dcp stats: (a). dcp_backfill_expected - the expected number of sequences in backfill (b). dcp_backfill_completed - the number of backfill items processed (c). dcp_rollback_count - the number of DCP rollbacks"

	Crc32MatchCountDesc = "The total number of instances during import when the document cas had changed, but the document was not imported because the document body had not changed."

	DCPCachingCountDesc = "The total number of DCP mutations added to Sync Gateway's channel cache. Can be used with dcp_caching_time to monitor cache processing latency. That is, the time between seeing a change on the DCP feed and when it's available in the channel cache: " +
		"DCP cache latency = dcp_caching_time / dcp_caching_count"

	DCPCachingTimeDesc = "The total time between a DCP mutation arriving at Sync Gateway and being added to channel cache. This metric can be used with dcp_caching_count to monitor cache processing latency. That is, the time between seeing a change on the DCP feed and when it's available in the channel cache: " +
		"dcp_cache_latency = dcp_caching_time / dcp_caching_count"

	DCPReceivedCountDesc = "The total number of document mutations received by Sync Gateway over DCP."

	DCPReceivedTimeDesc = "The time between a document write and that document being received by Sync Gateway over DCP. If the document was written prior to Sync Gateway starting the feed, it is recorded as the time since the feed was started. " +
		"This metric can be used to monitor DCP feed processing latency"

	DocReadsBytesBlipDesc = "The total number of bytes read via Couchbase Lite 2.x replication since Sync Gateway node startup."

	DocWritesBytesDesc = "The total number of bytes written as part of document writes since Sync Gateway node startup."

	DocWritesBytesBlipDesc = "The total number of bytes written as part of Couchbase Lite document writes since Sync Gateway node startup."

	DocWritesXattrBytesDesc = "The total size of xattrs written (in bytes)."

	HighSeqFeedDesc = "Highest sequence number seen on the caching DCP feed."

	NumAttachmentsCompactedDesc = "The number of attachments compacted import_feed"

	ImportFeedDesc = "Contains low level dcp stats: (a). dcp_backfill_expected - the expected number of sequences in backfill (b). dcp_backfill_completed - the number of backfill items processed (c). dcp_rollback_count - the number of DCP rollbacks"

	NumDocsReadsBlipDesc = "The total number of documents read via Couchbase Lite 2.x replication since Sync Gateway node startup."

	NumDocReadsRestDesc = "The total number of documents read via the REST API since Sync Gateway node startup. Includes Couchbase Lite 1.x replication."

	NumDocWritesDesc = "The total number of documents written by any means (replication, rest API interaction or imports) since Sync Gateway node startup."

	NumReplicationsActiveDesc = "The total number of active replications."

	NumReplicationsTotalDesc = "The total number of replications created since Sync Gateway node startup."

	SequenceAssignedCountDesc = "The total number of sequence numbers assigned."

	SequenceGetCountDesc = "The total number of high sequence lookups."

	SequenceIncrCountDesc = "The total number of times the sequence counter document has been incremented."

	SequenceReleasedCountDesc = "The total number of unused, reserved sequences released by Sync Gateway."

	SequenceReservedCountDesc = "The total number of sequences reserved by Sync Gateway."

	WarnChannelNameSizeCountDesc = "The total number of warnings relating to the channel name size."

	WarnChannelsPerDocCountDesc = "The total number of warnings relating to the channel count exceeding the channel count threshold."

	WarnGrantsPerDocCountDesc = "The total number of warnings relating to the grant count exceeding the grant count threshold."

	WarnsXattrSizeCountDesc = "The total number of warnings relating to the xattr sync data being larger than a configured threshold."

	ReplicationBytesReceivedDesc = "Total bytes received over replications to the database."

	ReplicationBytesSentDesc = "Total bytes sent over replications from the database."

	NumTombstonesCompactedDesc = "Number of tombstones compacted through tombstone compaction task on the database."

	SyncFunctionExceptionCountDesc = "The total number of times that a sync function encountered an exception (across all collections)."

	NumReplicationsRejectedLimitDesc = "The total number of times a replication connection is rejected due to it being over the threshold."

	NumPublicRestRequestsDesc = "The total number of requests sent over the public REST api."

	TotalSyncTimeDesc = "The total total sync time is a proxy for websocket connections. Tracking long lived and potentially idle connections. " +
		"This stat represents the continually growing number of connections per sec."

	ImportProcessComputeDesc = "Represents the compute unit for import processes on the database."

	PublicRestBytesWrittenDesc = "Number of bytes written over public interface for REST api"

	PublicRestBytesReadDesc = "The total amount of bytes read over the public REST api"

	SyncProcessComputeDesc = "The compute unit for syncing with clients measured through cpu time and memory used for sync"

	NumIdleKvOpsDesc = "The total number of idle kv operations."
)

// Delta Sync stats descriptions
const (
	DeltaCacheHitDesc = "The total number of requested deltas that were available in the revision cache."

	DeltaCacheMissDesc = "The total number of requested deltas that were not available in the revision cache."

	DeltaPullReplicationCountDesc = "The number of delta replications that have been run."

	DeltaPushDocCountDesc = "The total number of documents pushed as a delta from a previous revision."

	DeltasRequestedDesc = "The total number of times a revision is sent as delta from a previous revision."

	DeltasSentDesc = "The total number of revisions sent to clients as deltas."
)

// Query stats descriptions
const (
	QueryNameCountDesc = "The total number of gsi/view queries performed."

	QueryNameErrorCountDesc = "The total number of errors that occurred when performing the gsi/view query"

	QueryNameTimeDesc = "The total time taken to perform gsi/view queries."
)

// Security stats descriptions
const (
	AuthFailedCountDesc = "The total number of unsuccessful authentications. This metric is useful in monitoring the number of authentication errors."

	AuthSuccessCountDesc = "The total number of successful authentications. This metric is useful in monitoring the number of authenticated requests."

	NumAccessErrorsDesc = "The total number of documents rejected by write access functions (requireAccess, requireRole, requireUser)."

	NumDocsRejectedDesc = "The total number of documents rejected by the sync_function."

	TotalAuthTimeDesc = "The total time spent in authenticating all requests. This metric can be compared with auth_success_count and auth_failed_count " +
		"to derive an average success and-or fail rate."
)

// Shared Bucket Import stats descriptions
const (
	ImportCancelCASDesc = "The total number of imports cancelled due to cas failure."

	ImportCountDesc = "The total number of docs imported."

	ImportErrorCountDesc = "The total number of errors arising as a result of a document import."

	ImportHighSeqDesc = "The highest sequence number value imported."

	ImportPartitionsDesc = "The total number of import partitions."

	ImportProcessingTimeDesc = "The total time taken to process a document import."
)

// DB Replicators stats descriptions (ISGR Specific)
const (
	SGRDocsCheckedSentDesc = "The total number of documents checked for changes since replication started. This represents the number " +
		"of potential change notifications pushed by Sync Gateway. This is not necessarily the number of documents pushed, as a given target might already have the change " +
		"and this is used by Inter-Sync Gateway and SG Replicate. This metric can be useful when analyzing replication history, and to filter by active replications."

	SGRNumDocsFailedToPushDesc = "The total number of documents that failed to be pushed since replication started. Used by Inter-Sync Gateway and SG Replicate."

	SGRNumDocsPushedDesc = "The total number of documents that were pushed since replication started. Used by Inter-Sync Gateway and SG Replicate."

	SGRNumAttachmentsPushedDesc = "The total number of attachments that were pushed since replication started."

	SGRNumAttachmentBytesPushedDesc = "The total number of bytes in all the attachments that were pushed since replication started."

	SGRNumAttachmentsPulledDesc = "The total number of attachments that were pulled since replication started."

	SGRNumAttachmentBytesPulledDesc = "The total number of bytes in all the attachments that were pulled since replication started."

	SGRNumDocsPulledDesc = "The total number of documents that were pulled since replication started."

	SGRNumDocsPurgedDesc = "The total number of documents that were purged since replication started."

	SGRNumDocsFailedToPullDesc = "The total number of document pulls that failed since replication started."

	SGRPushConflictCountDesc = "The total number of pushed documents that conflicted since replication started."

	SGRPushRejectedCountDesc = "The total number of pushed documents that were rejected since replication started."

	SGRDocsCheckedRecvDesc = "The total number of document changes received over changes message."

	SGRDeltasRecvDesc = "The total number of documents that were purged since replication started."

	SGRDeltasRequestedDesc = "The total number of deltas requested."

	SGRDeltasSentDesc = "The total number of deltas sent."

	SGRConflictResolvedLocalCountDesc = "The total number of conflicting documents that were resolved successfully locally (by the active replicator)"

	SGRConflictResolvedRemoteCountDesc = "The total number of conflicting documents that were resolved successfully remotely (by the active replicator)"

	SGRConflictResolvedMergeCountDesc = "The total number of conflicting documents that were resolved successfully by a merge action (by the active replicator)"

	SGRNumConnectAttemptsPushDesc = "Number of connection attempts made for push replication connection."

	SGRNumReconnectsAbortedPushDesc = "Number of times push replication connection reconnect loops have failed."

	SGRNumConnectAttemptsPullDesc = "Number of connection attempts made for pull replication connection."

	SGRNumReconnectsAbortedPullDesc = "Number of times pull replication connection reconnect loops have failed."

	SGRNumHandlersPanickedDesc = "The number of times a handler panicked and didn't know how to recover from it."

	SGRExpectedSequenceLengthDesc = "The length of expectedSeqs list in the ISGR checkpointer."

	SGRExpectedSequenceLengthPostCleanupDesc = "The length of expectedSeqs list in the ISGR checkpointer after the cleanup task has been run."

	SGRProcessedSequenceLength = "The length of processedSeqs list in the ISGR checkpointer."

	SGRProcessedSequenceLengthPostCleanupDesc = "The length of processedSeqs list in the ISGR checkpointer after the cleanup task has been run."
)

// Collection stats descriptions
const (
	SyncFunctionCountCollDesc = "The total number of times that the sync_function is evaluated for this collection."

	SyncFunctionTimeCollDesc = "The total time spent evaluating the sync_function for this keyspace."

	SyncFunctionRejectCountCollDesc = "The total number of documents rejected by the sync_function for this collection."

	SyncFunctionRejectAccessCountCollDesc = "The total number of documents rejected by write access functions (requireAccess, requireRole, requireUser) for this collection."

	SyncFunctionExceptionCountCollDesc = "The total number of times the sync function encountered an exception for this collection."

	ImportCountCollDesc = "The total number of documents imported to this collection since Sync Gateway node startup."

	NumDocReadsCollDesc = "The total number of documents read from this collection since Sync Gateway node startup (i.e. sending to a client)"

	DocReadsBytesCollDesc = "The total number of bytes read from this collection as part of document writes since Sync Gateway node startup."

	NumDocWritesCollDesc = "The total number of documents written to this collection since Sync Gateway node startup (i.e. receiving from a client)"

	DocWritesBytesCollDesc = "The total number of bytes written to this collection as part of document writes since Sync Gateway node startup."
)
