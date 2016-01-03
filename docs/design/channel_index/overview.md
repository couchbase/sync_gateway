##Overview

*TODO: provide a high-level overview with diagrams here*

##Components

 * [DCP Feed Management](feed_management.md)
    * How the Sync Gateway cluster will partition the DCP feed
 * [Sequence Handling](sequence_handling.md)
    * Describes the changes to the sequence definition used by Sync Gateway
 * [Index Storage](index_storage.md)
    * What is stored in the channel index, and how it is stored
 * [Clocks and Consistency](clock_handling.md)
    * How the channel index will manage consistency 
 * [Channel Index Writes](index_writes.md)
    * Design for writing to the channel index
 * [Channel Index Reads](index_reads.md)
    * Design for reading from the channel index

##Planning

###Milestone 1 features

####Feed Partitioning/Management
 * Initial feed partition based on config
 * Handle DCP stream end/reconnection
 * Buffer incoming feed to minimize rollback risk

####Cluster Administration
 * Temporary support for DCP partitioning based on a range of vbuckets defined in the config

####Index Writes
 * One cache block per vbucket per channel
 * Use suffix to co-locate channel docs
 * Write single clock per channel with latency (once per second), accept contention
 * Incr-based channel clock for notify
 * Shared update of stable clock

####Index Reads
 * Bulk get for channel block retrieval
 * Clock polling for notification
 * Clock polling for user/role changes


####Monitoring User and Role Changes
 * Write to user and role pseudo-channel indexes, to support notify handling
 * Include user clock, role clock
 * Ideally would optimize better than one doc per user/role

####Sequence handling
 * vector clock, simplified compression testing
 * Initial implementation of lookup map

####Performance Testing
 * continuous
 * longpoll
 * connect/disconnect
 * sync function channel assignment


###Milestone 2 features

####Feed Partitioning
 * Auto-assignment of vbucket ranges to SG nodes on startup, based on interaction with shared config document in bucket
 * Potentially leverage cbft's infrastructure
 
####Cluster Administration
 * SG node failover handling.  Detect failure, and reassign partitions to other Sync Gateway nodes.  

####Index Writes 
 * Compact/prune channel index entries when revisions are pruned

####Index Reads
 * In-memory channel cache, based on recent index reads.

####Monitoring user and role changes
 * (If needed) Improved notification - an alternative to polling, potentially based on the index bucket feed.

####Sequence handling
 * Performance improvements for lookup map

####Handling index bucket failure
 * Detecting rollback and healing index


###Milestone 3 features

####Partitioning DCP Feed
 * Heuristic vbucket allocation to partitions based on vbucket distribution in Couchbase Server cluster

####Cluster Administration

 * SG cluster rebalance support.  Vbucket reallocation across cluster, based on tuning for either contention or Couchbase Server optimization