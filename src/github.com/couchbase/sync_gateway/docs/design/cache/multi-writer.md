###Multi-writer Cache

###Main Challenges
 1. Revision sharding. Need to distribute the incoming revisions across cache writers.
 2. Stable sequence. Calculation of the stable sequence for the cache requires understanding of cache updates made by all writers.
 3. Node failure. Handling when a cache writer node fails.

###Revision Sharding

**Options**
1. Multiple DCP feeds, each for a subset of vbucket
Each cache writer would instantiate a DCP feed, connected to a subset of vbuckets.  This solves the "DDOS" problem - each Sync Gateway node would only be processing a subset of the revisions seen on the bucket.  This doesn't address the server performance degradation as a result of serving multiple DCP feeds. 

**Open Issues**
 * Confirm with server team that multiple DCP feeds connecting to partitioned subsets of vbuckets has the same performance overhead/concerns as multiple full DCP feeds.



2. Using projector, by vbucket
The 2i projector component (https://github.com/couchbase/indexing/blob/master/secondary/docs/design/markdown/projector.md) supports partitioning a DCP stream into multiple substreams, without the overhead of additional server nodes.

**Open Issues**
 * If we're running a local projector instance (on a Sync Gateway node), the projector node becomes a single point of failure - requires failover handling at minimum.
 * Projector on its own doesn't do any cluster-management type operations - we'd need to do the work to ensure projector clients are online, covering the full set of index values, etc.
 * Currently projector isn't available as a server component - when 2i is available, we may be able to reuse existing projector instance on the indexing nodes, but that would introduce a dependency on 2i for mobile.


3. Projector, by sequence mod
Instead of sharding revisions by vbucket, shard them by a mod of the current SG sequence number.  Has the advantage that each writer knows what sequence values it expects to see, and so can do the work to identify a stable sequence for the sequences it's responsible for, simplifying the work to calculate an overall stable sequence.

**Issues**
 * Same projector issues as #2
 * We have several corner cases where a single revision allocates several sequence values (unused sequences during during CAS conflicts, sequences being deduplicated by DCP).  These sequences will be getting routed to the "wrong" cache writer.


###Stable Sequence 

**Options**
 1. Cache controller.  Cache writers post status to well-known location in the cache bucket.  Controller process monitors and aggregates status, and updates the stable sequence.  This would support continued use of the existing sequence model.  

 2. [DCP timestamp as sequence](timestamp_sequence.md) 


###Node Failure

Can rely on cache bucket connection for sharing state.  Failover scenarios are more complex than the single writer case - potentially needs to handle rebalancing of mutation load based on the number of available writers.