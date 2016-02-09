##Timestamp Sequence

Discussion on using Couchbase Server's native sequence handling as an alternative to Sync Gateway's current sequence allocation model

###Overview
Couchbase Server assigns an internal sequence value to each mutation.  Within a given vbucket, Couchbase Server guarantees monotomically increasing sequence values.  

Based on this, we can construct a stable timestamp for the entire cluster, as a vector clock of the current high sequence values for each vbucket. This timestamp satisfies the requirements for use in Sync Gateway replication as a sequence.


###Benefits

 1. Sync Gateway no longer needs to do it's own allocation of sequences via the _sync:seq document. This removes the need for the Incr call we're currently making for each write to the bucket.
 2. Removes the need to store the sequence in the sync metadata.
 3. Decouples stable sequence handling when multiple Sync Gateway cache writers are assigned different subsets of vbuckets.

###Issues

 1. The size of the full timestamp can be as large as 1024 x 8 bytes (assumes maximum 8 bytes for variable length storage of uint64 sequence number, calculation of vbucket no. based on position in timestamp).  8k per revision is a lot of additional throughput on _changes requests, which currently include the sequence number for each change returned.  Compression may alleviate this, but it's going to be significantly more data than the current sequence values.
 2. The compare operations for sequences becomes much more computationally expensive. During changes processing there is a great deal of sequence comparison happening.  Some of that may go away if we no longer need to buffer incoming sequences, but it's still a concern.

###DCP snapshots and hashed sequences

Mutation events on the DCP stream are sent in **snapshots**: snapshot start events appear regularly on the feed. One way to simplify the complexity of the timestamp-sequence would be to assign all mutations in the snapshot the same sequence (the sequence-timestamp at the start of the snapshot).  A lookup table could be used in the cache bucket to map the snapshot-sequence values to a simpler, monotomically increasing value, and that value could be used by Sync Gateway for replication processing.

###Sharding writers by vbucket

One approach for distributing the cache writer work across multiple nodes is to shard by vbucket. This could be done using separate DCP streams on each cache writer (each one requesting a stream for a subset of vbuckets), or using something like projector to filter out the DCP feed.  When using a timestamp-based sequence, each cache writer would be responsible for a discrete subset of the timestamp, which should significantly simplify calculation of the overall stable timestamp.

###Cache Block Storage

Cache block storage could be modified to use one cache block per channel per vbucket subset.  Within the cache block, sequences would be grouped by vbucket.  Cache reader requests for a given cache block would pass the subset timestamp as a since value.

**Benefits**
 * Avoids contention on the cache blocks by cache writers

**Drawbacks**
 * Each cache reader channel request would require one cache block read per cache writer per channel
 * Additional storage space is needed in the cache block for vbucket id metadata
 * More computation required (parsing of the full cache block) for each cache block read


