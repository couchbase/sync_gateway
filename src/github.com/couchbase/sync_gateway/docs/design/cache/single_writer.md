##Single Writer Cache

The single writer cache aims to improve Sync Gateway scalability by avoiding the use of multiple server feeds, and centralizing the feed processing work on a single node that can be scaled up. Under this model there will still be a threshold at which the inflow rate on the feed exceeds the ability for a single Sync Gateway to process it, but this limit should be significantly higher than the existing in-memory model.   

###Overview

- **Single TAP/DCP feed** - Sync Gateway cluster only uses a single TAP/DCP feed, instead of one per Sync Gateway node  
- **Scale up writer, scale out readers** – Can increase Sync Gateway cluster capacity by scaling up the cache writer node, and scaling out cache reader nodes.  
- **Batched cache writer** - Improve processing capacity by batching cache update operations


###Cache Format
Uses sequence-indexed array, as described in the cache overview documentation.

###Cache Writer

The cache writer is responsible for writing sequences to the cache, and determining and publishing the **stable sequence** value.  

For each sequence seen on the feed, the cache writer must perform the following operations on the cache:
 1. Insert a new **sequence document** to the cache.
 2. For each channel associated with the revision:
  - Read and update of the **cache block** corresponding to the channel and sequence
  - Increment the **channel counter** document for the channel
 3. Update the **stable sequence** document (when appropriate) 

For a document with `n` channels, this results in:
 * `n` reads
 * `n + 2` writes
 * `n` incrs
 * `3n + 2` total operations per document (with the caveat that all operations aren't equal)


To improve performance, these updates can be batched and performed in parallel.  The POC uses the following approach:

Incoming feed entries are placed in a queue for caching. A separate goroutine runs a loop to work the queue, doing the following in each iteration.
 1. Read entries from the queue (up to a configurable maximum number of entries).
 2. Loop over queue entries - for each entry:
   - start a new goroutine to insert the **sequence document**
   - add sequence to appropriate channel sets
 3. Loop over channel sets - for each set:
   - Start a goroutine to read and update the **cache block(s)** corresponding to the channel and sequences
 4. Wait for goroutine completion from #2 and #3
 5. For each channel set: 
   - Start a goroutine to increment the **channel counter** for the channel
 6. Wait for goroutine completiong from #5
 7. Update the **stable sequence** document

For `m` documents distributed over `n` channels, this results in:
 * `m + n + 1` writes (`m` sequence docs + `n` channel blocks + `1` stable sequence)
 * `n` reads (`n` channel blocks)
 * `n` incrs (`n` channel counters)
 * `(3n + m + 1)/m` total operations per document.  
    * If `m` is large relative to `n` (many updates in the same channels), this approaches 1 op/sequence 
    * If `n` approaches `m` (many updates in unrelated channels), this is closer to 4 op/sequence
    * If `n` is large relative to `m` (many updates, each going to several unrelated channels), this approaches (`3n + 1`) ops per sequence


###Issues

 - **Single point of failure** - when the cache writer node fails, clients won't see any new data until the cache writer comes back online
 - **Scalability limited by cache writer** - Single cache writer must process entire feed

###Restart

**Using DCP**
Restart support is straightforward when using DCP. When the cache writer receives a snapshot start event on the DCP feed, it persists the current DCP vbucket timestamp to the cache database.  On Sync Gateway restart, it just needs to start a new DCP feed from the previous timestamp.

**Using TAP** 
TAP doesn't support starting a feed from a particular position - only a full backfill from zero, or listening to new revisions.  Reprocessing the entire mutation history from zero on each Sync Gateway restart isn't practical. On Sync Gateway restart, we need a way to  backfill the sequences between the last `stable sequence` (as stored in the cache), and the current `_sync:seq` value (we can assume any new revisions after the _sync:seq value will appear on the new TAP feed).

On Sync Gateway startup, the cache writer should:
 - Start the TAP feed listening for new mutations
 - Start a new process to backfill from the previous `stable sequence` to the current `sync:seq`

We don't currently have a view that supports a simple sequence-based range query (e.g. all sequences across all channels from seq1 to seq2). One approach would be to use the `channels` view to 
query the `*` channel for a sequence range. This would require that the `*` channel always be enabled.  


###Failover

Proposed approach - use the cache bucket to store cluster state, to avoid the need for a full consensus protocol implementation, based on the assumptions that:
 * We can assume that cache writers have a connection to the cache bucket.  When the cache is unavailable to a node, the Sync Gateway node should go offline. No communication necessary between SG nodes, so should avoid partitioning issues
 * Leverage CAS support on coordination documents in the bucket to avoid conflicting updates between cache writers

Known issues
 * We should assume arbitrary latency on cache operations.  Need to do a deeper dive into standard consensus algorithms to figure out what subset of the functionality would be required in our scenario, to avoid things like unnecessary cycling between cache writers.


Option
1. Backup writer(s)
 - define primary and failover writer(s) in config
 - primary writer updates heartbeat document in cache, failover writer(s) monitor




###Cache writer election

Similar to failover.

###Late Sequence Handling

The **stable sequence** value corresponds to the 'low' value used for late sequence handling (#525).  A similar approach can be implemented using the remote cache.  The additional work involved would be to support continuous changes requests without repeating already sent sequences.  The existing (in-memory) cache does that handling at the cache level - maintaining a list of late-arriving entries to the cache, and tracking the position of continuous changes requests in that list.  This would need to be refactored for the remote cache - information on what sequences have already been sent would need to be stored on the reader side (the SG node processing the _changes feed), and there would be additional processing by the readers to avoid sending duplicate values.

There shouldn't be any difference to the handling for non-continuous _changes requests - either longpoll or single _changes requests.

###Backfill vs. Permanent Cache

The preferred approach would be to treat the remote cache as a full index - all sequences would be present, with no need to backfill from a view query, the way we do today for the in-memory cache.  The potential concern would be the number of documents in the cache bucket - we end up with one **sequence document** for each revision in the base bucket, effectively doubling the total number of documents in the cluster.

One optimization would be to expire the **sequence documents** after a relatively long period (days?), when the total number of documents in the bucket exceeds a specified threshold.  The information in the sequence document is also available from the document in the base bucket - it just requires additional overhead to unmarshal the _sync metadata and retrieve that data. 

A similar optimization could be done to purge **sequence documents** for obsolete revisions.

Revision compaction in the base bucket should also be applied to the corresponding revisions in the cache bucket.