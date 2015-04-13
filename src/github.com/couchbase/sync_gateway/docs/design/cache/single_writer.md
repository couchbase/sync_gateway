##Single Writer Cache

The primary goal of the distributed cache implementation for Sync Gateway is to provide an alternative to the existing in-memory change cache. The existing cache requires each Sync Gateway node to process every mutation occuring on the Couchbase Server bucket.  The intention of the distributed cache is to increase the scaling capacity of a Sync Gateway cluster.

###Overview

- **Single TAP/DCP feed** - Sync Gateway cluster only uses a single TAP/DCP feed, instead of one per Sync Gateway node  
- **Scale up writer, scale out readers** – Can increase Sync Gateway cluster capacity by scaling up the cache writer node, and scaling out cache reader nodes.  


###Cache Format
(see cache_overview)

###Cache Writer

The cache writer is responsible for writing sequences to the cache, and determining and publishing the **stable sequence** value.  

For each sequence seen on the feed, the cache writer needs to perform the following operations on the cache:
 1. Insert a new **sequence document** to the cache.
 2. For each channel associated with the revision:
  - Read and update of the **cache block** corresponding to the channel and sequence
  - Increment the **channel counter** document for the channel
 3. Update the **stable sequence** document (when appropriate) 

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






###Issues

 - **Single point of failure** - when the cache writer node fails, clients won't see any new data until the cache writer comes back online

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

Proposed approach - use the cache bucket to store cluster state, to avoid the need for a full consensus protocol implementation, based on:
 - Require that cache writers are able to communicate with the cache.  When the cache is unavailable to a node, the Sync Gateway node should go offline. No communication necessary between SG nodes, so should avoid partitioning issues
 - Leverage CAS support on coordination documents in the bucket to manage interaction between cache writers

Known issues
- Arbitrary latency on cache operations.  Need to do a deeper dive into standard consensus algorithms to figure out what subset of the functionality would be required in our scenario, to avoid things like unnecessary cycling between cache writers.


Option
1. Backup writer(s)
 - define primary and failover writer(s) in config
 - primary writer updates heartbeat document in cache, failover writer(s) monitor




###Cache writer election

Similar to failover.

###Late Sequence Handling

###Cache Write at Front End

###Backfill vs. Permanent Cache

