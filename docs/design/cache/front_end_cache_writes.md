##Front-end Cache Writes

Remote cache scalability is capped by the ability for cache writer(s) to keep up with the inflow on the feed.  A potential optimization is to have the front-end nodes write cache information when the original document is being written to the bucket, instead of waiting for it to appear on the feed.  This allows a portion of the cache write work to be distributed across the whole cluster.  In this scenario the cache writer's only responsibility is calculation of the **stable sequence** value.  

Here "front-end" just refers to any Sync Gateway node that's processing writes from clients.  

###Front-end processing

After a successful write of the base document to the bucket, front-end nodes perform the following:
 1. For each channel:
   - Read and CAS write to the **cache block** (once per channel)
   - Increment the **channel counter**.
 2. Write the **sequence document** to the cache bucket.

##Cache writer (controlller) processing

With the front end nodes updating the cache, the cache writer is primarily responsible for calculating and storing the **stable sequence** value.  This can use the standard buffered feed reader approach.  

However, the cache writer can also be used to validate consistency of updates made by the front-end nodes, by validating the existence of the **sequence document** in the cache for each sequence.  This covers the scenario where a front-end node fails after updating the base bucket, but before cache update is complete.  

##Cache reader processing

There are two options for the cache reader.  

 1. Return all data available in the cache for a given request, returning a compound sequence number with **stable sequence** as the low value. This addresses the issue originally fixed by #525 - ensuring that late arriving TAP sequences aren't blocking.  DocID deduplication will ensure that we don't have a scenario where a client sees revisions out of order on the client. This is a better solution than the one used for #525, as changes are available as soon as they are written, instead of waiting for them to reappear on the TAP feed.
 2. Only return sequences earlier than the **stable sequence** value. Omits the fast/slow processing, but simplifies the cache reader processing and makes it more intuitive/easier to reason about.

### Drawbacks
Increased contention on the **cache block** documents. This would be a particular issue for high-volume channels, such as when the `*` channel is enabled. It may be necessary to move to an append-based cache block structure to reduce contention (i.e. replace the Read/Write with an atomic Append).

###Open Issues

1. Is there a server node rollback scenario here that's different than existing scenarios?  At minimum there's a larger window for potential node failure to impact Sync Gateway processing - the time between the initial write completes and the sequence appears on the feed.