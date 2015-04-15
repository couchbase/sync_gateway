
##Storage
The distributed cache will be stored in Couchbase Server, using a Couchbase bucket.  The main advantage of using a Couchbase bucket is that the cache is persistent, which means that there is near zero cache warmup time required on a system startup. A Couchbase bucket can replicate cache data across a CBS instances, so that the cache will not become a single point of failure within a sync gateway cluster.

Using a memcached bucket was considered, but based on initial discussions with the Couchbase support team, it's not expected to provide significant performance improvements. Head-to-head performance analysis is planned, if the Couchbase bucket performance doesn't meet targets.

##Data Model

The cache contains one or more channel caches, where each channel cache contains the set of sequences in that channel. A sequence entry in the cache represents either a document assigned to that channel, or a document revision indicating removal from the channel.

For each sequence, a separate document is stored containing the details of the document revision referenced by that sequence.

For each channel, a separate document stores a counter for the channel.  This is stored externally to the channel cache to reduce contention when accessing the counter.



##Document Types and Terminology

**Cache block** documents are used to store the sequences associated with a channel.  A **channel cache** consists of one or more blocks. Block numbers start at '0'. 

 * Document name: _cache:[channel name]:block[n]
 * Document format: see below

**Sequence document** stores the Document ID, Revision ID, and change flags associated with the sequence. 

 * Document name: _cache:\_seq:[sequence number]
 * Document format: JSON

The **channel counter** document stores a single integer counter, which is incremented when a change is made to a cache block document.

 * Document name: _cache:\_count:[channel name]
 * Document format: integer value

##Cache Implementation options

###Sequence-indexed Array

Cache block documents are a fixed size byte array.  The value of `byte[n]` is a flag for the presence of sequence `n` in the channel.  

Currently two bits are used to store each sequence:
 * bit 0 - presence of sequence in the channel
 * bit 1 - whether the sequence is a removal from the channel

The cache block size is currently set at 10000 bytes, so cache block 0 stores sequences 0-9999, cache block 1 stores sequences 10000-19999, etc.  More detailed performance analysis is required to identify the ideal cache block size (to balance document size with multiple retrievals).  

Note: The POC assigns a full byte for each sequence, but if we don't identify a need for the additional 6 bits, this can be refactored to increase the number of sequences we can store per cache block.


**Benefits**
 1. No cache introspection needed to identify the cache block for a given sequence - can be determined based on cache block size. This is beneficial for both the standard read operations (find all sequences since n), and write operations (set flags for sequence n)
 2. Cache is sorted for readers.

**Drawbacks**
 1. Updates to cache blocks require two operations (read and CAS write).
 2. Write contention for multiple writers.
 3. Sparsely populated channels have a high block-to-sequence ratio, meaning more cache block reads when processing a large range of sequence values. It's possible to mitigate this to some degree by adding linked-list style 'previous block' or 'next block' metadata to the cache block, to avoid attempted reads for empty cache blocks.



###Append-based Array

Cache block documents are an unsorted array of sequence entries.  Each sequence entry consists of variable-size byte representation of an int64 (maximum 8 bytes), where the integer value represents the sequence number, and the sign is used as the removal flag.

Cache writers add sequences to the cache block using an atomic append.  Cache writers are responsible for parsing the full cache block and doing the required sorting/processing of the contained sequences.

**Benefits**
 1. Fast write operation (atomic append, instead of read/CAS write)
 2. Reduced write contention

**Drawbacks** 
 1. Writers need to iterate over entire cache block and sort results to perform usual operations (since=n)
 2. More space required per sequence (8 bytes) - results in fewer sequences per block


###Using Couchbase Server secondary indexes to replace the channel cache.

Theoretically secondary indexes should out perform the document based cache implementations for high load scenarios as indexes can be traversed on the server without the need to transfer data to the client.

We need to explore whether a secondary index can be built that has the functionality to support the channel cache.

#Cache API - POC

Cache writers and readers are the objects in Sync Gateway that are responsible for interacting with the distributed cache.

In the initial implementation of the POC it is assumed there is a single writer in a cluster and that there will be multiple readers servicing client _changes requests.

##Writer Operations

###AddToCache(entry LogEntry)

  * Writes the sequence entry – contains DocId, RevId, sequence, Flags. Currently writing as JSON for simplicity, but could change that as needed

  * For each channel:
    * adds sequence to the cache block
      1. Does GetRaw on the appropriate block for the sequence (uses block number=uint16(sequence / cacheBlockCapacity) to get block as []byte
        * If not found, initializes a new block
      2. Updates byte[sequence]
        * There's some code in place for a fixed length offset at the start of the block to handle any block metadata (linked list reference) at the start of the block
      3. Does SetRaw to write the updated block
        * This is usually the last sequence in the block (but not always, if we want to support late sequences like in issue #525)
    * updates the cache clock.
      * Currently the POC is setting clock to the entry sequence value, only if the sequence is greater than the previous value.
      * I think this should probably change to setting two values in the 'clock' file – the high sequence (as above), and a generic counter. This will ensure Notify gets triggered for late arriving sequences

###Prune()
  * Currently a no-op for the distributed cache.  Would remove old cache blocks based on some criteria?

###Clear()
  * Currently a no-op.  Should flush the cache


##Reader Operations

###GetChanges()

  * Calls GetCachedChanges, then does a view-based backfill if necessary

###GetCachedChanges(channelName, ChangesOptions)

  * We're caching the changes from the previous notify, to optimize when there are many continuous connections. So first checks if the options.Since matches the cached notify. If so, returns that, otherwise…
  * Gets the cache values (ascending)
    * Gets the starting cache block based on the since value
    * Sets startIndex to sequence
    * Iterate over blocks, until nextBlock=nil
      * Iterate within the block, from startIndex to end. If byte[i] > 0,
        * read the sequence entry (need DocId for next phase), and add to 'cacheContents'
        * do 'removed' handling
      * update startIndex to the start of the next block
    * Deduplicate by iterating backwards over 'cacheContents', removing duplicate DocIDs
    * Return 'cacheContents', to limit