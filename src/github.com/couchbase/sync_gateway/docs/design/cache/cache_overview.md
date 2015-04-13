
##Storage
The distributed cache will be stored in Couchbase Server, using a Couchbase bucket.  The advantage of using a Couchbase bucket is that the cache is persistent, which means that there is near zero cache warmup time required on a system startup. A Couchbase bucket can replicate cache data across a CBS instances, so that the cache will not become a single point of failure within a sync gateway cluster.

##Data Model

The cache contains one or more channel caches, where each channel cache contains the set of sequences in that channel. A sequence entry in the cache represents either a document assigned to that channel, or a document revision indicating removal from the channel.

For each sequence, a separate document is stored containing the details of the document revision referenced by that sequence.

For each channel, a separate document stores a counter for the channel.  This is stored externally to the channel cache to reduce contention when accessing the counter.



##Document Types and Terminology

**Cache block** documents are used to store the sequences associated with a channel.  A **channel cache** consists of one or more blocks. Block numbers start at '0'. 

 - Document name: _cache:[channel name]:block[n]
 - Document format: TBD

**Sequence documents** store the Document ID, Revision ID, and change flags associated with the sequence.. 

- Document name: _cache:\_seq:[sequence number]
- Document format: TBD

The **channel counter** document is a 

 - Document name: _cache:\_count:[channel name]

##Cache Implementation options
###Single cache document per channel, with sparse sequence entries.

This implementation uses a single document for each channel cache, the cache will be limited to a maximum file size of 20MBytes.

When the file limit is reached, the lowest sequence ID's will be removed, sync Gateway will then have to retrieve these low sequence ID's using a CBS view query.

In this implementation Sync Gateway takes on responsibility for the following cache operations:

Inserting out of order sequence ID's
Compaction of removed sequence ID's
Contention of multiple cache writers 

There is a good description of how to store a mutable set in memcached, which is append only and implements compaction on read [here](http://blog.couchbase.com/maintaining-set-memcached).


###Multiple cache documents per channel, linked list/array hybrid, with contiguous (dense) sequence entries.
This implementation uses multiple documents for each channel cache, there is a root document for each channel that provides cache metadata and a reference to the next cache document. The last document has a null reference to the next document to indicate that the end of the cache has been reached.

This model is based on a singly linked list, with a prefix sentinel document.

Each cache document consist of an array of contiguous sequence ID's, sequence ID values values consist of two bits:

Bit 1 indicates the absence or existence of the sequence in the channel

0: This SequenceID does not exist in this channel

1: This Sequence ID does exist in this channel

Bit 2 indicates whether this sequence represents an addition to the channel or a removal from the channel.

0: Document has been added at this sequence ID

1: Document has been removed at this sequence ID

The size of the cache documents can be configured for optimal overall performance, e.g. more smaller docs may perform better during high load read write operations.

The size of the cache documents is stored in the header document for the cache.

A basic implementation would use enough documents to store all the possible sequences in a channel regardless if the sequences exist in the channel or not.

Various optimisations are possible.

If none of the sequence ID's in a single channel cache document are in the channel, then that document can be ommitted, the previous document's forward reference would simply point to the next populated block.

The total number of sequence ID's stored in a channel cache could be capped, in this case once the limit was reached cache documents at the beginning of the cache are removed and the forward reference in the root document updated to point to the first populated cache document.

There is no need for compaction as sequenceID's always exist in the contiguous cache documents, but cache documents with no sequenceID's set can be removed from the linked list.

Readers can efficiently start read sequence ID's that start after their since value, as they can seek to the since sequenceID with minimal reads.

Writers can efficiently write out of order sequenceID's, as they can seek to where the sequenceID must be inserted with minimal reads.

#Other Options

##Hybrid channel cache designs
Alternative approaches might combine features from the two implementations described above.
As an example a single document cache, with non-sparce sequenceID's using append only writes.


##Using Couchbase Server secondary indexes to replace the channel cache.

Theoretically secondary indexes should out perform the document based cache implementations for high load scenarios as indexes can be traversed on the server without the need to transfer data to the client.

We need to explore whether a secondary index can be built that has the functionality to support the channel cache.

#Cache API

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
      * Currently I'm setting clock to the entry sequence value, only if the sequence is greater than the previous value.
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