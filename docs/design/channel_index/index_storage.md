###Index Storage

This document covers the options for storing the actual channel index data - the information that's returned on the `_changes` feed - in the index documents.  Channel clock (and stable clock) handling is described in the [Clocks and Consistency](clock_handling.md) documentation.

##Data Requirements

In simplest terms, the channel index needs to store the data required to generate a `_changes` feed response. Each entry in the _changes response needs to include:

 * Doc Id
 * Rev Id
 * Sequence
 * Removal Flag

Queries made against the channel index are by channel and sequence value - retrieving all entries added to the channel since a given sequence.

There are several goals for the storage design:

 * Optimize write performance (minimize the number of doc reads/writes per index entry)
 * Minimize write contention (when multiple Sync Gateway nodes are updating the index)
 	* Support vbucket write responsibility switching between SG nodes
 * Optimize read performance (minimize the number of reads needed to perform the since=x query for a channel)
 * Minimize the index bucket size (number of docs, bucket size)
   * Minimize redundant information in the index
   * Support removal of deleted revisions from the index


##Option 1 - Channel Blocks and Sequence Documents

The initial approach (used in the single writer POC) used two types of documents:

 * Use **channel block** documents to store the sequences associated with each channel
   * Each **channel block** is assigned to a range of sequences (e.g. 0-10000)
   * Each **channel block** stores the values for a partition of vbuckets.
   * **Channel block** document key is based on channel name, vbucket partition index, block index.
     * `b_[channelName]_[vbucket_partition_index]_[index]`
   * The removal flag is also stored in the **channel block** with the sequence
 * Use **sequence documents** to store doc id, rev id for the sequence
   * Document key is based on vbucket and sequence
     * `s_[vbno]_[sequence]`

#####Pros

 * When a document belongs to multiple channels, the doc id and rev id are only stored once
 * The channel blocks only contain fixed length information (sequence, flags), so there are a variety of optimizations around how the sequence/flags are stored
   * Can use the block index (as offset) to minimize the size needed to store the sequence (reduces to uint16 instead of uint64)


#####Cons

 * Large number of documents in the index bucket - one document per revision in the base bucket
 * Compacting obsolete revisions is difficult - requires knowing the vb-sequence number of the revision in order to remove either the sequence document or the channel block.  Sync Gateway doesn't know the vb sequence at write time, so we can't store in the metadata for use in subsequent revisions


##Option 2 - Channel Block only approach

Store all the changes information in the **channel block** - so that the channel block stores an array of {docId, revId, sequence, flag} per vbucket

#####Pros

 * Fewer documents in index bucket
 * Fewer reads to generate the `_changes` for a channel

#####Cons

 * DocId is variable length, so requires additional size information to be stored in the channel block
 * Redundant storage of doc id/rev id for each channel
 * More work for readers to parse
 * Compacting still difficult


##Option 3 - Hybrid approach

 * Store doc id/sequence/flags in the channel block.
 * Store rev id/sequence in **sequence document**, keyed by doc id
 * On write for new revision
   * Read **sequence document** to get the previous sequence for the doc
   * Write new sequence to block, remove old from block (may be same block, may be different)

#####Pros

 * Does compacting well

#####Cons

 * Same issues related to storing DocId in the channel blocks as previous approach
 * Additional doc read per sequence to build `_changes` entry



##Notes on Partitioning

The benefits of a single writer per partition goes beyond simple write contention/CAS.  If only one SG node is writing to a partition at a time, that node can write the channel block without needing to read it first, significantly reducing the bucket ops.


