##Index Writes

###Design

Each Sync Gateway node is assigned a subset of vbuckets from the cluster- [see the Feed Management documentation for details.](feed_management.md).  The node works that feed, and writes to the index bucket.

In the index bucket, channel membership is tracked per vbucket.

As with the current model, events received over the feed will be processed by Sync Gateway and added to the bucket.  Processing involves the following steps:

####Standard Document Handling

To optimize writes to the index bucket, updates to the index will typically be done in batches.  The initial implementation will be a simple loop to process the current batch, and queue entries for the next batch while the previous batch is being processed.  

Batch processing steps (in order): 

  1. Group the updates in the batch by channel
  2. For each channel:
    a. Issue an update to the appropriate **channel block(s)** for the vbucket(s) and channel.
    b. Update the **channel clock**, based on the high sequence values for each vbucket in the batch (by channel). 
    c. Increment the **channel counter**.
  3. Update the **stable clock**, based on the high sequence values for each vbucket in the batch.


####Principal Document Handling (user, role)

We need to track changes to user and role documents to be able to notify continuous changes feeds that the user document has changed.  Principal document handling is just to increment the **principal channel counter** document.



##Milestone 1 Tasks

 1. Abstract interface for index writer, to support config-based switching between existing in-memory cache and channel index. We'll want this ability at least during development for comparing 
 results of the implementations.  The initial implementation is already on the distributed_cache branch.  This was intended for POC, though - needs a more thought out refactoring to properly 
 decouple feed processing, sequence management, and index storage.

 2. Implement initial implementation for channel index storage

 3. Implement batch processing (already partially implemented on distributed)cache branch)


##Milestone 2 Tasks

  1. Add support for removal of 
