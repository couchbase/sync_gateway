###Index Writes

##Overall Design

Each Sync Gateway node is assigned a subset of vbuckets from the cluster- [see the Feed Management documentation for details.](feed_management.md).  The node works that feed, and writes to the index bucket.

In the index bucket, channel membership is tracked per vbucket.

##Milestone 1

Tasks

 1. Abstract interface for index writer, to support toggling between existing in-memory cache and channel index.
Initial implementation is already on the distributed_cache branch.  This was intended for POC, though - needs review for potential redesign.

 2. Implement initial implementation for channel index

