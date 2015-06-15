##Clocks and Consistency

One of the key requirements of the channel index is the ability to define a **stable sequence**, which allows replication processing to deliver the guarantee that a client has seen everything up to a given point in a channel.  This is achieved in the current architecture using sequence buffering: each Sync Gateway node buffers incoming events on the DCP feed to be able to validate that it's seen everything up to a given sequence number.

When each Sync Gateway node only sees a subset of the mutations coming from the Couchbase Server (see (Feed Management)[feed_management.md] for more details), each node isn't able to identify the stable sequence on it's own.  Each Sync Gateway node can only define the last seen sequence for the vbuckets it has been allocated.  

Despite this, using the vbucket sequence has a significant advantage over the metadata-based sequence - we are guaranteed to see monotonically increasing sequences for a given vbucket.  There isn't any buffering required - the process of determining the overall **stable sequence** for the channel index is just a matter of building the vector clock of the highest sequences that have been indexed for each vbucket, and storing this in the index as the **stable clock**.  

####Stable Clock 

The initial implementation of the stable clock will store it in a single document.  Each Sync Gateway node doing index writes will periodically update the stable clock sequences for the vbuckets in its partitions.  This is done periodically (instead of on each index update) to optimize performance - having each sync gateway node attempt to update the stable clock after every mutation isn't going to be efficient.  The default setting will be to update the stable clock at least once per second.  This attempts to balance `_changes` latency with clock write performance.

The preferred approach is to maintain a single **stable clock** document, updated by all SG writer nodes, to reduce the amount of reads that index readers need to do perform to retrieve the stable clock value. This has the potential for high contention/CAS-loop processing during high write load. If that proves to be a problem, we would need to switch to multiple **stable clock** documents, each handling one or more vbucket partitions.   

####Channel Clock

Each channel in the index will also maintain a **channel clock** document.  This has the same construction as the **stable clock**, but unlike the **stable clock** it's main purpose isn't data consistency.  Instead, the channel clock is used to optimize performance when reading the channel index.  The **channel clock** can be used to identify whether a particular channel has new revisions, without loading and parsing the **channel block** documents. In addition, comparing the requested `since` value to the channel clock will allow index readers to identify exactly which vbuckets have changed, and so only have to process those **channel blocks**.

####Channel Counter

To support notification handling for continuous `_changes` feeds, Sync Gateways reading the channel index need to be able to quickly identify whether a channel has changed - ideally without having to do a full compare of the **channel clock**.  To better support this, the channel index will also include a **channel counter** doc, storing a simple numeric value.  Index writers will issue an atomic increment to the channel counter after updating the channel index.


####User and Role Counters

Another requirement of continuous `_changes` processing is to receive notification when a user or role document has been changed (to handle things like access grants in the middle of a continuous `_changes` request).  

When a Sync Gateway node sees an update to a `_user` or `_role` document on the DCP feed, it will increment the corresponding **principal counter** document in the bucket.  

*TODO: is there a potential optimization here, where the Sync Gateway node only updates the **principal counter** if it already exists (so that we're not maintaining counters for users that don't have an active `_changes` feed)?  The tricky question there would be how to expire user and role counters once they are no longer in use, without deleting users that are involved in long-running `_changes` requests that have gone quiet, or counters being used by multiple requests across different nodes. It might be possible to use document expirty, and do a touch on the relevant user and role document as part of heartbeat processing, but it still feels like there's a potential for missed user updates.*  