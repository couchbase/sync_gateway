###Sequence Handling

In the current architecture, Sync Gateway manages its own sequence value: using atomic increments to a document in the bucket (_sync:seq) to generate unique increasing sequence numbers across the SG cluster. Each Sync Gateway node buffers the sequences seen on the server feed in order to ensure completeness/consistency for clients.

There are several scaling issues when retaining the current sequence management approach:

 1. We've already started to see high contention on the _sync:seq document in performance testing, even with only 2 Sync Gateway nodes (doing 500 writes/sec each).  This will only become more of a bottleneck as we attempt to generate higher throughput.
 2. When moving to a remote channel index, with each SG node writing responsible for a subset of the server's mutation feed, individual nodes no longer have the ability to buffer the sequence set independently.  All SG index writer nodes would need to calculate the stable sequence value collaboratively, by monitoring and updating a shared value in the bucket.  This would also be subject to high contention, and also increase latency significantly.
 3. Sync Gateway needs to do an additional bucket op (the sequence incr) for each write operation.


The proposed approach is to use Couchbase Server's internal vbucket sequences.  Each vbucket maintains a monotonically increasing sequence for changes in that vbucket.  Sync Gateway can build a vector clock made up of the full set of {vbucket, sequence} tuples, and use this as a sequence for replication.

This approach has several advantages:

 * No contention or incr overhead at write time
 * Comparing two vector clocks allows us to identify exactly which vbuckets have changed.
 * We can restart the DCP feed at a given vector clock, which avoids the problem described in issue #707

However, there are three main drawbacks of using this approach:

  1. Additional computation required to compare two sequences
  2. A sequence that's a vector clock of 1024 uint64 values is going to be large - even with compression we'd be unlikely to get it below 1-2K. This adds significant processing and network overhead to replication
  3. The sequence value is no longer going to be (obviously) intuitive for users to reason about. This doesn't violate the Couch DB replication protocol, which specifically treats sequences as opaque, but it's still unfortunate.


## Proposed Implementation

The key problem is how to transport vector clock information.  The planned approach is to use a lookup table to map the vector clocks to more compact values.Define a non-unique hash function to convert the vector clock value to a shorter string.  Use this string as the key for a lookup document in the index bucket. Within that document, store an array of vector clock values (to handle hash collisions).  The sequence used for replication will be of the form [string]:[index].

This approach could be used for `since` and `last_seq` values.  However, it won't be efficient to use this for every sequence value sent in a continuous `_changes` response.  For these, Sync Gateway would calculate the lookup sequence value once per changes loop iteration, and send the same lookup sequence value for each value sent in that iteration.  If the client drops partway through processing, worst case the client will retrieve the same values on the next changes request (which will be `revs_diff`'d away).


