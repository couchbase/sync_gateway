##Remote Change Cache

The primary goal of the distributed cache implementation for Sync Gateway is to provide an 
alternative to the existing in-memory change cache. The existing cache requires each Sync Gateway
node to process every mutation occuring on the Couchbase Server bucket.  The intention of the distributed
cache is to increase the scaling capacity of a Sync Gateway cluster.

###Limitations of Current Approach
- [multiple feeds cause server perf problems]
- [each node must process full feed - "DDOS" - results in a cap on throughput]

###Goals

- **Scale Out Sync Gateway** – Ability to scale out Sync Gateway capacity by adding additional nodes to the cluster.
- **Reduce Couchbase Server Overhead** – Add additional Sync Gateway nodes to the cluster without requiring the Couchbase Server to provide additional TAP/DCP feeds 
- **Replication Consistency** – Consistent sequence handling to ensure replication stability and consistency

###Components
- **[Cache Overview](cache_overview.md)** 

###Implementations
- **[Single Writer Cache](single_writer.md)** - A remote cache with a single cache writer, multiple cache readers
- **[Multi-writer Cache](multi_writer.md)** - Multi-writer cache

