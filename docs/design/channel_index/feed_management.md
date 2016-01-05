##Feed Partition/Management

Each Sync Gateway node will be assigned a subset of vbuckets - call this set of vbuckets a **partition**.

The number of partitions defined will be a configurable setting, defined for the cluster. The number of partitions should be greater than the number of Sync Gateway nodes initially in the cluster, to support future scale out.  More detailed performance testing is required to identify the recommended partition values.

Each Sync Gateway node is given responsibility for one or more partitions.  When establishing it's DCP feed, a Sync Gateway node will start streams for each vbucket it is responsible for.  

###Milestone 1

The number of partitions for the cluster will be fixed at 64.  Each of the Sync Gateway nodes will be assigned partitions in its config file.  For this milestone, partitions will simply be made up of a continuous sequence of vbuckets - e.g. partition 0 represent vbuckets 0 through 15.  When the partition assignment is moved out of the Sync Gateway config in future revisions, it will also be possible to define something other than a sequential range of vbuckets as a partition.  

```
{"partitions": [0,1,2,3,4,5,6,7,8,9]} 
```

Each Sync Gateway will start a DCP feed, and start streams for each vbucket in each of their partitions.

This milestone should also include support for handling a the case where a vbucket stream is closed by the CBS cluster.  Sync Gateway will attempt to reopen the stream at the last stable sequence for that vbucket and continue processing.

