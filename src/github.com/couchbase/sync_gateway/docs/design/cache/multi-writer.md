###Multi-writer Cache

###Main Challenges
 1. Revision sharding
 2. Stable sequence 
 3. Node failure

###Revision Sharding

**Options**
1. Multiple DCP feeds, by vbucket
Each cache writer would instantiate a DCP feed, connected to a subset of vbuckets.  This solves the "DDOS" problem, but not the multiple DCP feed issue.
2. Projector, by vbucket
3. Projector, by sequence mod
Has the advantage that each writer knows what sequence values it expects to see, and so can do the work to identify a stable sequence for the sequences it's responsible for.


###Stable Sequence 

**Options**
1. Controller.  Cache writers post status to well-known location.  Controller process monitors and aggregates status, updates the stable sequence.  Allows continued use of existing sequence model.
2. DCP timestamp as sequence.  
[TBD - link to description of stable time stamp, cache storage, cache retrieval, key size and usage, possible snapshot based key model (multiple revisions per value)]


###Node Failure

- can rely on cache bucket connection for sharing state
- failover needs to handle the number of available writers changing