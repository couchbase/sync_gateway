# Couchbase Sync Gateway

[![Join the chat at https://gitter.im/couchbase/mobile](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/couchbase/mobile?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Gluing [Couchbase Lite][COUCHBASE_LITE] to [Couchbase Server][COUCHBASE_SERVER]

The Sync Gateway manages HTTP-based data access for mobile clients. It handles access control and data routing, so that a single large Couchbase Server cluster can manage data for multiple users and complex applications.

[**Product home page**](http://www.couchbase.com/mobile)

[**Documentation**](http://developer.couchbase.com/mobile/develop/guides/sync-gateway/index.html)

[**Downloads**](http://www.couchbase.com/download#cb-mobile)

## Building From Source

To build Sync Gateway from source, you must have the following installed:

* Go 1.4 or later 
* GCC for CGO (required on Sync Gateway 1.2 or later)

On Mac or Unix systems, you can build Sync Gateway from source as follows:

Open a terminal window and change to the directory that you want to store Sync Gateway in.

Clone the Sync Gateway GitHub repository:

```
$ git clone https://github.com/couchbase/sync_gateway.git
```
 
Change to the sync_gateway directory:

```
$ cd sync_gateway
```
 
Set up the submodules:

```
$ git submodule init
$ git submodule update
```
Build Sync Gateway:

```
$ ./build.sh
```
Sync Gateway is a standalone, native executable located in the ./bin directory. You can run the executable from the build location or move it anywhere you want.

To update your build later, pull the latest updates from GitHub, update the submodules, and run ./build.sh again.


<img src="http://jchris.ic.ht/files/slides/mobile-arch.png" width="600px"/>

### License

Apache 2 license.

## Tutorials and Other Resources

* [Broad overview of mobile and Couchbase Lite](https://github.com/couchbase/mobile)

* [Example Sync Gateway Configurations](https://github.com/couchbase/sync_gateway/wiki/Example-Configs)

* [Mailing list][MAILING_LIST] -- feel free to ask for help!

* [File a bug report][ISSUE_TRACKER] if you find a bug.


[COUCHBASE_LITE]: https://github.com/couchbase/couchbase-lite-ios
[COUCHDB]: http://couchdb.apache.org
[COUCHDB_API]: http://wiki.apache.org/couchdb/Complete_HTTP_API_Reference
[COUCHBASE_SERVER]: http://www.couchbase.com/couchbase-server/overview
[WALRUS]: https://github.com/couchbaselabs/walrus
[HTTPIE]: http://httpie.org
[MAILING_LIST]: https://groups.google.com/forum/?fromgroups#!forum/mobile-couchbase
[ISSUE_TRACKER]: https://github.com/couchbase/sync_gateway/issues?state=open
[MAC_STABLE_BUILD]: http://cbfs-ext.hq.couchbase.com/mobile/SyncGateway/SyncGateway-Mac.zip
