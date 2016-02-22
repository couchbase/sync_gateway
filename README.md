[![Build Status](http://drone.couchbasemobile.com/api/badges/couchbase/sync_gateway/status.svg)](http://drone.couchbasemobile.com/couchbase/sync_gateway)

# Couchbase Sync Gateway

[![Join the chat at https://gitter.im/couchbase/sync_gateway](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/couchbase/sync_gateway?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Gluing [Couchbase Lite][COUCHBASE_LITE] to [Couchbase Server][COUCHBASE_SERVER]

The Sync Gateway manages HTTP-based data access for mobile clients. It handles access control and data routing, so that a single large Couchbase Server cluster can manage data for multiple users and complex applications.

[**Product home page**](http://www.couchbase.com/mobile)

[**Documentation**](http://developer.couchbase.com/mobile/develop/guides/sync-gateway/index.html)

[**Downloads**](http://www.couchbase.com/download#cb-mobile)

## Build pre-requisites

To build Sync Gateway from source, you must have the following installed:

* Go 1.5 or later with your `$GOPATH` set to a valid directory
* GCC for CGO (required on Sync Gateway 1.2 or later)

## Building From source (without dependency pinning)

Warning: while this is the easiest way to build sync gateway from source, there are [known issues](https://github.com/couchbase/sync_gateway/issues/1585) with this approach that cause certain tests to fail!  To be able to run the full unit test suite and have it pass, skip down to the approach to build from source with dependency pinning.

```
go get -u -t github.com/couchbase/sync_gateway/...
```

After this operation completes you should have a new `sync_gateway` binary in `$GOPATH/bin`

**Running Unit Tests**

```
$ go test github.com/couchbase/sync_gateway/...
```

**Running Benchmarks**

```
go test github.com/couchbase/sync_gateway/... -bench='LoggingPerformance' -benchtime 1m -run XXX
go test github.com/couchbase/sync_gateway/... -bench='RestApiGetDocPerformance' -cpu 1,2,4 -benchtime 1m -run XXX
```

## Building From Source (with dependency pinning)

Running the scripts below will clone this repository and all of it's dependencies (pinned to specific versions as specified in the manifest)

```
$ mkdir ~/sync_gateway
$ cd ~/sync_gateway
$ brew install repo
$ wget https://raw.githubusercontent.com/couchbase/sync_gateway/master/bootstrap.sh
$ chmod +x bootstrap.sh
$ ./bootstrap.sh
```

**Build**

```
$ ./build.sh
```

**Running Unit Tests**

```
$ ./test.sh
```

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
