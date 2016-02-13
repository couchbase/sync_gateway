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

## Building From Source 

**Get repo tool**

```
$ curl https://storage.googleapis.com/git-repo-downloads/repo > repo
$ chmod +x repo
```

**Init repo**

```
$ ./repo init -u "https://github.com/couchbaselabs/sync_gateway.git" -m manifest/default.xml
```

**Repo sync**

```
$ ./repo sync
```

**Build, Test and Install**

```
$ export GOPATH=`pwd`/godeps
$ cd $GOPATH/src/github.com/couchbase/sync_gateway
$ go test ./... && go install ./...
```

## Building From source via `go get`

Warning: there are [known issues](https://github.com/couchbase/sync_gateway/issues/1585) with this approach!

```
go get -u -t github.com/couchbase/sync_gateway/...
```

After this operation completes you should have a new `sync_gateway` binary in `$GOPATH/bin`

**Running Unit Tests**

```
$ cd $GOPATH/src/github.com/couchbase/sync_gateway/
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
