[![Build Status](http://drone.couchbase.io/api/badges/couchbase/sync_gateway/status.svg)](http://drone.couchbase.io/couchbase/sync_gateway) [![Join the chat at https://gitter.im/couchbase/discuss](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/couchbase/discuss?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![Go Report Card](https://goreportcard.com/badge/github.com/couchbase/sync_gateway)](https://goreportcard.com/report/github.com/couchbase/sync_gateway) [![codebeat badge](https://codebeat.co/badges/a8fb8053-742a-425b-8e8c-96f1c5bdbd26)](https://codebeat.co/projects/github-com-couchbase-sync_gateway) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Sync Gateway

*Features*

* Manages HTTP-based data access for [Couchbase Lite][COUCHBASE_LITE] mobile clients 
* Leverages [Couchbase Server][COUCHBASE_SERVER] as it's horizontally scalable backing data store
* Clustered into a horizontally scalable tier
* Provides access control and data routing
* Provides HTTP longpoll changes stream of all database mutations

![Diagram](http://images.cbauthx.com/mobile/1.3/20161004-093613/diagrams.001.png)

## Resources

[**Official product home page**](http://www.couchbase.com/mobile)

[**Documentation**](http://developer.couchbase.com/mobile/develop/guides/sync-gateway/index.html)

[**Downloads**](http://www.couchbase.com/download#cb-mobile)

[**Issue Tracker**][ISSUE_TRACKER] 

[**Mailing List**][MAILING_LIST]

[**Discussion Forum**][FORUM]


## Build pre-requisites

To build Sync Gateway from source, you must have the following installed:

* Go 1.7.3 or later with your `$GOPATH` set to a valid directory 
* GCC

**Install Go**

See [Installing Go](https://golang.org/doc/install)

**Install GCC**

```
$ yum install gcc
```

## Download and build

**Warning** currently the `go get` style of building is [broken](https://github.com/couchbase/sync_gateway/issues/2209) due to upstream library changes, please use the [Extended Build Instructions](docs/BUILD.md)

Download and build the code in a single step via `go get`:

```
$ go get -u -t github.com/couchbase/sync_gateway/...
```

After this operation completes you should have a new `sync_gateway` binary in `$GOPATH/bin`

*NOTE:* This build style is only suitable for development rather than deployment.  There is a chance this might fail or have runtime errors due to using the latest version of all dependencies (whereas release builds use dependency pinning).  Please file an [issue][ISSUE_TRACKER] if you run into problems.

See the [Extended Build Instructions](docs/BUILD.md) to build with dependency pinning via the `repo` multi-repository tool.

### License

Apache 2 license.


[COUCHBASE_LITE]: https://github.com/couchbase/couchbase-lite-ios
[COUCHDB]: http://couchdb.apache.org
[COUCHDB_API]: http://wiki.apache.org/couchdb/Complete_HTTP_API_Reference
[COUCHBASE_SERVER]: http://www.couchbase.com/couchbase-server/overview
[WALRUS]: https://github.com/couchbaselabs/walrus
[HTTPIE]: http://httpie.org
[MAILING_LIST]: https://groups.google.com/forum/?fromgroups#!forum/mobile-couchbase
[FORUM]: http://forums.couchbase.com
[ISSUE_TRACKER]: https://github.com/couchbase/sync_gateway/issues?state=open
[MAC_STABLE_BUILD]: http://cbfs-ext.hq.couchbase.com/mobile/SyncGateway/SyncGateway-Mac.zip
