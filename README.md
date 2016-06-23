[![Build Status](http://drone.couchbase.io/api/badges/couchbase/sync_gateway/status.svg)](http://drone.couchbase.io/couchbase/sync_gateway)

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

## Install GCC

This is required on Sync Gateway 1.2 or later.  Yum is used here, but if you are on Ubuntu/Debian you will want to use `apt-get` instead.

```
$ yum install gcc
```

## Building From source (via go get)

```
$ go get -u -t github.com/couchbase/sync_gateway/...
```

At this point, you will have Sync Gateway and all of it's dependencies at the master branch for each dependency.  To anchor the dependencies to the revisions specified in the manifest XML, do the following:

```
$ cd $GOPATH
$ ./src/github.com/couchbase/sync_gateway/tools/manifest-helper -u
```

After this operation completes you should have a new `sync_gateway` binary in `$GOPATH/bin`

**Running Unit Tests**

```
$ go test github.com/couchbase/sync_gateway/...
```

**Rebuilding**

If you need to make any changes to the source and want to rebuild, run:

```
$ go install github.com/couchbase/sync_gateway/...
```

which will overwrite the `sync_gateway` binary in `$GOPATH/bin` with the newer version.


**Switching to a feature branch**

First, checkout the branch you want for Sync Gateway:

```
$ cd $GOPATH/src/github.com/couchbase/sync_gateway/
$ git checkout -t remotes/origin/feature/issue_1688
```

Run `go get` again to get any missing dependencies (for example, new dependencies that have been added for this branch)

NOTE: you will get a lot of warnings from running this command.

```
$ cd $GOPATH/src/github.com/couchbase/sync_gateway/
$ go get -u 
```

Running `go get` here will put your Sync Gateway back on the master branch, so you'll need to go *back* to the feature branch again:

```
$ cd $GOPATH/src/github.com/couchbase/sync_gateway/
$ git checkout -t remotes/origin/feature/issue_1688
```

Anchor all dependencies to the revisions specified in the manifest:

```
$ cd $GOPATH
$ ./src/github.com/couchbase/sync_gateway/tools/manifest-helper -u
```

Run tests:

```
$ go test github.com/couchbase/sync_gateway/...
```

**Switching back to the master branch**

First, checkout the master branch in the Sync Gateway repo

```
$ cd $GOPATH/src/github.com/couchbase/sync_gateway/
$ git checkout master
```

Update all dependencies to checkout their master branch

```
$ cd $GOPATH
$ ./src/github.com/couchbase/sync_gateway/tools/manifest-helper -r
```

Update Sync Gateway and all dependencies to latest versions on github

```
$ go get -u -t github.com/couchbase/sync_gateway/...
```

Run tests:

```
$ go test github.com/couchbase/sync_gateway/...
```

**Running Benchmarks**

```
go test github.com/couchbase/sync_gateway/... -bench='LoggingPerformance' -benchtime 1m -run XXX
go test github.com/couchbase/sync_gateway/... -bench='RestApiGetDocPerformance' -cpu 1,2,4 -benchtime 1m -run XXX
```

## Building From Source (via repo)

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

Warning: If you see the following error message and no `godeps` directory is created when running `bootstrap.sh` 

```
gpg: keyring `/Users/youruser/.repoconfig/gnupg/pubring.gpg' created
gpg: Signature made Wed  2 Mar 20:59:22 2016 GMT using DSA key ID 920F5C65
gpg: Can't check signature: public key not found
error: could not verify the tag 'v1.12.33'
```

Then [See this ticket](https://github.com/couchbase/sync_gateway/issues/1654) 

**Switching to a different branch**

```
$ ./bootstrap.sh <commit-sha>
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
