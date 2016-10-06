

*This method of building Sync Gateway is not recommended and the following docs may be out of date -- see Building From Source (via repo) instead*

**Pinning dependencies**

At this point, you will have Sync Gateway and all of it's dependencies at the master branch for each dependency.  To anchor the dependencies to the revisions specified in the manifest XML, do the following:

```
$ cd $GOPATH
$ ./src/github.com/couchbase/sync_gateway/tools/manifest-helper -u
```

**Running Unit Tests**

```
$ go test github.com/couchbase/sync_gateway/...
```

See [the build instructions](build.md) for more details on building, including how to pin dependencies, as is done in the official release builds.

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
