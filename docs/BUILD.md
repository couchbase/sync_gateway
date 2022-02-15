
Building with Go Modules 
------------------------

Recent versions of Sync Gateway provide support for Go modules, which simplifies the process of building from source. Older versions will require building with pinned dependencies, see below.

Keep in mind that building the Enterprise Edition requires access to private repos, and is intended for internal use only.

**Manual Build**
```bash
go build -o bin/sync_gateway_ce .
```

**Build Script**
```bash
SG_EDITION=CE ./build.sh
```

Building Older Versions (via repo)
----------------------------------

In order to build with [pinned dependencies](https://github.com/couchbase/sync_gateway/blob/master/manifest/default.xml), you will need to use the the `repo` multi-repository tool.

**Install prequisites**

```bash
$ brew install repo
```

**Bootstrap initialization -- one time step**

Create a directory where you want to store the Sync Gateway source code and bundled dependencies, and change to that directory.  For example:

```bash
$ mkdir ~/sync_gateway; cd ~/sync_gateway
```

Download the `bootstrap.sh` shell script and run it:

```bash
$ wget https://raw.githubusercontent.com/couchbase/sync_gateway/master/bootstrap.sh
$ chmod +x bootstrap.sh
$ ./bootstrap.sh
```

After it's complete, you should see a message that says `Bootstrap complete!  Run ./build.sh to build and ./test.sh to run tests`

*Note:* if you want to run the bootstrap initialization and start on a particular Sync Gateway commit, you can provide the `-c` flag, eg `./bootstrap.sh -c y0pl33g0r425`.  

**Build and Test**

To build Sync Gateway from source and run the unit tests, run:

```bash
$ ./build.sh && ./test.sh
```

If you run into a `gpg: Can't check signature: public key not found` error, see [issue 1654](https://github.com/couchbase/sync_gateway/issues/1654) for help.

**Bootstrap variations: start on a different commit**

To bootstrap and start with a different Sync Gateway commit:

```
$ ./bootstrap.sh -c commit-hash
```

**Switch to a different sync gateway branch via snap-manifest.sh**

Make sure the `repo status` doesn't show any uncommitted changes.  For example in the output below, `docs/BUILD.md` is an uncommitted change:

```bash
$ repo status
project godeps/src/github.com/couchbase/sync_gateway/ branch feature/fix_snap_manifest_rebased
 -m     docs/BUILD.md
project godeps/src/github.com/couchbaselabs/walrus/ branch feature/sg_2418_sgbucket_interface
```

Once the `repo status` is clean, to switch to a different sync gateway commit (which must be pushed up to github):

```bash
$ ./snap-manifest.sh --sg-commit sync-gateway-commit-or-branch
```

Build via go get w/ dependency pinning
--------------------------------------

See the [Build Extra](BUILD_EXTRA.md) for instructions on how to build via `go get` (as opposed to `repo`) with dependency pinning.


Cross-compiling for Linux
--------------------------

**x86 Linux**

```
$ ls $(go env GOROOT)/pkg && sudo chown -R $USER $(go env GOROOT)/pkg
$ GOOS=linux GOARCH=amd64 ./build.sh
```

**ARM Linux**

```
GOOS=linux GOARCH=arm ./build.sh
```

The binaries will be saved to `godeps/bin/linux_amd64/` or `godeps/bin/linux_arm/`

Unit Testing options
--------------------


### Environment variables

| Name  | Description |
| ------------- | ------------- |
| SG_TEST_BACKING_STORE  | Walrus by default, but can set to "Couchbase" to have it use http://localhost:8091  |
| SG_TEST_COUCHBASE_SERVER_URL  | Uses http://localhost:8091 by default, but you can set this to any valid Couchbase Server URL  |
| SG_TEST_USE_XATTRS  | Don't use Xattrs by default, but provide the test runner to specify Xattr usage  |
| SG_TEST_USE_AUTH_HANDLER  | Don't use an auth handler by default, but provide a way to override  |


### Running a single unit test against Walrus and Couchbase Server

**Walrus**

```
$ export GOPATH=`pwd`/godeps
$ go test -v -run ^TestChangesAfterChannelAdded$ github.com/couchbase/sync_gateway/...
```

**Couchbase Server**

Install Couchbase Server locally and set the Administrator username / password to match the values in `base/constants.go` for `DefaultCouchbaseAdministrator` and `DefaultCouchbasePassword`

```
$ export GOPATH=`pwd`/godeps
$ SG_TEST_BACKING_STORE=Couchbase go test -v -run ^TestChangesAfterChannelAdded$ github.com/couchbase/sync_gateway/...
```

### Running full set of unit tests against Couchbase Server

Install Couchbase Server locally and set the Administrator username / password to match the values in `base/constants.go` for `DefaultCouchbaseAdministrator` and `DefaultCouchbasePassword`

```
$ ./test_integration.sh
```
