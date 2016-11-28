
Building From Source (via repo)
-------------------------------

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

**Snap dependencies to manifest**

If you switch to a different Sync Gateway commit, for example after doing a `git pull` or a `git checkout branch`, you should re-pin (aka "snap") all of your dependencies to the versions specified in the [manifest.xml](https://github.com/couchbase/sync_gateway/blob/master/manifest/default.xml)

```bash
$ ./snap-manifest.sh
```


Build via go get w/ dependency pinning
--------------------------------------

See the [Build Extra](BUILD_EXTRA.md) for instructions on how to build via `go get` (as opposed to `repo`) with dependency pinning.
