Testing Sync Gateway
====================

Sync Gateway's Go tests run against one of two backing stores. Which one you pick changes how long the
tests take, which features are exercised, and which flags you need.

Backing stores
--------------

**Rosmar (default)** — an in-memory Couchbase bucket simulator ([couchbaselabs/rosmar](https://github.com/couchbaselabs/rosmar)).
No external dependencies, fast, and what runs by default:

```sh
go test ./...
```

Rosmar does not have 100% feature parity with Couchbase Server. Anything that leans heavily on server
behaviour — DCP, XDCR, GSI/N1QL queries, xattr semantics, collections — should be tested in both modes
before you trust it.

**Couchbase Server** — requires a local cluster:

```sh
SG_TEST_BACKING_STORE=Couchbase go test -count=1 -p 1 -timeout 45m ./...
```

Three flags matter here and are easy to get wrong:

- `-count=1` disables the test result cache.
- `-p 1` runs packages serially. Integration tests share a pool of real buckets, so parallel packages
  interfere with each other.
- `-timeout 45m` — the default is 10 minutes per package, which is not enough against a real server.

Tests run with `-shuffle=on` by default (see `.github/workflows/ci.yml`), so ordering dependencies
between tests will surface as intermittent failures.

Starting a Couchbase Server
---------------------------

`integration-test/start_cbs.py` allocates a single-node cluster via
[cbdinocluster](https://github.com/couchbaselabs/cbdinocluster) (Docker + Go are the only
prerequisites; it runs `cbdinocluster init` for you on first use). This is what CI uses.

```sh
./integration-test/start_cbs.py   # allocate/reuse a cluster, write ./cbs.env
source cbs.env                    # SG_TEST_COUCHBASE_SERVER_URL, CBS_CLUSTER_ID
SG_TEST_BACKING_STORE=Couchbase go test -count=1 -p 1 -timeout 45m ./...
```

Useful flags: `--env-file` (default `./cbs.env`), `--version` (default 8.0.1), `--nodes`,
`--services` (default `kv,n1ql,index`), `--kv-memory-mb`/`--index-memory-mb`, `--tls`, `--purpose`.

The allocated cluster ID is recorded in `.cbdinocluster-sg-cluster-id` in the working directory
(gitignored), so re-running the script reuses the running cluster when the version/node
count/services still match. Tear down with
`go run github.com/couchbaselabs/cbdinocluster@latest remove "$CBS_CLUSTER_ID"` (or `list` /
`remove-all`); clusters expire on their own but hold Docker memory until then.

Capturing output
----------------

Integration runs produce a lot of output. Redirect it to a file and read only the parts you need:

```sh
SG_TEST_BACKING_STORE=Couchbase go test -count=1 -p 1 -timeout 45m ./... > /tmp/sg-test.log 2>&1
grep -n "^--- FAIL\|^FAIL\|^panic:" /tmp/sg-test.log
```

The bucket pool
---------------

In integration mode a pool of buckets is created up front and leased to tests as they run
(`base/main_test_bucket_pool.go`). A test that cannot get a bucket waits
`waitForReadyBucketTimeout` — **2 minutes** — before failing.

This produces a distinctive symptom: if a batch of tests all fail after almost exactly 120 seconds,
the problem is usually pool starvation (too few buckets, or buckets held by preserved failures), not
the tests themselves.

When running a single test, shrinking the pool avoids the cost of preparing buckets you will not use:

```sh
SG_TEST_BACKING_STORE=Couchbase SG_TEST_BUCKET_POOL_SIZE=1 go test -count=1 -run TestFoo ./db/
```

Leave `SG_TEST_BUCKET_POOL_SIZE` at its default when running more than one test.

Environment variables
---------------------

Read via `os.Getenv` by test code, so they work with plain `go test`. Defined in `base/constants.go`,
`base/main_test_bucket_pool_config.go`, and `testing/sgtest/sgtest.go`.

### Choosing a backing store

| Variable | Purpose | Default |
|---|---|---|
| `SG_TEST_BACKING_STORE` | Set to `Couchbase` to test against a real cluster instead of Rosmar | `Walrus` (Rosmar, in-memory) |
| `SG_TEST_COUCHBASE_SERVER_URL` | Couchbase Server connection string | `couchbase://127.0.0.1` |
| `SG_TEST_ROSMAR_URL` | Override the Rosmar URL | in-memory |
| `SG_TEST_USERNAME` | Cluster admin username | `Administrator` |
| `SG_TEST_PASSWORD` | Cluster admin password | `password` |

### Bucket pool (integration mode only)

| Variable | Purpose | Default |
|---|---|---|
| `SG_TEST_BUCKET_POOL_SIZE` | Buckets pre-created for the pool. Set to `1` for single-test runs | `4` |
| `SG_TEST_COLLECTION_POOL_SIZE` | Collections prepared per bucket | `2` |
| `SG_TEST_BUCKET_QUOTA_MB` | Memory quota per bucket | `200` |
| `SG_TEST_BUCKET_NUM_REPLICAS` | Replica count for created buckets | `0` |
| `SG_TEST_BUCKET_POOL_PRESERVE` | Keep buckets from failed tests for inspection. Preserved buckets leave the pool, so remaining tests are skipped once all are consumed | unset |
| `SG_TEST_BUCKET_POOL_DEBUG` | Verbose logging from the pooling framework | unset |
| `SG_TEST_USE_EXISTING_BUCKET` | Use a named existing bucket instead of the pool (single bucket only) | unset |
| `SG_TEST_SKIP_SERVER_VERSION_CHECK` | Allow running against an unsupported server version | unset |

### Feature toggles

| Variable | Purpose | Default |
|---|---|---|
| `SG_TEST_USE_GSI` | Set `false` to use views instead of GSI | `true` |
| `SG_TEST_USE_DEFAULT_COLLECTION` | Run against `_default._default` rather than named collections | unset |
| `SG_TEST_USE_SYSTEM_METADATA_COLLECTION` | Store SG metadata in `_system._mobile` | `false` |
| `SG_TEST_DISABLE_REV_CACHE` | Disable the revision cache | unset |
| `SG_TEST_TLS_SKIP_VERIFY` | Skip TLS certificate verification | `true` |
| `SG_TEST_USE_AUTH_HANDLER` | Use an auth handler | unset |

### Diagnostics

| Variable | Purpose | Default |
|---|---|---|
| `SG_TEST_LOG_LEVEL` | Global log level for all tests | unset |
| `SG_TEST_GOROUTINE_DUMP` | Capture a goroutine pprof profile at the end of each package and log its location | unset |
| `SG_TEST_PROFILE_FREQUENCY` | Capture pprof profiles at this interval | unset |

Enterprise Edition
------------------

EE tests need build tags. See [BUILD.md](BUILD.md).

Writing tests
-------------

- Assertions use the wrapper packages `github.com/couchbase/sync_gateway/testing/require` and `github.com/couchbase/sync_gateway/testing/assert` (thin wrappers around [testify](https://github.com/stretchr/testify)): `require` to stop the test, `assert` to
  continue.
- REST-level tests use `rest.NewRestTester(t, &rest.RestTesterConfig{...})`.
- Bucket-level tests use `base.GetTestBucket(t)`.
