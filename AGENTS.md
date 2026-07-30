# Couchbase Sync Gateway

Sync Gateway is a horizontally scalable web server that securely manages access and synchronization between Couchbase Lite clients and Couchbase Server. Written in Go, it exposes REST and BLIP (WebSocket-based) APIs.

## Build & Test

```sh
go build -o bin/sync_gateway .   # Community Edition (default)
go test ./...                    # unit tests, in-memory Rosmar backing store
```

- **Enterprise Edition** builds and tests need the `cb_sg_enterprise,cb_sg_devmode` build tags on every Go command, plus SSH access to a private repo — see [docs/BUILD.md](docs/BUILD.md). Don't add these tags unless you specifically intend to test EE.
- **Integration tests** against a real Couchbase Server, the `SG_TEST_*` environment variables, and the bucket pool are covered in [docs/TESTING.md](docs/TESTING.md).
- **Python tooling** (`tools/`): See [tools/AGENTS.md](tools/AGENTS.md).
- **Lint**: CI enforces `.golangci-strict.yml`; reproduce it locally with `pre-commit run golangci-lint --all-files`. Some conventions are enforced here rather than written down — the linter message explains the fix.

### Starting a Couchbase Server for integration tests

`integration-test/start_cbs.py` allocates a single-node cluster via [cbdinocluster](https://github.com/couchbaselabs/cbdinocluster) (Docker + Go are the only prerequisites; it runs `cbdinocluster init` for you on first use). This is what CI uses.

```sh
./integration-test/start_cbs.py   # allocate/reuse a cluster, write ./cbs.env
source cbs.env                    # SG_TEST_COUCHBASE_SERVER_URL, CBS_CLUSTER_ID
SG_TEST_BACKING_STORE=Couchbase go test -count=1 -p 1 -timeout 45m ./...
```

Useful flags: `--env-file` (default `./cbs.env`), `--version` (default 8.0.1), `--nodes`, `--services` (default `kv,n1ql,index`), `--kv-memory-mb`/`--index-memory-mb`, `--tls`, `--purpose`.

The allocated cluster ID is recorded in `.cbdinocluster-sg-cluster-id` in the working directory (gitignored), so re-running the script reuses the running cluster when the version/node count/services still match. Tear down with `go run github.com/couchbaselabs/cbdinocluster@latest remove "$CBS_CLUSTER_ID"` (or `list` / `remove-all`); clusters expire on their own but hold Docker memory until then.

Git: `main` is the current in-development version. Released versions and backports live in `release/x.y.z` branches. Feature branches are named `CBG-xxxx` after the Jira ticket.

## Architecture Overview

The entry point is `main.go`, which calls `rest.ServerMain()`. The runtime object hierarchy is `ServerContext` → `DatabaseContext` → `DatabaseCollection`. Each HTTP request is handled by a short-lived `handler` struct; BLIP (WebSocket) replication uses `BlipSyncContext`/`blipHandler`. Three listener ports: Public (:4984), Admin (:4985), Metrics (:4986).

## Key Concepts

- **Channels** — primary access-control mechanism. A sync function assigns documents to channels; users/roles are granted channel access. Special channels: `!` (all docs) and `*` (public).
- **Sync functions** — JavaScript (ES5) executed per-collection on every doc write. API: `channel()`, `access()`, `role()`, `requireUser()`, `requireAccess()`, `throw()`.
- **Documents & revisions** — each document has a revision tree stored in the `_sync` xattr. Key types: `Document`, `SyncData`, `Body`, `DocumentRevision`.
- **BLIP protocol** — binary WebSocket-based replication protocol used by Couchbase Lite clients.
- **DCP** — streaming mutation feed from Couchbase Server; powers import processing of external writes.
- **Inter-SG replication** — `ActiveReplicator` with Push/Pull for SG-to-SG sync (ISGR).
- **Caching** — `RevisionCache` (LRU, sharded) + `ChannelCache` (per-channel change feeds).
- **Database states** — Offline → Starting → Online → Stopping (+ Resyncing).
- **Configuration** — `StartupConfig` (server-level, file/CLI) vs `DbConfig` (per-database); persistent config stored in Couchbase Server.
- **Editions** — CE is the default; EE is gated behind the `cb_sg_enterprise` build tag. Edition-specific implementations live in paired `*_ce.go` / `*_ee.go` files.

## Conventions

### JSON

Call `base.JSONMarshal`, `base.JSONUnmarshal`, and `base.JSONDecoder` rather than `json.Marshal`, `json.Unmarshal`, and `json.NewDecoder` — EE swaps in `jsoniter` underneath, and direct calls silently bypass that. Importing `encoding/json` for types such as `json.RawMessage` or to implement `json.Marshaler` is fine and common.

### Logging

Use the context-aware wrappers — `base.InfofCtx`, `base.WarnfCtx`, `base.DebugfCtx`, `base.TracefCtx` — so log keys and redaction are applied. Wrap User Data (doc IDs, document contents, usernames, PII) in `base.UD()` and metadata (db names, config keys) in `base.MD()`. Never log credentials, tokens, or keys. Auth flows are basic auth, session-based auth, and OIDC (`auth/oidc.go`).

### Testing

Rosmar (in-memory) is the default backing store.
Assertions use the wrapper packages `github.com/couchbase/sync_gateway/testing/require` and `github.com/couchbase/sync_gateway/testing/assert` (thin wrappers around testify) — `require` to stop the test, `assert` to continue.
REST-level tests use `rest.NewRestTester(t, &rest.RestTesterConfig{...})`; bucket-level tests use `base.GetTestBucket(t)`.
Rosmar is not fully feature-compatible with Couchbase Server, so anything leaning on server behaviour (DCP, XDCR, GSI, xattrs, collections) should be exercised in both modes.

### REST API changes

Changing a handler, query parameter, or response schema means updating the OpenAPI specs in [docs/api/](docs/api/README.md). `redocly lint` and `yamllint` gate this in both pre-commit and CI.

### Xattrs are mandatory (SG 4.0+)

Xattr mode is the only supported mode on `main`. New code must assume xattrs are enabled — do not add `UseXattrs` checks, non-xattr write/read branches, or config surfaces that let xattr mode be turned off.

The one preserved carve-out is **read-side migration of pre-existing non-xattr documents** already in a bucket: that gradual-migration path must keep working so older data is upgraded on access. No new code path should *produce* non-xattr data.

When touching an existing `UseXattrs` check, prefer simplifying toward the xattrs-on branch and deleting the alternative, unless the code is part of the read/migration path above.
