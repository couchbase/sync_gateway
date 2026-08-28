# Couchbase Sync Gateway

Sync Gateway is a horizontally scalable web server that securely manages access and synchronization between Couchbase Lite clients and Couchbase Server. Written in Go, it exposes REST and BLIP (WebSocket-based) APIs.

## Build & Test

```sh
go build -tags cb_sg_enterprise,cb_sg_devmode -o bin/sync_gateway .   # Enterprise Edition
go test -tags cb_sg_enterprise,cb_sg_devmode ./...                    # unit tests, in-memory Rosmar backing store
```

- **Enterprise Edition is the default** — pass the `cb_sg_enterprise,cb_sg_devmode` build tags on every Go command unless you were specifically asked to test Community Edition. EE also needs SSH access to a private repo — see [docs/BUILD.md](docs/BUILD.md).
- **Community Edition** is what you get with no build tags: `go build -o bin/sync_gateway .` / `go test ./...`. Only drop the tags when CE behaviour is what's being tested.
- **If the EE dependencies can't be fetched** (no SSH access to the private repo), fall back to CE and say explicitly that the results are CE-only.
- **CI runs both**: GitHub Actions (`.github/workflows/ci.yml`) builds and tests CE; Jenkins (`Jenkinsfile`, `jenkins-integration-build.sh`) runs EE.
- **Integration tests** against a real Couchbase Server are covered in [docs/TESTING.md](docs/TESTING.md): starting a local cluster with `integration-test/start_cbs.py`, the `SG_TEST_*` environment variables, and the bucket pool.
- **Python tooling** (`tools/`): See [tools/AGENTS.md](tools/AGENTS.md).
- **Lint**: CI enforces `.golangci-strict.yml`; reproduce it locally with `pre-commit run golangci-lint --all-files`. Some conventions are enforced here rather than written down — the linter message explains the fix.
- **CI tools** are pinned by `tool` directives in `go.mod` and run as `go tool <name>` — `gotestsum`, `addlicense`, `goimports`, `goveralls`. No install step is needed. `cbdinocluster` is pinned in its own module: `go -C integration-test/tools tool cbdinocluster`.

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
- **Editions** — EE is gated behind the `cb_sg_enterprise` build tag; CE is what builds without it. Edition-specific implementations live in paired `*_ce.go` / `*_ee.go` files.

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
