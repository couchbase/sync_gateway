# Couchbase Sync Gateway

Sync Gateway is a horizontally scalable web server that securely manages access and synchronization between Couchbase Lite clients and Couchbase Server. Written in Go, it exposes REST and BLIP (WebSocket-based) APIs.

## Build & Test

- Build: `go build -o bin/sync_gateway .`
- Building or testing EE (requires private repo SSH access) must use the `cb_sg_enterprise, cb_sg_devmode` build tags for all Go commands. E.g: `go build -tags cb_sg_enterprise,cb_sg_devmode .`
- Run all unit tests (Rosmar/in-memory, no Couchbase Server): `go test ./...`
- Run a single test: `go test -run ^TestFunctionName$ github.com/couchbase/sync_gateway/rest`
- Run a single package: `go test github.com/couchbase/sync_gateway/rest`
- Run integration tests (requires local Couchbase Server): `SG_TEST_BACKING_STORE=Couchbase go test ./...`
- Run a specific benchmark: `go test -bench=^BenchmarkSomething$ -run=- ./...`
- Lint: `golangci-lint run`
- Python tooling lint/typecheck: `uv run ruff check tools/ tools-tests/` and `uv run mypy`
- Python tests: `uv run pytest`

## Architecture Overview

The entry point is `main.go`, which calls `rest.ServerMain()`. The runtime object hierarchy is `ServerContext` → `DatabaseContext` → `DatabaseCollection`. Each HTTP request is handled by a short-lived `handler` struct; BLIP (WebSocket) replication uses `BlipSyncContext`/`blipHandler`. Three listener ports: Public (:4984), Admin (:4985), Metrics (:4986).

| Package | Purpose |
|---------|---------|
| `rest/` | REST API handlers, routing (gorilla/mux), config management, server context |
| `db/` | Core database logic: CRUD, documents, revisions, changes feed, BLIP sync, replication, import |
| `base/` | Shared utilities: logging, bucket abstraction, stats, DCP, test infrastructure |
| `auth/` | Authentication: users, roles, sessions, OIDC, JWT |
| `channels/` | Channel mapping, sync function runner, channel sets |
| `xdcr/` | Cross-datacenter replication (CBS and Rosmar backends) |
| `topologytest/` | Multi-actor topology integration tests |
| `service/` | OS service install/upgrade scripts |
| `tools/` | Python tooling: `sgcollect.py` (log collection), `password_remover.py` |

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

## Key Files

| File | Contents |
|------|----------|
| `rest/handler.go` | REST handler struct, privilege levels |
| `rest/routing.go` | Route registration (Gorilla mux) |
| `rest/server_context.go` | ServerContext |
| `rest/config.go` | DbConfig |
| `rest/config_startup.go` | StartupConfig |
| `db/database.go` | DatabaseContext, database states |
| `db/database_collection.go` | DatabaseCollection, DatabaseCollectionWithUser |
| `db/document.go` | Document, SyncData, revision structures |
| `db/blip_sync_context.go` | BlipSyncContext (BLIP session) |
| `db/blip_handler.go` | blipHandler (per-message BLIP handling) |
| `db/active_replicator.go` | ActiveReplicator (inter-SG replication) |
| `db/changes.go` | Changes feed |
| `channels/sync_runner.go` | Sync function execution |
| `base/dcp_receiver.go` | DCP feed processing |

## Editions: CE vs EE

Sync Gateway ships two editions controlled by the `cb_sg_enterprise` build tag:
- **CE** (Community Edition): default, no build tag needed. Files: `*_ce.go`
- **EE** (Enterprise Edition): `go build -tags cb_sg_enterprise`. Files: `*_ee.go`

Never add the `cb_sg_enterprise` tag to test commands unless intentionally testing EE features.

## Conventions & Patterns

### Go style
- Use the Go version declared in `go.mod`. Tabs for indentation (standard `goimports`).
- 120-char soft line limit (`.editorconfig`).
- Use `github.com/stretchr/testify` for assertions (`require` for fatal, `assert` for non-fatal).
- Use stdlib and base utilities where possible unless a module is already referenced by `go.mod`.
- JSON marshaling uses `base.JSONMarshal`, `base.JSONUnmarshal`, and `base.JSONDecoder` wrappers — never `encoding/json` directly. (EE builds use `jsoniter` under the hood; CE uses the standard library.)

### Logging
- Use `base.InfofCtx`, `base.WarnfCtx`, `base.DebugfCtx`, `base.TracefCtx` — never `fmt.Printf` or `log.Printf`.
- Wrap User Data (doc IDs, usernames, PII) with `base.UD()`.
- Wrap Metadata (db names, config keys) with `base.MD()`.

### Testing patterns
- Each top-level package has a `main_test.go` with `TestMain` that sets up `TestBucketPool`.
- Default test backing store is **Rosmar** (in-memory). Set `SG_TEST_BACKING_STORE=Couchbase` for CBS.
- REST tests use `rest.NewRestTester(t, &RestTesterConfig{...})`.
- Bucket tests use `base.GetTestBucket(t)`.
- Tests run with `-shuffle=on` by default.
- Test timeout defaults to 20 minutes per package.

### REST API changes
- When modifying REST handlers, query parameters, or response schemas, update the OpenAPI specs in `docs/api/`.

## Security

- NEVER log credentials, tokens, or keys.
- User Data must be redacted via `base.UD()` in all log output.
- System/Metadata must use `base.MD()`.
- Auth flows include basic auth, session-based auth, OIDC (`auth/oidc.go`)

## Test Environment Variables

These variables are read by Go test code via `os.Getenv` and work with `go test`.

**Core configuration:**

| Variable | Purpose | Default |
|----------|---------|---------|
| `SG_TEST_BACKING_STORE` | Set to `Couchbase` to test against a real CBS cluster instead of Rosmar | Rosmar (in-memory) |
| `SG_TEST_COUCHBASE_SERVER_URL` | Couchbase Server URL for integration tests | `couchbase://localhost` |

**Integration test tuning** (only relevant with `SG_TEST_BACKING_STORE=Couchbase`):

| Variable | Purpose | Default |
|----------|---------|---------|
| `SG_TEST_BUCKET_POOL_SIZE` | Number of buckets to pre-create in the test pool. Single tests usually only need one or two buckets and will speed up testing. Leave this as default if running more than one test. | `4` |

## Gotchas

- EE build requires SSH access to `github.com/couchbaselabs/go-fleecedelta` private repo.
- Integration tests must always force `-count=1 -p 1` (serial execution) to avoid cross-package interference.
- Rosmar is an in-memory Couchbase bucket simulator used for unit tests; it lives in `github.com/couchbaselabs/rosmar`.
  - It does not have 100% feature-parity, so any features heavily using Couchbase-Server should be tested in both modes to ensure compatibility.
