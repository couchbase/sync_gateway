# ISGR v4 writes discard sync function output

Documents written by Inter-Sync-Gateway Replication over BLIP v4 are stored without their channels, access grants, or channel removals. The sync function runs; its result is thrown away.

Reported at [couchbase.com/forums/t/41307](https://www.couchbase.com/forums/t/sync-gateway-4-0-ce-blip-v4-isgr-pull-stores-a-new-document-with-channel-set-null-so-couchbase-lite-never-receives-it/41307).

## Status

| | |
|---|---|
| Affected versions | 4.0.0 through `main` — reproduced on `main` @ `e5326239b` |
| Not | CBG-5713, which is already in `main` |
| Existing ticket | None found |
| Fix | 1 line + comment in `db/crud.go` |

## Symptom

- `_sync.channel_set: null` on the receiving peer, with a valid `sequence`
- Document readable via Admin API, absent from the authenticated `_changes` feed
- Couchbase Lite never receives it
- Any operation forcing a reprocess (resync, external write, read-and-write-back) fixes that one document

## Cause

`documentUpdateFunc` captured `prevCurrentRev` **after** the update callback ran.

For an ISGR write the callback has already made the incoming revision current:

1. `PutExistingCurrentVersion` calls `alignRevTreeHistoryForHLVWrite`
2. which ends in `doc.SetRevTreeID(newRev)` — `db/crud.go:4131`
3. and for ISGR, `newRev` *is* the incoming rev — `db/crud.go:1700`

So `prevCurrentRev == newRevID`, the guard at `db/crud.go:2835` is false, and the block that persists the sync function's output is skipped:

```go
if doc.GetRevTreeID() != prevCurrentRev || createNewRevIDSkipped {
    _, err = doc.updateChannels(ctx, channelSet)          // skipped
    changedAccessPrincipals = doc.Access.updateAccess(...) // skipped
    changedRoleAccessUsers = doc.RoleAccess.updateAccess(...) // skipped
}
```

## Scope

Both directions — push and pull. Both peers set `clientType = SGR2`, so `ISGRWrite` is true on whichever side receives (`db/active_replicator.go:262`, `rest/blip_sync.go:78`).

**Affected** — each verified by a test that fails before the fix and passes after:

| Case | Result before fix |
|---|---|
| New document | `channel_set: null` — invisible to the channel feed |
| Update that changes channels | Stale set kept — new channel never added, **old channel never removed** |
| `access()` / `role()` grants | `access: {}` — dynamic grants silently dropped |
| Tombstone of a live local doc | Channel removal not recorded — doc stays in its channels |
| Pre-upgrade doc (rev tree, no HLV) | Legacy channel set kept — sync function output discarded |

**Not affected** — these paths don't `SetRevTreeID` inside the callback, so the fix is a no-op for them:

- All non-ISGR writes: CBL v4 push, BLIP v3, REST `new_edits=false`, normal REST PUT
- HLV-conflict-resolver path (`db/crud.go:1669`)
- Tombstone over an already-tombstoned doc (`db/crud.go:1601`, `skipHistoryCheck=true`)

> The update case is worse than what was reported: a document moved out of a channel on the source **stays readable in that channel** on the receiver. That is an access-control consequence, not just a visibility one.

## Fix

Capture the rev before the callback, and use it only for the channel/access decision:

```go
preCallbackRev := doc.GetRevTreeID()   // new, before callback
newDoc, ... := callback(doc)
...
if doc.GetRevTreeID() != preCallbackRev || createNewRevIDSkipped {
```

`prevCurrentRev` stays where it is — `storeOldBodyInRevTreeAndUpdateCurrent` genuinely needs the post-callback value.

> Simply moving the existing capture earlier — the fix implied in the forum post — **breaks other paths**. It panics `TestXattrSGWriteOfNonImportedDoc` with `rev id ... not found`.

For non-ISGR paths `preCallbackRev == prevCurrentRev`, since nothing else in a callback calls `SetRevTreeID`.

## Why no test caught it

Three independent layers of masking:

| # | Masking | Where |
|---|---|---|
| 1 | Every change is added to the `*` channel cache regardless of its channel set | `db/channel_cache.go:257` |
| 2 | Test peers create their user with `["*"]` access | `rest/utilities_testing_isgr.go:237` |
| 3 | Admin `_changes` has no user, so it reads the star channel | `db/changes.go:1425` |

And the coverage simply wasn't there:

- 90 of 92 `WaitForChanges` calls in `replicatortest` used the admin port
- `.Channels` appeared in **zero** assertions across `replicatortest` and `topologytest`
- `ISGRWrite: true` appeared in **zero** db-package tests — that branch had no unit coverage

`TestActiveReplicatorPullBasic` / `PushBasic` were one assertion away: they already write `"channels":[...]` into the doc and fetch the received doc with `DocUnmarshalAll`, then stop at the body.

## Test changes

**Updated** — these should have caught it, and now do:

| Test | Change |
|---|---|
| `TestActiveReplicatorPullBasic` | Assert receiver persisted the doc's channel |
| `TestActiveReplicatorPushBasic` | Assert receiver persisted the doc's channel |

**Added:**

| Test | Package | Covers |
|---|---|---|
| `TestPutExistingCurrentVersionISGRNewDocChannels` | `db` | New ISGR doc gets channels |
| `TestPutExistingCurrentVersionISGRAccessGrant` | `db` | `access()` grant persisted |
| `TestPutExistingCurrentVersionISGRTombstoneChannelRemoval` | `db` | Tombstone records removal |
| `TestPutExistingCurrentVersionISGRLegacyDocChannels` | `db` | Pre-upgrade doc with no HLV |
| `TestActiveReplicatorPullNewDocChannels` | `replicatortest` | Pull e2e, incl. public `_changes` |
| `TestActiveReplicatorPushNewDocChannels` | `replicatortest` | Push e2e, incl. public `_changes` |
| `TestActiveReplicatorPullUpdatedDocChannels` | `replicatortest` | Channel change on update |

The e2e tests run under both subprotocols. `revtree` (v3) passes throughout; `versionVector` (v4) fails before the fix — matching the report.

## Verification

- All 11 tests fail before the fix, pass after
- `./db/... ./rest/... ./topologytest/` green on this branch and on `main`
