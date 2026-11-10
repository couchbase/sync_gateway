# cache_perf_tool

A load generator for the Sync Gateway **DCP → change cache → channel cache** write path, run
in-process against an in-memory Rosmar bucket. It drives the real `changeCache` / `channelCache`
code with synthetic mutations, so throughput and contention in the caching pipeline can be measured
without a Couchbase Server cluster, a network, or any client.

It is a measurement harness, not a functional test: nothing is asserted, and the only output is
statistics.

## Build

```sh
go build -o cache_perf_tool ./tools/cache_perf_tool          # Community Edition
```

Enterprise Edition (jsoniter instead of `encoding/json`) needs the build tags and SSH access to the
private EE repo — see [docs/BUILD.md](../../docs/BUILD.md):

```sh
go build -tags cb_sg_enterprise,cb_sg_devmode -o cache_perf_tool_ee ./tools/cache_perf_tool
```

Build the edition you intend to quote numbers for. DCP-mode throughput is lock-bound rather than
parser-bound, so it measures close to the same in both, but document unmarshalling is ~2x faster
under EE.

## Modes

**`-mode dcp`** — the full pipeline. Synthetic `DcpMutation`s are fed through a real DCP client's
worker pool into `ProcessFeedEvent`, so the run exercises DCP event handling, `_sync` xattr
unmarshalling, sequence buffering (pending/skipped), the channel caches, and the notify/broadcast
path. This is the mode that reproduces production shape.

**`-mode processEntry`** — calls `changeCache.processEntry` directly, one goroutine per simulated
Sync Gateway node. It skips DCP delivery and unmarshalling entirely, isolating the sequence-buffering
and channel-cache write cost.

## Running

```sh
./cache_perf_tool -mode dcp -duration 10m -writeDelay 0 \
  -numChannels 1 -totalNumberOfChans 100 \
  -numChangesFeeds 20000 -channelsPerClient 5 \
  2> run.csv 1> run_summary.csv
```

**The two output streams are different things and must be redirected separately:**

- **stderr** — a per-second CSV of cumulative counters, plus interleaved Sync Gateway log lines.
- **stdout** — the end-of-run summary (see below).

In `-mode dcp` writer goroutines are started 100 ms apart, and that ramp runs to completion *before*
the `-duration` timer starts, so the per-second CSV covers the ramp as well as the measured period.
How long the ramp takes depends on `-writerGoroutines`:

- **`-writerGoroutines 0`** (the default) starts one writer per vBucket, so startup takes
  `-numVBuckets` × 100 ms — a **~100 s ramp** at the default 1024. A default `-duration 10m` run
  therefore takes about 11m40s wall-clock. Lowering `-numVBuckets` shortens the ramp proportionally,
  which is useful for smoke tests (64 vBuckets ramps in ~6 s) — but it also lowers the offered load,
  so see the flag table below.
- **`-writerGoroutines N`** starts N writers (plus the hot sync-seq vBucket's own writer, which is not
  staggered), so the ramp is N × 100 ms: `-writerGoroutines 4` ramps in ~400 ms at any vBucket count.

`seqs_cached_per_sec_steady` is ramp-aware: its window never reaches back past the point the last
writer started, so it is never contaminated. But that also means a short run leaves it a short window
— check `seqs_cached_per_sec_steady_window_secs`, and give `-duration` at least `5m` for a full
300 s window.

### Flags

| Flag | Default | Applies to | Meaning |
| --- | --- | --- | --- |
| `-mode` | `processEntry` | — | `dcp` or `processEntry`. |
| `-duration` | `5m` | both | How long to run, e.g. `30s`, `10m`. |
| `-writeDelay` | `0` | both | Comma-separated per-writer delays in ms; `0` means write as fast as possible. In `dcp` mode the list is assigned round-robin across vBuckets; in `processEntry` mode it must have **exactly one entry per `-sgwNodes`**. `processEntry` mode caps each delay at 150 ms. |
| `-sgwNodes` | `1` | processEntry | Number of simulated Sync Gateway nodes (writer goroutines), each with its own sequence-allocation batch. |
| `-batchSize` | `10` | both | Sequences per allocator batch (1–10). |
| `-numChannels` | `1` | both | Channels assigned to each document. `0` gives a no-channel baseline: documents are cached with no channel-cache writes at all. |
| `-totalNumberOfChans` | `1` | both | Size of the channel population documents are spread over. Must be `>= -numChannels`. |
| `-numChangesFeeds` | `0` | dcp | Number of simulated changes feeds. Each is a real `ChangeWaiter` woken by the notify path; on each wake it reads its channels from the channel cache. `0` = no readers. |
| `-channelsPerClient` | `0` | dcp | Channels each simulated feed watches, allocated round-robin from the channel population. |
| `-rapidUpdateDocs` | `false` | dcp | Simulate KV-side deduplication on alternating mutations in every vBucket: allocate 5 sequences, then deliver a single event carrying all of them via `RecentSequences`. |
| `-numDCPWorkers` | `8` | dcp | DCP client worker goroutines. |
| `-numVBuckets` | `1024` | dcp | Number of vBuckets, **1–1024**. Sizes both the DCP client (worker routing, metadata) and the set of vBuckets the generator writes to. With `-writerGoroutines 0` the generator starts **one writer goroutine per vBucket**, so this is also the offered write concurrency; otherwise `-writerGoroutines` sets the writer count. **Runs with different values are not comparable with each other.** See [Running](#running) for the startup ramp. |
| `-writerGoroutines` | `0` | dcp | Number of goroutines driving the data vBuckets, each writing a round-robin share flat out. `0` starts one writer per vBucket. Fewer writers remove generator-side contention so the cache, not the generator, is measured, but they also reduce arrival disorder (fewer allocated-but-undelivered sequences), so **only compare runs at the same value**. Requires `-writeDelay 0`. The hot sync-seq vBucket always keeps its own writer. |
| `-generator` | `template` | dcp | How each document's `_sync` is built. `template` fills per-write values into pre-marshalled templates, allocating only the value the feed receives. `marshal` marshals every document from scratch; its garbage is collected in the same process as the cache, so it lowers measured throughput. |
| `-starChannel` | `false` | both | Activate the `*` channel cache, so every change is also inserted into it, as in production when any user with `*` access has a feed open. Off by default, which leaves the star cache inactive and never inserted into. |
| `-starChannelFeeds` | `0` | dcp | Number of simulated changes feeds watching only `*`, reading the star cache on every wake. Implies `-starChannel`. |
| `-profileInterval` | `0` | both | If > 1 s, enable profiling: a whole-run CPU + fgprof profile, plus heap/mutex/block/goroutine profiles written to the working directory at this interval. Must be less than `-duration`. **Profiling perturbs throughput — do not quote numbers from a profiled run.** |

## Output

### stderr: per-second CSV

One row per second, **10 columns**, all values **cumulative since the start of the run**:

```
timestamp,high_seq_feed,pending_seq_len,high_seq_stable,current_skipped_seq_count,num_skipped_seqs,skipped_sequence_skip_list_nodes,dcp_caching_count,dcp_caching_time,avg_time_per_seq_ms
```

Sync Gateway log lines are written to stderr too, so filter before parsing — the timestamp column is
a 10-digit Unix time:

```sh
grep -aE '^[0-9]{10},' run.csv | awk -F, 'NF==10' > run_data.csv
```

`avg_time_per_seq_ms` (column 10) is a *running mean over the whole run*, so it lags: a value still
climbing at the end means the run had not settled. It is `dcp_caching_time / dcp_caching_count`; both
counters are accumulated at the same point, so it covers exactly the sequences `dcp_caching_count`
counts (see below).

### stdout: end-of-run summary

The same 10 columns as a header row plus one final row of totals, followed by labelled
`name,value` lines:

```
seqs_cached_per_sec_overall,183456.123456
seqs_cached_per_sec_steady,190123.456789
seqs_cached_per_sec_steady_window_secs,300
dcp_received_count,1834561
seqs_per_event,3.000000
seqs_cached_per_event,1.000000
avg_time_per_seq_ms_steady,0.004321
feed_wakes,52000
feed_wakes_with_data,48000
feed_entries_read,910000
feed_newest_entry_age_ms_per_channel_read,1.234567
star_feed_wakes,0
star_feed_wakes_with_data,0
star_feed_entries_read,0
star_feed_newest_entry_age_ms_per_channel_read,0.000000
```

- **`seqs_cached_per_sec_steady`** is the headline throughput number: sequences cached per second
  over the last 300 s of the run, which excludes the vBucket ramp. **Use this to compare runs.**
- `seqs_cached_per_sec_overall` covers the whole run *including* ramp, so it reads low.
- `seqs_cached_per_sec_steady_window_secs` is the window actually available. It is less than 300 on a
  short run — in which case the steady figure still includes ramp and is not comparable — and `0` if
  the run was too short to measure at all.
- `dcp_received_count` is DCP events received (one per `DocChanged`).
- **`seqs_per_event`** is the `processEntry` fan-out: sequences carried by the feed per DCP event,
  `high_seq_feed / dcp_received_count`. `1.0` normally, `~3.0` with `-rapidUpdateDocs` (half the
  events carry 5 sequences). This is the amplification number.
- **`seqs_cached_per_event`** is `dcp_caching_count / dcp_received_count` — the sequences that were
  actually *cached*, per event. It stays at `1.0` even with `-rapidUpdateDocs`, and **the gap between
  it and `seqs_per_event` is the point**: `DCPCachingCount` is incremented at the end of
  `_addToCache`, *after* the `UnusedSequence` and `IsPrincipal` early returns, and the deduplicated
  sequences a rapid-update event replays via `RecentSequences` are marked `UnusedSequence` unless
  they are channel removals. Those sequences are ordered but never cached, so the amplified
  `processEntry` calls are cheap ordering ops that skip the channel dispatch entirely. **Read
  `dcp_caching_count` as sequences cached, not sequences processed.**
- Both ratios are `0` in `processEntry` mode, which bypasses DCP delivery.
- `avg_time_per_seq_ms_steady` is the mean time to cache one sequence over the same steady window as
  `seqs_cached_per_sec_steady`. Unlike `avg_time_per_seq_ms`, which averages over the whole run, it
  excludes the ramp. `0` when there is no steady window.
- The `feed_*` lines are summed over the `-numChangesFeeds` feeds and the `star_feed_*` lines over the
  `-starChannelFeeds` feeds. The cache-side numbers say how fast entries were cached, not whether
  feeds were woken for them, so a change to when notify fires is only visible here:
  - `*_wakes` — wakes that reported changes.
  - `*_wakes_with_data` — of those, wakes where at least one watched channel had a new entry.
    `*_wakes` minus this is the number of wasted wakes.
  - `*_entries_read` — channel-cache entries returned to the feeds.
  - `*_newest_entry_age_ms_per_channel_read` — over per-channel reads that returned at least one
    entry, the mean age of the newest entry since DCP receipt: how stale the feed's view was when it
    read.

  These lines are always printed, as `0` when no feeds of that kind ran, so a parser always finds
  them. Each feed merges its counters once on exit, so they are complete only if every feed shut down
  before the summary was printed.

To grab throughput from a script:

```sh
awk -F, '$1=="seqs_cached_per_sec_steady"{print $2}' run_summary.csv
```

## Caveats

- **Rosmar, not Couchbase Server.** The backing store is in-memory and is only used to create the
  database context; the mutation stream is synthetic. Nothing here measures KV, DCP transport, or
  disk.
- **No client, no BLIP, no HTTP.** `-numChangesFeeds` models the *channel-cache read* a woken feed
  performs — a real feed additionally builds and serialises `ChangeEntry`s, checks user access, and
  reads its late-sequence feed. Feed cost here is a lower bound.
- **Notify cadence changes behaviour.** The broadcast ticker runs at 50 ms normally and 500 ms while
  any sequence is on the skipped list, which moves throughput substantially. A run that accumulates
  skipped sequences part-way through is measuring two different regimes; check
  `num_skipped_seqs` before averaging.
- **`-numVBuckets` is part of the workload, not just a client setting.** With the default
  `-writerGoroutines 0` it is the offered write concurrency. Lowering it makes runs start much faster,
  but it is a different workload — reduce it for smoke tests and debugging, leave it at 1024 for any
  number you intend to compare or publish.
- The tool exits by cancelling its context and waiting for writer goroutines to drain, so the
  summary appears a moment after `-duration` elapses.
