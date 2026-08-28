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

In `-mode dcp` the 1024 vBucket writer goroutines are started 100 ms apart, so there is a **~100 s
ramp** — and that ramp runs to completion *before* the `-duration` timer starts. A `-duration 10m`
run therefore takes about 11m40s wall-clock, and the per-second CSV covers the ramp as well as the
measured period.

`docs_cached_per_sec_steady` is ramp-aware: its window never reaches back past the point the last
writer started, so it is never contaminated. But that also means a short run leaves it a short window
— check `docs_cached_per_sec_steady_window_secs`, and give `-duration` at least `5m` for a full
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
| `-numVBuckets` | `1024` | dcp | vBuckets the DCP client is sized for (worker routing, metadata). **This does not size the generator** — it always starts 1024 vBucket writers, so lowering this does not lower the offered load. |
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
climbing at the end means the run had not settled.

### stdout: end-of-run summary

The same 10 columns as a header row plus one final row of totals, followed by labelled
`name,value` lines:

```
docs_cached_per_sec_overall,183456.123456
docs_cached_per_sec_steady,190123.456789
docs_cached_per_sec_steady_window_secs,300
dcp_received_count,1834561
seqs_cached_per_event,1.000000
```

- **`docs_cached_per_sec_steady`** is the headline throughput number: documents cached per second
  over the last 300 s of the run, which excludes the vBucket ramp. **Use this to compare runs.**
- `docs_cached_per_sec_overall` covers the whole run *including* ramp, so it reads low.
- `docs_cached_per_sec_steady_window_secs` is the window actually available. It is less than 300 on a
  short run — in which case the steady figure still includes ramp and is not comparable — and `0` if
  the run was too short to measure at all.
- `dcp_received_count` is DCP events received; `dcp_caching_count` is sequences cached.
  `seqs_cached_per_event` is their ratio, i.e. how many `processEntry` calls each DCP event costs
  (> 1 when `-rapidUpdateDocs` is on, or when unused sequences are being released). It is 0 in
  `processEntry` mode, which bypasses DCP delivery.

To grab throughput from a script:

```sh
awk -F, '$1=="docs_cached_per_sec_steady"{print $2}' run_summary.csv
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
- The tool exits by cancelling its context and waiting for writer goroutines to drain, so the
  summary appears a moment after `-duration` elapses.
