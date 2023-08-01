# Sync Gateway Prometheus Stat Definition Exporter
This tool exports all the Prometheus Sync Gateway stats definitions to a JSON file. The JSON is prettified by using tab indents and new lines.

## Output
### To File
By default, this tool will output to a file. named `metrics_metadata.json`. This is outputted in the current working directory.

Use the `-output` flag to change the output location and name.

### To Stdout
To output to stdout only and not to a fail, use the `-no-file` flag. The `-output` flag will be ignored when this is `true`.

## Logging
The tool may output to stderr when unexpected problems occur. For example, this maybe due to a stat not being initialized, a stat being nil, or an unexpected problem writing to a file.

## JSON format
The JSON is an array of objects in the current format:
```json
[
  {
    "name": "string",
    "unit": "string",
    "labels": ["strings"],
    "help": "string",
    "added": "string",
    "deprecated": "string",
    "stability": "string",
    "type": "string"
  }
]
```

- `name` is the fully qualified name of the stat.
- `unit` is what unit the stat uses such as `bytes` or `nanoseconds`. If the stat has no units, then this will be omitted.
- `labels` is a list of label keys that Prometheus uses to uniquely distinguish between the same stat being declared multiple times. For example, `databases`, `collections` etc. This is omitted if the stat has no labels.
- `help` contains a description of what the stat does.
- `added` is the Sync Gateway version the stat got added.
- `deprecated` is the Sync Gateway version that this stat has been deprecated in.
- `stability` is what the current stability of the stat is such as `committed`, `volatile`, or `internal`.
- `type` is the Prometheus type that changes the way the stat is shown such as `counter`, `gauge`, etc.

## Sample output
```json
[
  {
    "name": "sgw_database_public_rest_bytes_read",
    "unit": "bytes",
    "labels": [
      "database"
    ],
    "help": "The total amount of bytes read over the public REST api",
    "added": "3.2.0",
    "stability": "volatile",
    "type": "counter"
  },
  {
    "name": "sgw_database_num_replications_total",
    "labels": [
      "database"
    ],
    "help": "The total number of replications created since Sync Gateway node startup.",
    "added": "3.0.0",
    "stability": "committed",
    "type": "counter"
  },
  {
    "name": "sgw_resource_utilization_go_memstats_heapidle",
    "unit": "bytes",
    "help": "HeapIdle is bytes in idle (unused) spans. Idle spans have no objects in them. These spans could be (and may already have been) returned to the OS, or they can be reused for heap allocations, or they can be reused as stack memory. HeapIdle minus HeapReleased estimates the amount of memory that could be returned to the OS, but is being retained by the runtime so it can grow the heap without requesting more memory from the OS. If this difference is significantly larger than the heap size, it indicates there was a recent transient spike in live heap size.",
    "added": "3.0.0",
    "stability": "committed",
    "type": "gauge"
  },
  {
    "name": "sgw_collection_sync_function_count",
    "labels": [
      "database",
      "collection"
    ],
    "help": "The total number of times that the sync_function is evaluated for this collection.",
    "added": "3.0.0",
    "stability": "committed",
    "type": "counter"
  }
]
```