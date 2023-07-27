# Sync Gateway Prometheus Stat Definition Exporter
This tool exports all the Prometheus Sync Gateway stats definitions to a JSON file. The JSON is prettified by using tab indents and new lines.

## Output
### To File
By default, this tool will output to a file. named `metrics_metadata.json`. This is outputted in the current working directory.

Use the `-output` flag to change the output location and name.

### To Stdout
To output to stdout only and not to a fail, use the `-no-file` flag. The `-output` flag will be ignored when this is `true`.

## Logging
The tool may output to stderr when unexpected problems occur. For example, this maybe due to a stat not being initialized or a stat being nil.

## JSON format
The JSON is an array of objects in the current format:
```json
[
  {
    "name": "string",
    "unit": "string",
    "labels": ["strings"],
    "help": "string",
    "format": "string",
    "type": "string"
  }
]
```

- `name` is the fully qualified name of the stat.
- `unit` is what unit the stat uses such as bytes or nanoseconds. If the stat has no units, then this will be omitted.
- `labels` is a list of label keys that Prometheus uses to uniquely distinguish between the same stat being declared multiple times. For example, `databases`, `collections` etc. This is omitted if the stat has no labels.
- `help` contains a description of what the stat does.
- `format` is the format of the value the stat stores such as int, float, duration, etc.
- `type` is how Prometheus shows the stat such as it being a such as counter, gauge, etc.

## Sample output
```json
[
  {
    "name": "sgw_resource_utilization_admin_net_bytes_recv",
    "unit": "bytes",
    "help": "The total number of bytes received (since node start-up) on the network interface to which the Sync Gateway api.admin_interface is bound. By default, that is the number of bytes received on 127.0.0.1:4985 since node start-up.",
    "format": "int",
    "type": "counter"
  },
  {
    "name": "sgw_cache_high_seq_cached",
    "labels": [
      "database"
    ],
    "help": "The highest sequence number cached. Note: There may be skipped sequences lower than high_seq_cached.",
    "format": "int",
    "type": "counter"
  },
  {
    "name": "sgw_shared_bucket_import_import_partitions",
    "unit": "total count",
    "labels": [
      "database"
    ],
    "help": "The total number of import partitions.",
    "format": "int",
    "type": "gauge"
  },
  {
    "name": "sgw_collection_sync_function_count",
    "unit": "total count",
    "labels": [
      "collection",
      "database"
    ],
    "help": "The total number of times that the sync_function is evaluated for this collection.",
    "format": "int",
    "type": "counter"
  },
  {
    "name": "sgw_resource_utilization_process_cpu_percent_utilization",
    "unit": "%",
    "help": "The CPU’s utilization as percentage value. The CPU usage calculation is performed based on user and system CPU time, but it does not include components such as iowait. The derivation means that the values of process_cpu_percent_utilization and %Cpu, returned when running the top command, will differ",
    "format": "float",
    "type": "gauge"
  },
  {
    "name": "sgw_cache_chan_cache_hits",
    "unit": "hits",
    "labels": [
      "database"
    ],
    "help": "The total number of channel cache requests fully served by the cache. This metric is useful in calculating the channel cache hit ratio: channel cache hit ratio = chan_cache_hits / (chan_cache_hits + chan_cache_misses)",
    "format": "int",
    "type": "counter"
  }
]
```