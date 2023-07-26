# Sync Gateway Prometheus Stat Definition Exporter
This tool exports all the Prometheus Sync Gateway stats definitions to a JSON file. The JSON file is put inside of a gzipped tarball archive. The JSON is prettified by using tab indents and new lines.

## Output
### To File
By default, this tool will output to a file. The archive file outputted is called `sync_gateway_metrics_metadata.tar.gz` however if the string `@PRODUCT_VERSION@` has been replaced in the `main.go` file, the archive file will be called  `sync_gateway_metrics_metadata_PRODUCT_VERSION.tar.gz`. The JSON file inside the archive is always called `sync_gateway_metrics_metadata.json`. This is outputted in the current working directory.

### To Stdout
To output to stdout only and not to a fail, use the `-no-file` flag.

## Logging
The tool may output to stderr when unexpected problems occur. For example, this maybe due to a stat not being initialized or a stat being nil.