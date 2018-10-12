

# Create bucket "database-1" by hand
# TODO: add CLI command for this

# Create general config in metakv
sg config metakv set /mobile/gateway/config/general --input-file-path /Users/tleyden/Development/sync_gateway/godeps/src/github.com/couchbase/sync_gateway/examples/mercury/metakv-general.json

# Create listener config in metakv
sg config metakv set /mobile/gateway/config/listener --input-file-path /Users/tleyden/Development/sync_gateway/godeps/src/github.com/couchbase/sync_gateway/examples/mercury/metakv-listener.json

# Create database config in metakv
sg config metakv set /mobile/gateway/config/databases/database-1 --input-file-path /Users/tleyden/Development/sync_gateway/godeps/src/github.com/couchbase/sync_gateway/examples/mercury/metakv-database-1.json
