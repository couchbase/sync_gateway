#!/bin/sh -e
# This script runs the gateway, with the supplied arguments.

./go.sh run src/github.com/couchbase/sync_gateway/main.go "$@"
