#!/bin/sh -e
# This script runs the gateway, with the supplied arguments.

ulimit -n 5000
./go.sh run src/github.com/couchbaselabs/sync_gateway/main.go "$@"
