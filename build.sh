#!/bin/bash

# This script builds the sync gateway. You can't just run "go install" directly, because we
# need to tell the Go compiler how to find the dependent packages (in vendor) and the gateway
# source code (in src) by setting $GOPATH.

set -e
export GOPATH="`pwd`:`pwd`/vendor"
go install github.com/couchbaselabs/sync_gateway
echo "Success! Output is bin/sync_gateway"
