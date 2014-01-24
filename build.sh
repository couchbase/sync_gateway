#!/bin/sh -e

# This script builds the sync gateway. You can't just run "go install"
# directly, because we need to tell the Go compiler how to find the
# dependent packages (in vendor) and the gateway source code (in src)
# by setting $GOPATH.

export GOBIN="`pwd`/bin"
cp utils/admin_bundle.go src/github.com/couchbaselabs/sync_gateway/rest/
./go.sh install -v github.com/couchbaselabs/sync_gateway
echo "Success! Output is bin/sync_gateway"
