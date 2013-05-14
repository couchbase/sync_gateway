#!/bin/sh -e
# This script runs unit tests in all the subpackages.

export GOPATH="`pwd`:`pwd`/vendor"
cd src/github.com/couchbaselabs/sync_gateway

# First build everything so the tests don't complain about out-of-date packages
go test -i

go test ./base ./auth ./channels ./db ./rest $@
