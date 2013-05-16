#!/bin/sh -e
# This script runs unit tests in all the subpackages.

export GOPATH="`pwd`:`pwd`/vendor"

go run src/github.com/couchbaselabs/sync_gateway/main.go "$@"
