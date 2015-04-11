#!/bin/sh -e
# This script runs benchmark tests in all the subpackages.

export GOPATH="`pwd`"
cd src/github.com/couchbase/sync_gateway

go test ./... -bench='.*' -benchtime 1m -run XXX
