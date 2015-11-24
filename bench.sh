#!/bin/sh -e
# This script runs benchmark tests in all the subpackages.

export GOPATH="`pwd`"
cd src/github.com/couchbase/sync_gateway

go test ./... -bench='LoggingPerformance' -benchtime 1m -run XXX

go test ./... -bench='RestApiGetDocPerformance' -cpu 1,2,4 -benchtime 1m -run XXX

#TODO: Add additional benchmark tests here with appropriate parameters
