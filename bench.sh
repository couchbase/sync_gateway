#!/bin/sh -e
# This script runs benchmark tests in all the subpackages.

go test ./... -bench='LoggingPerformance' -benchtime 1m -run XXX

go test ./... -bench='RestApiGetDocPerformance' -cpu 1,2,4 -benchtime 1m -run XXX


