
#!/bin/sh -e

# Copyright 2015-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# This script runs benchmark tests in all the subpackages.

if [ -d "godeps" ]; then
  export GOPATH=`pwd`/godeps
fi

go test github.com/couchbase/sync_gateway/... -bench='LoggingPerformance' -benchtime 1m -run XXX

go test github.com/couchbase/sync_gateway/... -bench='RestApiGetDocPerformance' -cpu 1,2,4 -benchtime 1m -run XXX

go test github.com/couchbase/sync_gateway/... -bench='RestApiPutDocPerformanceDefaultSyncFunc' -benchtime 1m -run XXX

go test github.com/couchbase/sync_gateway/... -bench='RestApiPutDocPerformanceExplicitSyncFunc' -benchtime 1m -run XXX
