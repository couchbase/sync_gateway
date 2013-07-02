#!/bin/sh -e

# This script builds the sync gateway. You can't just run "go install"
# directly, because we need to tell the Go compiler how to find the
# dependent packages (in vendor) and the gateway source code (in src)
# by setting $GOPATH.

# populate version info before each build
CURRENT_BRANCH=`git status -b -s | sed q | sed 's/## //'`
CURRENT_COMMIT=`cat .git/refs/heads/$CURRENT_BRANCH | sed 's/\n//'`
sed -i -e 's/CurrentBranch.*=.*/CurrentBranch     = "'$CURRENT_BRANCH'"/' ./src/github.com/couchbaselabs/sync_gateway/rest/config.go
sed -i -e 's/CurrentCommit.*=.*/CurrentCommit     = "'$CURRENT_COMMIT'"/' ./src/github.com/couchbaselabs/sync_gateway/rest/config.go

# build
export GOBIN="`pwd`/bin"
./go.sh install -v github.com/couchbaselabs/sync_gateway
echo "Success! Output is bin/sync_gateway"
