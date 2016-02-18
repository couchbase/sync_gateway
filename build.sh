#!/bin/sh -e

# This script builds sync gateway using pinned dependencies.

## This script is not intended to be run "in place" from a git clone.
## The next check tries to ensure that's the case
if [ -f "main.go" ]; then
    echo "This script is meant to run outside the clone directory.  See README"
    exit 1
fi 

## Make sure the repo tool is installed, otherwise throw an error
which repo
if (( $? != 0 )); then
    echo "You must install the repo tool first.  Try running: "
    echo "sudo curl https://storage.googleapis.com/git-repo-downloads/repo > /usr/bin/repo"
    exit 1
fi

## If we don't already have a .repo directory, run "repo init"
REPO_DIR=.repo
if [ ! -d "$REPO_DIR" ]; then
    echo "No .repo directory found, running 'repo init'"
    repo init -u "https://github.com/couchbase/sync_gateway.git" -m manifest/default.xml
fi

## Repo Sync
repo sync

## Update the version
SG_DIR=`pwd`/godeps/src/github.com/couchbase/sync_gateway
CURRENT_DIR=`pwd`
cd $SG_DIR

./set-version-stamp.sh

# Go back to previous dir
cd $CURRENT_DIR

## Go Install
GOPATH=`pwd`/godeps go install "$@" github.com/couchbase/sync_gateway/...

echo "Success! Output is godeps/bin/sync_gateway and godeps/bin/sg_accel"
