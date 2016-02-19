#!/bin/sh -e

# This script builds sync gateway using pinned dependencies via the repo tool
#
# It is intended to be run locally via developers (see README for instructions)
# as well as by a CI server.
#
# The basic flow is:
#
# - Detect if repo tool is installed, if not, install it
# - Run 'repo init' to pull Sync Gateway manifest
# - Run 'repo sync' to pull Sync Gateway code and dependencies
# - (optional) for CI server, it can rewrite the manifest for a branch/commit
# - Set GOPATH and call 'go install' to compile and build Sync Gateway binaries

# -------------------------------- Functions ---------------------------------------

# rewriteManifest (): Function which rewrites the manifest according commit passed in arguments.
# This is needed by the CI system in order to test feature branches.
#
# Steps
#   - Fetches manifest with given commit
#   - Updates sync gateway pinned commit to match the given commit (of feature branch)
#   - Overwrites .repo/manifest.xml with this new manifest
#
# It should be run *before* 'repo sync'
#
# This technically doesn't need to run on the master branch, and should be a no-op
# in that case.  I have left that in for now since it enables certain testing.
rewriteManifest () {
    BRANCH="$1"  # ignored for the time being
    COMMIT="$2"
    curl "https://raw.githubusercontent.com/couchbase/sync_gateway/$COMMIT/rewrite-manifest.sh" > rewrite-manifest.sh
    chmod +x rewrite-manifest.sh
    ./rewrite-manifest.sh --manifest-url "https://raw.githubusercontent.com/couchbase/sync_gateway/$2/manifest/default.xml" --project-name "sync_gateway" --set-revision "$COMMIT" > .repo/manifest.xml
}

# ------------------------------------ Main ---------------------------------------

## This script is not intended to be run "in place" from a git clone.
## The next check tries to ensure that's the case
if [ -f "main.go" ]; then
    echo "This script is meant to run outside the clone directory.  See README"
    exit 1
fi 

## Make sure the repo tool is installed, otherwise throw an error
if ! type "repo" > /dev/null; then
    echo "Did not find repo tool, downloading to current directory"
    curl https://storage.googleapis.com/git-repo-downloads/repo > repo
    chmod +x repo
    export PATH=$PATH:.
fi

## If we don't already have a .repo directory, run "repo init"
REPO_DIR=.repo
if [ ! -d "$REPO_DIR" ]; then
    echo "No .repo directory found, running 'repo init'"
    repo init -u "https://github.com/couchbase/sync_gateway.git" -m manifest/default.xml
fi

## If two command line args were passed in (branch and commit), then rewrite manifest.xml
if [ "$#" -eq 2 ]; then
    rewriteManifest "$@"
fi

## Repo Sync
repo sync

## Update the version stamp in the code
SG_DIR=`pwd`/godeps/src/github.com/couchbase/sync_gateway
CURRENT_DIR=`pwd`
cd $SG_DIR
./set-version-stamp.sh
cd $CURRENT_DIR

## Go Tests
echo "Testing code with 'go test' ..."
GOPATH=`pwd`/godeps go test github.com/couchbase/sync_gateway/...

## Go Install
echo "Building code with 'go install' ..."
GOPATH=`pwd`/godeps go install -v github.com/couchbase/sync_gateway/...

echo "Success! Output is godeps/bin/sync_gateway and godeps/bin/sg_accel "

