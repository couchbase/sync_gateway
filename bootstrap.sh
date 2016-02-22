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
# - Pull build.sh and test.sh script locally

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

downloadHelperScripts () {

    # If run from CI, then use the commit specified by the CI server,
    # otherwise default to using master
    COMMIT="master"
    if [ "$#" -eq 2 ]; then
	COMMIT="$2"
    fi

    if [ ! -f build.sh ]; then
	echo "Downloading build.sh"
	curl "https://raw.githubusercontent.com/couchbase/sync_gateway/$COMMIT/build.sh" > build.sh
	chmod +x build.sh    
    fi

    if [ ! -f test.sh ]; then
	echo "Downloading test.sh"
	curl "https://raw.githubusercontent.com/couchbase/sync_gateway/$COMMIT/test.sh" > test.sh
	chmod +x test.sh
    fi

}


# ------------------------------------ Main ---------------------------------------

## This script is not intended to be run "in place" from a git clone.
## The next check tries to ensure that's the case
if [ -f "main.go" ]; then
    echo "This script is meant to run outside the clone directory.  See README"
    exit 1
fi 

## If the repo tool is not installed, then download it into current directory
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

## Download helper scripts
downloadHelperScripts "$@"


echo "Bootstrap complete!  Run ./build.sh to build sync gateway, and ./test.sh to run tests"
