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


# Default to SG product 
PRODUCT="sg"

# Even when we build sg-accel, we want to grab the sync_gateway
# repo since it includes sg-accel in it's manifest and will
# build *both* sync gateway and sg-accel
TARGET_REPO="https://github.com/couchbase/sync_gateway.git"
			
# By default, will run "repo init" followed by "repo sync".
# If this is set to 1, skips "repo sync" 
INIT_ONLY=0

# Parse the options and save into variables
parseOptions () {

    local product_arg_specified=0
    
    while getopts "p:c:u:t:ihd" opt; do
	case $opt in
	    i)
		# If the -i option is set, skip the "repo sync".
		# Useful if you want to hand-tweak the manifest before running "repo sync"
		INIT_ONLY=1
		;;
	    c)
		COMMIT=$OPTARG
		echo "Using commit: $COMMIT"
		;;
	    d)
		set -x  # print out all commands executed
		set -e  # abort on non-zero exit codes
		echo "Running in debug mode"
		;;
	    h)
		echo "./bootstrap.sh -p sg -c y0pl33g0r425 -i"
		echo "-p <product> to choose which product to bootstrap"
		echo "-c <commit> to start with a particular commit"
		echo "-i to only run repo init and skip repo sync"
		echo "-d to run in debug mode"
		exit 0
		;;
	    p)
		case $OPTARG in
		    sg)
			PRODUCT="sg"
			;;
		    sg-accel)
			PRODUCT="sg-accel"
			;;		
		    *)
			echo "Unknown product"
			exit 1
			;;
		esac
		;;
	    \?)
		echo "Invalid option: -$OPTARG.  Aborting" >&2
		exit 1
		;;
	esac
    done

}


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

    curl "https://raw.githubusercontent.com/couchbase/sync_gateway/$COMMIT/rewrite-manifest.sh" > rewrite-manifest.sh
    chmod +x rewrite-manifest.sh

    MANIFEST_URL="https://raw.githubusercontent.com/couchbase/sync_gateway/$COMMIT/manifest/default.xml"
    PROJECT_NAME="sync_gateway"

    echo "Using manifest: $MANIFEST_URL on commit $COMMIT for project $PROJECT_NAME with username: $GITHUB_USERNAME"
    ./rewrite-manifest.sh --manifest-url "$MANIFEST_URL" --project-name "$PROJECT_NAME" --set-revision "$COMMIT" > .repo/manifest.xml

}

downloadHelperScripts () {

    if [ ! -f build.sh ]; then
	echo "Downloading build.sh"
	curl -s "https://raw.githubusercontent.com/couchbase/sync_gateway/$COMMIT/build.sh" > build.sh
	chmod +x build.sh    
    fi

    if [ ! -f test.sh ]; then
	echo "Downloading test.sh"
	curl -s "https://raw.githubusercontent.com/couchbase/sync_gateway/$COMMIT/test.sh" > test.sh
	chmod +x test.sh
    fi

    if [ ! -f bench.sh ]; then
	echo "Downloading bench.sh"
	curl -s "https://raw.githubusercontent.com/couchbase/sync_gateway/$COMMIT/bench.sh" > bench.sh
	chmod +x bench.sh
    fi

    if [ ! -f snap-manifest.sh ]; then
	echo "Downloading snap-manifest.sh"
	curl -s "https://raw.githubusercontent.com/couchbase/sync_gateway/$COMMIT/snap-manifest.sh" > snap-manifest.sh
	chmod +x snap-manifest.sh
    fi
    
}

repoInit () {

    case $PRODUCT in
	sg)
	    repo init -u "$TARGET_REPO" -m manifest/default.xml
	    ;;
	sg-accel)
	    # Use -g all to pull in sg-accel deps as well
	    repo init -u "$TARGET_REPO" -m manifest/default.xml -g all
	    ;;
	*)
	    echo "Unknown product: $PRODUCT (Aborting)"
	    exit 1
	    ;;
    esac
    echo "Done running repo init"    
}


# --------------------------------------- Main -------------------------------------------

# By default, unless overridden by commit getopt arg, use the master branch everywhere
COMMIT="master"

# Parse the getopt options and set variables
parseOptions "$@"

## This script is not intended to be run "in place" from a git clone.
## Ensure that's the case by making sure there is no main.go file
if [ -f "main.go" ]; then
    echo "This script is meant to run outside the clone directory.  See README"
    exit 1
fi 

## If the repo tool is not installed, then download it into current directory
if ! type "repo" > /dev/null; then
    echo "Did not find repo tool, downloading to current directory"
    curl -s https://storage.googleapis.com/git-repo-downloads/repo > repo
    chmod +x repo
    export PATH=$PATH:.
fi

## If we don't already have a .repo directory, run "repo init"
REPO_DIR=.repo
if [ ! -d "$REPO_DIR" ]; then
    echo "No .repo directory found, running 'repo init' on $TARGET_REPO"
    repoInit "$@"
fi

## If a command line arg was passed in (commit), then rewrite manifest.xml
if [ "$COMMIT" != "master" ]; then
    rewriteManifest "$@"
fi

## Repo sync (unless disabled)
if [ $INIT_ONLY -eq 0 ]; then
    repo sync
fi

## Download helper scripts
downloadHelperScripts "$@"

echo "Bootstrap complete!  Run ./build.sh to build and ./test.sh to run tests"
