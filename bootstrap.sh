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

set -x  # print out all commands executed
set -e  # abort on non-zero exit codes

# By default, will run "repo init" followed by "repo sync".  If this is set to 1, skips "repo sync" 
INIT_ONLY=0

# Parse the options and save into variables
parseOptions () {

    local product_arg_specified=0
    local github_username_specified=0
    local github_api_token_specified=0    
    
    while getopts "p:c:u:t:i" opt; do
	case $opt in
	    i)
		# If the -i option is set, skip the "repo sync".
		# Useful if you want to hand-tweak the manifest before running "repo sync"
		INIT_ONLY=1
		;;
	    u)
		github_username_specified=1
		GITHUB_USERNAME=$OPTARG
		echo "Using github username: $GITHUB_USERNAME"
		;;
	    t)
		github_api_token_specified=1    
		GITHUB_API_TOKEN=$OPTARG
		;;
	    c)
		COMMIT=$OPTARG
		echo "Using commit: $COMMIT"
		;;
	    p)
		product_arg_specified=1
		case $OPTARG in
		    sg)
			PRODUCT="sg"
			TARGET_REPO="https://github.com/couchbase/sync_gateway.git"
			;;
		    sg-accel)
			PRODUCT="sg-accel"
			# Even when we build sg-accel, we want to grab the sync_gateway
			# repo since it includes sg-accel in it's manifest and will
			# build *both* sync gateway and sg-accel
			TARGET_REPO="https://github.com/couchbase/sync_gateway.git"
			;;		
		    *)
			echo "Unknown product.  Aborting."
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

    if [ $product_arg_specified -eq 0 ]; then
	echo "You must specify a product.  Aborting."
	exit 1
    fi

    if [ $github_username_specified -eq 1 ] && [ $github_api_token_specified -eq 0 ]; then
	echo "If specifying a github username, you must also specify a github api token"
	exit 1
    fi

    if [ $github_username_specified -eq 0 ] && [ $github_api_token_specified -eq 1 ]; then
	echo "If specifying a github api token, you must also specify a github username"
	exit 1
    fi    
	
}

# Super dirty hack to get rewrite-manifest.sh python script on file system
# since it is awkward to download it from github via curl.
# The deeper fix is to rewrite this boostrap.sh in python and just
# call the code directly (rewrite-manifest.sh needs to parse xml)
emitRewriteManifestPythonScript() {

    cat > rewrite-manifest.sh <<EOF
#!/usr/bin/env python

# This script updates the manifest created from a 'repo init -u <url>'
# command with a *different* manifest, presumably before 'repo sync'
# has been run.
#
# The purpose is to build from a manifest on a feature branch, for example
# as part of validating a github pull request.
#
# Here are the actions performed:
# 
# 1. Fetches manifest from the given url
# 2. Updates the given project revision to match the given commit
# 3. Emits modified manifest to stdout
#
# Usage:
#
#     rewrite-manifest --manifest-url http://yourwebsite.co/manifest.xml --project-name your-project --set-revision new-sha > .repo/manifest.xml
#
# If your existing manifest contained a project entry like:
#
#     <project name="your-project" path="somepath" remote="someremote"/> 
#
# the above command would modify it to be:
#
#     <project name="your-project" path="somepath" revision="new-sha" remote="someremote"/> 
#
# and emit it to stdout, which you can redirect to overwrite your .repo/manifest.xml file

import optparse
import xml.etree.ElementTree as ET
import urllib2
import sys
import base64

def parse_args():
    """
    Parse command line args and return a tuple
    """
    parser = optparse.OptionParser()
    parser.add_option('-u', '--manifest-url', help='Manifest URL')
    parser.add_option('-n', '--username', help='Basic Auth username')
    parser.add_option('-w', '--password', help='Basic Auth password')        
    parser.add_option('-p', '--project-name', help="Project name to modify revision")
    parser.add_option('-s', '--set-revision', help="SHA hash of revision to modify project specified via --project-name")
    (opts, args) = parser.parse_args()
    return (parser, opts.manifest_url, opts.project_name, opts.set_revision, opts.username, opts.password)

def validate_args(parser, manifest_url, project_name, set_revision):
    """
    Make sure all required args are passed, or else print usage
    """
    if manifest_url is None:
        parser.print_help()
        exit(-1)
    if project_name is None:
        parser.print_help()
        exit(-1)
    if set_revision is None:
        parser.print_help()
        exit(-1)

if __name__=="__main__":
    
   # get arguments
   (parser, manifest_url, project_name, set_revision, username, password) = parse_args()

   # validate arguments
   validate_args(parser, manifest_url, project_name, set_revision)
   
   # fetch manifest content and parse xml
   request = urllib2.Request(manifest_url)
   if username != "":
       base64string = base64.b64encode('%s:%s' % (username, password))
       request.add_header("Authorization", "Basic %s" % base64string)
   tree = ET.ElementTree(file=urllib2.urlopen(request))

   # modify xml according to parameters
   root = tree.getroot()
   for element in root:
       if element.get("name") == project_name:
           element.set("revision", set_revision)
           
   # write modified xml to stdout
   tree.write(sys.stdout)
   sys.stdout.write("\n") # trailing newline

EOF

    chmod +x rewrite-manifest.sh

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

    # First emit the rewrite-manifest.sh script embedded as a string to the
    # file system so we can run it.  See comments on that function.
    emitRewriteManifestPythonScript
    
    case $PRODUCT in
	sg)
	    MANIFEST_URL="https://raw.githubusercontent.com/couchbase/sync_gateway/$COMMIT/manifest/default.xml"
	    PROJECT_NAME="sync_gateway"
	    ;;
	sg-accel)
	    if [ -z "$GITHUB_USERNAME" ]; then
		echo "You must proviate a github username and API token.  Aborting"
		exit 1
	    fi
	    MANIFEST_URL="https://raw.githubusercontent.com/couchbaselabs/sync-gateway-accel/$COMMIT/manifest/default.xml"
	    PROJECT_NAME="sync-gateway-accel"
	    ;;
	*)
	    echo "Unknown product: $PRODUCT (Aborting)"
	    exit 1
	    ;;

    esac

    echo "Using manifest: $MANIFEST_URL on commit $COMMIT for project $PROJECT_NAME with username: $GITHUB_USERNAME"
    ./rewrite-manifest.sh --manifest-url "$MANIFEST_URL" --project-name "$PROJECT_NAME" --set-revision "$COMMIT" --username "$GITHUB_USERNAME" --password "$GITHUB_API_TOKEN" > .repo/manifest.xml

}

downloadHelperScripts () {

    if [ ! -f build.sh ]; then
	echo "Downloading build.sh"
	curl -s "https://raw.githubusercontent.com/couchbase/sync_gateway/master/build.sh" > build.sh
	chmod +x build.sh    
    fi

    if [ ! -f test.sh ]; then
	echo "Downloading test.sh"
	curl -s "https://raw.githubusercontent.com/couchbase/sync_gateway/master/test.sh" > test.sh
	chmod +x test.sh
    fi

    if [ ! -f bench.sh ]; then
	echo "Downloading bench.sh"
	curl -s "https://raw.githubusercontent.com/couchbase/sync_gateway/master/bench.sh" > bench.sh
	chmod +x bench.sh
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
