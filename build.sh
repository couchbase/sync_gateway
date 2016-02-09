#!/bin/sh -e

# This script builds the sync gateway.

# Set the git commit info before the build
BUILD_INFO="./rest/git_info.go"

#tell git to ignore any local changes to git_info.go, we don't want to commit them to the repo
git update-index --assume-unchanged ${BUILD_INFO}

# Escape forward slash's so sed command does not get confused
# We use thses in feature branches e.g. feature/issue_nnn
GIT_BRANCH=`git status -b -s | sed q | sed 's/## //' | sed 's/\.\.\..*$//' | sed 's/\\//\\\\\//g' | sed 's/[[:space:]]//g'`
GIT_COMMIT=`git rev-parse HEAD`
GIT_DIRTY=$(test -n "`git status --porcelain`" && echo "+CHANGES" || true)

sed -i.bak -e 's/GitCommit.*=.*/GitCommit = "'$GIT_COMMIT'"/' $BUILD_INFO
sed -i.bak -e 's/GitBranch.*=.*/GitBranch = "'$GIT_BRANCH'"/' $BUILD_INFO
sed -i.bak -e 's/GitDirty.*=.*/GitDirty = "'$GIT_DIRTY'"/' $BUILD_INFO

./go.sh install "$@" -v ./...

echo "Success! Output is $GOPATH/bin/sync_gateway and $GOPATH/bin/sg_accel"
