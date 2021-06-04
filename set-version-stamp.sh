#!/bin/sh -e

# Copyright 2016-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# This script updates the rest/git_info.go file which bakes in the
# git version to the Sync Gateway binary

## Update the version
BUILD_INFO="./base/git_info.go"

#tell git to ignore any local changes to git_info.go, we don't want to commit them to the repo
git update-index --assume-unchanged ${BUILD_INFO}

# Escape forward slash's so sed command does not get confused
# We use thses in feature branches e.g. feature/issue_nnn
PRODUCT_NAME="Couchbase Sync Gateway"
GIT_BRANCH=`git status -b -s | sed q | sed 's/## //' | sed 's/\.\.\..*$//' | sed 's/\\//\\\\\//g' | sed 's/[[:space:]]//g'`
GIT_COMMIT=`git rev-parse HEAD`
GIT_DIRTY=$(test -n "`git status --porcelain`" && echo "+CHANGES" || true)

sed -i.bak -e 's/GitProductName.*=.*/GitProductName = "'"$PRODUCT_NAME"'"/' $BUILD_INFO
sed -i.bak -e 's/GitCommit.*=.*/GitCommit = "'$GIT_COMMIT'"/' $BUILD_INFO
sed -i.bak -e 's/GitBranch.*=.*/GitBranch = "'$GIT_BRANCH'"/' $BUILD_INFO
sed -i.bak -e 's/GitDirty.*=.*/GitDirty = "'$GIT_DIRTY'"/' $BUILD_INFO


