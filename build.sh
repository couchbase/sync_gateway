#!/bin/bash

# Copyright 2013-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# NOTE: building the EE version (SG_EDITION=EE ./build.sh) requires ssh access
# to private a repo.  Please make sure you have access to:
# https://github.com/couchbaselabs/go-fleecedelta

set -e

BLDPARS=${@:2}
SRCPATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OUTPATH=${SRCPATH}/bin
mkdir -p ${OUTPATH}

# Build both editions by default
# Limit via the $SG_EDITION env var
build_editions=( "CE" "EE" )
if [ "${SG_EDITION}" = "CE" -o "${SG_EDITION}" = "EE" ]; then
    echo "Building only ${SG_EDITION}"
    build_editions=( ${SG_EDITION} )
else
    echo "Building all editions ... Limit with 'SG_EDITION=CE $0'"
fi

doBuild () {
    cd ${SRCPATH}
    ./set-version-stamp.sh || true
    privRepos=""
    buildTags=""
    binarySuffix="_ce"
    if [ "$1" = "EE" ]; then
        buildTags="-tags cb_sg_enterprise"
        binarySuffix=""
        githubSshConfig=$( ( git config --global --list ; git config --system --list ) | grep -i "url.git@github.com:" | cat )
        if [ -z "${githubSshConfig}" ]; then
            git config --global url.git@github.com:couchbaselabs/go-fleecedelta.insteadOf https://github.com/couchbaselabs/go-fleecedelta
        fi
    fi

    ## Go Install Sync Gateway
    echo "    Building Sync Gateway"
    echo go build -o "${OUTPATH}/sync_gateway${binarySuffix}" ${buildTags} "${BLDPARS}" ${SRCPATH}
    go build -o "${OUTPATH}/sync_gateway${binarySuffix}" ${buildTags} "${BLDPARS}" ${SRCPATH}
    # Let user know where to find binaries
    if [ -f "${OUTPATH}/sync_gateway${binarySuffix}" ]; then
        echo "      Success!"
        echo "      Binary compiled to: ${OUTPATH}/sync_gateway${binarySuffix}"
    else
        echo "      ERROR: Binary not found!"
        exit 1
    fi
}

for edition in "${build_editions[@]}"; do
    echo "  Building edition: ${edition}"
    (doBuild $edition "$@")
done
