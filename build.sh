#!/bin/sh -e

# This script builds sync gateway using pinned dependencies via the repo tool
#
# - Set GOPATH and call 'go install' to compile and build Sync Gateway binaries

set -e

if [ -d "godeps" ]; then
  export GOPATH=`pwd`/godeps
fi

# Build both editions by default
# Limit via the $SG_EDITION env var
build_editions=( "CE" "EE" )
if [ "$SG_EDITION" = "CE" -o "$SG_EDITION" = "EE" ]; then
    echo "Building only $SG_EDITION"
    build_editions=( $SG_EDITION )
else
    echo "Building all editions ... Limit with 'SG_EDITION=CE $0'"
fi

updateVersionStamp () {
    # Build path to SG code directory
    SG_DIR=$GOPATH/src/github.com/couchbase/sync_gateway

    # Save the current directory
    CURRENT_DIR=`pwd`

    # Cd into SG code directory
    cd $SG_DIR
    ./set-version-stamp.sh || true

    # Go back to the original current directory
    cd $CURRENT_DIR
}

doBuild () {
    buildTags=""
    binarySuffix="_ce"
    if [ "$1" = "EE" ]; then
        buildTags="-tags cb_sg_enterprise"
        binarySuffix=""
    fi

    mkdir -p "${GOPATH}/bin"

    ## Go Install Sync Gateway
    echo "    Building Sync Gateway"
    go build -o sync_gateway${binarySuffix} ${buildTags} "${@:2}" github.com/couchbase/sync_gateway/cli
    mv "sync_gateway${binarySuffix}" "${GOPATH}/bin/sync_gateway${binarySuffix}"
    echo "      Success!"
    # Let user where to know where to find binaries
    if [ -f "${GOPATH}/bin/sync_gateway${binarySuffix}" ]; then
        echo "      Binary compiled to: ${GOPATH}/bin/sync_gateway${binarySuffix}"
    fi

    # Go install Sg Accel
    if [ -d godeps/src/github.com/couchbaselabs/sync-gateway-accel ]; then
        echo "    Building Sync Gateway Accel with 'go install' ..."
        go install "${@:2}" github.com/couchbaselabs/sync-gateway-accel/...
        echo "      Success!"
        if [ -f "godeps/bin/sync-gateway-accel" ]; then
            echo "        Binary compiled to: godeps/bin/sync-gateway-accel"
        fi
    fi
}

for edition in "${build_editions[@]}"; do
    echo "  Building edition: ${edition}"
    updateVersionStamp
    doBuild $edition "$@"
done
