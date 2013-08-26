#!/bin/bash

export   WORKSPACE=`pwd`

GOPATH=${WORKSPACE}:${WORKSPACE}/vendor
export GOPATH

export CGO_ENABLED=0

echo .....................linux-amd64
DEST_DIR=${WORKSPACE}/bin/linux-amd64
mkdir -p ${DEST_DIR}
GOOS=linux   GOARCH=amd64 go build -v github.com/couchbaselabs/sync_gateway
mv ${WORKSPACE}/sync_gateway      ${DEST_DIR}
echo "..........................Success! Output is ${DEST_DIR}/sync_gateway"

echo .....................linux-386
DEST_DIR=${WORKSPACE}/bin/linux-386
mkdir -p ${DEST_DIR}
GOOS=linux   GOARCH=386   go build -v github.com/couchbaselabs/sync_gateway
mv ${WORKSPACE}/sync_gateway      ${DEST_DIR}
echo "..........................Success! Output is ${DEST_DIR}/sync_gateway"

echo .....................windows-amd64
DEST_DIR=${WORKSPACE}/bin/windows-amd64
mkdir -p ${DEST_DIR}
GOOS=windows GOARCH=amd64 go build -v github.com/couchbaselabs/sync_gateway
mv ${WORKSPACE}/sync_gateway.exe  ${DEST_DIR}
echo "..........................Success! Output is ${DEST_DIR}/sync_gateway"

echo .....................windows-386
DEST_DIR=${WORKSPACE}/bin/windows-386
mkdir -p ${DEST_DIR}
GOOS=windows GOARCH=386   go build -v github.com/couchbaselabs/sync_gateway
mv ${WORKSPACE}/sync_gateway.exe  ${DEST_DIR}
echo "..........................Success! Output is ${DEST_DIR}/sync_gateway"

echo .....................darwin-amd64
DEST_DIR=${WORKSPACE}/bin/darwin-amd64
mkdir -p ${DEST_DIR}
GOOS=darwin  GOARCH=amd64 go build -v github.com/couchbaselabs/sync_gateway
mv ${WORKSPACE}/sync_gateway      ${DEST_DIR}
echo "..........................Success! Output is ${DEST_DIR}/sync_gateway"

