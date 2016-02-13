#!/bin/bash

export WORKSPACE=`pwd`

export CGO_ENABLED=0

FAILS=0

echo .....................windows-amd64
DEST_DIR=${WORKSPACE}/bin/windows-amd64
mkdir -p ${DEST_DIR}
GOOS=windows GOARCH=amd64 go build -v 
if [[ -e ${WORKSPACE}/sync_gateway.exe ]]
  then
    mv   ${WORKSPACE}/sync_gateway.exe  ${DEST_DIR}
    echo "..........................Success! Output is: ${DEST_DIR}/sync_gateway.exe"
  else
    FAILS=$((FAILS+1))
    echo "######################### FAIL! no such file: ${DEST_DIR}/sync_gateway.exe"
fi

echo .....................windows-386
DEST_DIR=${WORKSPACE}/bin/windows-386
mkdir -p ${DEST_DIR}
GOOS=windows GOARCH=386   go build -v 
if [[ -e ${WORKSPACE}/sync_gateway.exe ]]
  then
    mv   ${WORKSPACE}/sync_gateway.exe  ${DEST_DIR}
    echo "..........................Success! Output is: ${DEST_DIR}/sync_gateway.exe"
  else
    FAILS=$((FAILS+1))
    echo "######################### FAIL! no such file: ${DEST_DIR}/sync_gateway.exe"
fi

echo .....................linux-386
DEST_DIR=${WORKSPACE}/bin/linux-386
mkdir -p ${DEST_DIR}
GOOS=linux   GOARCH=386   go build -v 
if [[ -e ${WORKSPACE}/sync_gateway ]]
  then
    mv   ${WORKSPACE}/sync_gateway      ${DEST_DIR}
    echo "..........................Success! Output is: ${DEST_DIR}/sync_gateway"
  else
    FAILS=$((FAILS+1))
    echo "######################### FAIL! no such file: ${DEST_DIR}/sync_gateway"
fi

echo .....................linux-amd64
DEST_DIR=${WORKSPACE}/bin/linux-amd64
mkdir -p ${DEST_DIR}
GOOS=linux   GOARCH=amd64 go build -v 
if [[ -e ${WORKSPACE}/sync_gateway ]]
  then
    mv   ${WORKSPACE}/sync_gateway      ${DEST_DIR}
    echo "..........................Success! Output is: ${DEST_DIR}/sync_gateway"
  else
    FAILS=$((FAILS+1))
    echo "######################### FAIL! no such file: ${DEST_DIR}/sync_gateway"
fi

echo .....................darwin-amd64
DEST_DIR=${WORKSPACE}/bin/darwin-amd64
mkdir -p ${DEST_DIR}
GOOS=darwin  GOARCH=amd64 go build -v 
if [[ -e ${WORKSPACE}/sync_gateway ]]
  then
    mv   ${WORKSPACE}/sync_gateway      ${DEST_DIR}
    echo "..........................Success! Output is: ${DEST_DIR}/sync_gateway"
  else
    FAILS=$((FAILS+1))
    echo "######################### FAIL! no such file: ${DEST_DIR}/sync_gateway"
fi

exit ${FAILS}
