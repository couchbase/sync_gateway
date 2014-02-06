#!/bin/bash

# This script runs the given Go subcommand with GOPATH set up correctly for sync_gateway.

if [[ `go version` != *go1.2* ]]
then
    echo "*** Go 1.2 is required to build Sync Gateway; you have" `go version`
    echo "Please visit http://golang.org/doc/install or use your package manager to upgrade."
    exit 1
fi

export GOPATH="`pwd`:`pwd`/vendor"
go "$@"
