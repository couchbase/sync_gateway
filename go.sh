#!/bin/bash

# This script runs the given Go subcommand with GOPATH set up correctly for sync_gateway.

GO_VERSION=`go version | awk '{ match($3, /.*([0-9]+\.[0-9]).*/, res); print res[1] }'`
if [[ $(echo "$GO_VERSION >= 1.2" | bc) -eq 0 ]]; then
  echo "*** Go 1.2 or higher is required to build Sync Gateway; you have" `go version`
  echo "Please visit http://golang.org/doc/install or use your package manager to upgrade."
  exit 1
fi

export GOPATH="`pwd`:`pwd`/vendor"
go "$@"
