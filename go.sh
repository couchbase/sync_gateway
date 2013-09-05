#!/bin/sh -e

# This script runs the given Go subcommand with GOPATH set up correctly for sync_gateway.

export GOPATH="`pwd`:`pwd`/vendor"
go "$@"
