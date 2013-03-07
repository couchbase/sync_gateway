#! /bin/bash
# This script runs unit tests in all the subpackages.

set -e

# First build everything so the tests don't complain about out-of-date packages
go test -i

go test ./base ./auth ./channels ./db ./rest $@
