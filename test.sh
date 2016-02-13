#!/bin/sh -e
# This script runs unit tests in all the subpackages.

# vet reports two errors which are actually OK here.  There are flags
# to vet to tell it just what you want, but he invocation is a lot
# more awkward, so I'm just going to grep away the things we don't care
# about so we can see the things we do.
go vet ./...

# First build everything so the tests don't complain about out-of-date packages
go test -i ./...
go test ./... "$@"
