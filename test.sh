#! /bin/bash
# This script runs unit tests in all the subpackages.

set -e

# First build everything so the tests don't complain about out-of-date packages
go install .

#(cd base && go test .)
(cd auth     && go test .)
(cd channels && go test .)
(cd db       && go test .)
(cd rest     && go test .)
