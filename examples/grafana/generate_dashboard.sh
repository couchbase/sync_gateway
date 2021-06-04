#!/bin/bash

# Copyright 2020-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

set -eo pipefail
go get github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb
jb init
jb install https://github.com/grafana/grafonnet-lib/grafonnet
jsonnet -J grafana dashboard.jsonnet -o ./dashboard.json