#!/bin/bash
set -eo pipefail
go get github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb
jb init
jb install https://github.com/grafana/grafonnet-lib/grafonnet
jsonnet -J grafana dashboard.jsonnet -o ./dashboard.json