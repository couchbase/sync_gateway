#!/bin/bash

# Copyright 2020-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

set -eo pipefail
echo
echo "grafana: making sure Prometheus is a data source"
curl -fu admin:admin 'http://localhost:3000/api/datasources/id/Prometheus' ||
	curl -fu admin:admin -XPOST 'http://localhost:3000/api/datasources' \
		-H 'Content-Type: application/json;charset=UTF-8' \
		--data-binary '{
		  "name": "Prometheus",
		  "isDefault": true,
		  "type": "prometheus",
		  "url": "http://prometheus:9090",
		  "access": "proxy",
		  "basicAuth": false
		}'

echo
echo
echo "grafana: install/overwrite dashboard"
curl -fu admin:admin 'http://localhost:3000/api/dashboards/db' \
  -H 'Accept: application/json' \
  -H 'Content-Type: application/json' \
  --data-binary @<( echo "{\"overwrite\": true, \"dashboard\": $(cat ./dashboard.json)}" | jq . )
