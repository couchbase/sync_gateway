// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const cbdinocluster = "github.com/couchbaselabs/cbdinocluster@latest"

// cbdinoClusterInfo and cbdinoNode are minimal structs for parsing cbdinocluster list --json output.
type cbdinoClusterInfo struct {
	ID       string       `json:"id"`
	Deployer string       `json:"deployer"`
	Nodes    []cbdinoNode `json:"nodes"`
}

type cbdinoNode struct {
	ID string `json:"id"`
}

// allocateCluster provisions a new Couchbase cluster via cbdinocluster and returns
// its cluster ID and connection string. cbdinocluster must already be initialized.
func allocateCluster(serverVersion string, protocol Protocol, multiNode bool) (clusterID, connStr string, err error) {
	def := clusterDefYAML(serverVersion, multiNode)
	logger.Debugf("Cluster definition:\n%s", def)

	f, err := os.CreateTemp("", "cbdino-cluster-*.yaml")
	if err != nil {
		return "", "", fmt.Errorf("create cluster def file: %w", err)
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if _, err := f.WriteString(def); err != nil {
		return "", "", fmt.Errorf("write cluster def: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", "", fmt.Errorf("close cluster def file: %w", err)
	}

	raw, err := tryOutput("go", "run", cbdinocluster, "allocate", "--def-file", f.Name())
	if err != nil {
		return "", "", fmt.Errorf("allocate cluster: %w", err)
	}
	clusterID = strings.TrimSpace(raw)
	logger.Debugf("Cluster ID: %s", clusterID)

	tlsFlag := "--no-tls"
	if protocol == ProtocolCouchbases {
		tlsFlag = "--tls"
	}
	raw, err = tryOutput("go", "run", cbdinocluster, "connstr", tlsFlag, clusterID)
	if err != nil {
		return "", "", fmt.Errorf("get cluster connstr: %w", err)
	}
	connStr = strings.TrimSpace(raw)
	logger.Debugf("Connection string: %s", connStr)
	return clusterID, connStr, nil
}

// deallocateCluster removes a cbdinocluster cluster. Errors are logged but not fatal
// so that cleanup failures don't mask test results.
func deallocateCluster(clusterID string) {
	logger.Debugf("Deallocating cluster %s", clusterID)
	if err := runCommand("go", "run", cbdinocluster, "rm", clusterID); err != nil {
		nonFatal.add(fmt.Errorf("deallocate cluster %q: %w", clusterID, err))
	}
}

// kvDockerName returns the Docker container name for the first KV node in clusterID.
// Container names follow the cbdinocluster convention: "cbdynnode-<node-id>".
// Returns an empty string for non-docker deployers or if no nodes are found.
func kvDockerName(clusterID string) (string, error) {
	out, err := tryOutput("go", "run", cbdinocluster, "list", "--json")
	if err != nil {
		return "", fmt.Errorf("cbdinocluster list: %w", err)
	}
	var clusters []cbdinoClusterInfo
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &clusters); err != nil {
		return "", fmt.Errorf("parse cbdinocluster list output: %w", err)
	}
	for _, c := range clusters {
		if c.ID != clusterID {
			continue
		}
		if c.Deployer != "docker" || len(c.Nodes) == 0 {
			return "", nil
		}
		return "cbdynnode-" + c.Nodes[0].ID, nil
	}
	return "", fmt.Errorf("cluster %q not found in cbdinocluster list output", clusterID)
}

// clusterDefYAML returns a cbdinocluster cluster definition.
func clusterDefYAML(serverVersion string, multiNode bool) string {
	count := 1
	if multiNode {
		count = 3
	}
	return fmt.Sprintf(`---
nodes:
  - count: %d
    version: %s
    services:
      - kv
      - n1ql
      - index
docker:
  kv-memory: 3072
  index-memory: 3072
`, count, serverVersion)
}
