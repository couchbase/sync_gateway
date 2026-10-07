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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

const (
	cbdinocluster = "github.com/couchbaselabs/cbdinocluster@latest"
	dinoNetwork   = "dinonet"
)

// cbdinoClusterInfo and cbdinoNode are minimal structs for parsing cbdinocluster list --json output.
type cbdinoClusterInfo struct {
	ID       string       `json:"id"`
	Deployer string       `json:"deployer"`
	Nodes    []cbdinoNode `json:"nodes"`
}

type cbdinoNode struct {
	ID string `json:"id"`
}

// initCbdinocluster configures cbdinocluster for docker deployments. On colima, init creates the
// dinonet network itself, but only when colima has a host-routable address.
func initCbdinocluster() error {
	err := runCommand("go", "run", cbdinocluster, "init", "--auto",
		"--disable-k8s", "--disable-capella", "--disable-aws", "--disable-azure", "--disable-gcp", "--disable-dns")
	if err != nil {
		return fmt.Errorf("cbdinocluster init: %w", err)
	}
	if runtime.GOOS != "darwin" {
		return nil
	}
	if err := exec.Command("docker", "network", "inspect", dinoNetwork).Run(); err != nil {
		return errors.New("docker network " + dinoNetwork + " does not exist and cbdinocluster could not create it. " +
			"Restart colima with a network address (colima stop && colima start --network-address) and run again")
	}
	return nil
}

// cbdinoPrefix returns the log prefix for cbdinocluster commands run on behalf of a package.
func cbdinoPrefix(label string) string {
	return labelPrefix(label) + "cbdinocluster: "
}

// allocateCluster provisions a new Couchbase cluster via cbdinocluster and returns
// its cluster ID and connection string. cbdinocluster must already be initialized.
func allocateCluster(label, serverVersion string, protocol Protocol, multiNode bool) (clusterID, connStr string, err error) {
	prefix := cbdinoPrefix(label)
	def := clusterDefYAML(serverVersion, multiNode)
	for line := range strings.Lines(def) {
		logger.Debugf("%sdefinition: %s", prefix, strings.TrimRight(line, "\n"))
	}

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

	raw, err := tryLabeledOutput(prefix, "go", "run", cbdinocluster, "allocate", "--def-file", f.Name())
	if err != nil {
		return "", "", fmt.Errorf("allocate cluster: %w", err)
	}
	clusterID = strings.TrimSpace(raw)
	logger.Debugf("%sCluster ID: %s", prefix, clusterID)

	tlsFlag := "--no-tls"
	if protocol == ProtocolCouchbases {
		tlsFlag = "--tls"
	}
	raw, err = tryLabeledOutput(prefix, "go", "run", cbdinocluster, "connstr", tlsFlag, clusterID)
	if err != nil {
		return "", "", fmt.Errorf("get cluster connstr: %w", err)
	}
	connStr = strings.TrimSpace(raw)
	logger.Debugf("%sConnection string: %s", prefix, connStr)
	return clusterID, connStr, nil
}

// deallocateCluster removes a cbdinocluster cluster. Errors are logged but not fatal
// so that cleanup failures don't mask test results.
func deallocateCluster(label, clusterID string) {
	prefix := cbdinoPrefix(label)
	logger.Debugf("%sDeallocating cluster %s", prefix, clusterID)
	if err := runLabeledCommand(prefix, "go", "run", cbdinocluster, "rm", clusterID); err != nil {
		nonFatal.add(fmt.Errorf("deallocate cluster %q: %w", clusterID, err))
	}
}

// kvDockerName returns the Docker container name for the first KV node in clusterID.
// Container names follow the cbdinocluster convention: "cbdynnode-<node-id>".
// Returns an empty string for non-docker deployers or if no nodes are found.
func kvDockerName(label, clusterID string) (string, error) {
	out, err := tryLabeledOutput(cbdinoPrefix(label), "go", "run", cbdinocluster, "list", "--json")
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
  kv-memory: 1200
  index-memory: 1200
`, count, serverVersion)
}
