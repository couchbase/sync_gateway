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
	"path/filepath"
	"runtime"
	"strings"
)

const (
	// cbdinoToolsDir is the module that pins cbdinocluster with a tool directive.
	cbdinoToolsDir = "integration-test/tools"
	dinoNetwork    = "dinonet"
	clusterPurpose = "sync_gateway_integration"
)

// cbdinoArgs returns the go arguments that run cbdinocluster with args. cbdinocluster runs in cbdinoToolsDir,
// so any path in args must be absolute.
func cbdinoArgs(args ...string) []string {
	return append([]string{"-C", cbdinoToolsDir, "tool", "cbdinocluster"}, args...)
}

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
	err := runCommand("go", cbdinoArgs("init", "--auto",
		"--disable-k8s", "--disable-capella", "--disable-aws", "--disable-azure", "--disable-gcp", "--disable-dns")...)
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

	raw, err := tryLabeledOutput(prefix, "go", cbdinoArgs("allocate", "--def-file", f.Name(), "--purpose", clusterPurpose)...)
	if err != nil {
		return "", "", fmt.Errorf("allocate cluster: %w", err)
	}
	clusterID = strings.TrimSpace(raw)
	logger.Debugf("%sCluster ID: %s", prefix, clusterID)

	tlsFlag := "--no-tls"
	if protocol == ProtocolCouchbases {
		tlsFlag = "--tls"
	}
	raw, err = tryLabeledOutput(prefix, "go", cbdinoArgs("connstr", tlsFlag, clusterID)...)
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
	if err := runLabeledCommand(prefix, "go", cbdinoArgs("rm", clusterID)...); err != nil {
		nonFatal.add(fmt.Errorf("deallocate cluster %q: %w", clusterID, err))
	}
}

// collectLogs writes a cbcollect_info zip for every node in clusterID to zipFile on the host.
func collectLogs(label, clusterID, zipFile string) error {
	absZip, err := filepath.Abs(zipFile)
	if err != nil {
		return fmt.Errorf("resolve %q: %w", zipFile, err)
	}
	return runLabeledCommand(labelPrefix(label)+"cbcollect: ", "go", cbdinoArgs("collect-logs", clusterID, absZip)...)
}

// kvDockerName returns the Docker container name for the first KV node in clusterID.
// Container names follow the cbdinocluster convention: "cbdynnode-<node-id>".
// Returns an empty string for non-docker deployers or if no nodes are found.
func kvDockerName(label, clusterID string) (string, error) {
	out, err := tryLabeledOutput(cbdinoPrefix(label), "go", cbdinoArgs("list", "--json")...)
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

// clusterDefYAML returns a cbdinocluster cluster definition. A serverVersion containing "/" is a full docker
// image reference (e.g. ghcr.io/cb-vanilla/server:8.5.0), which cbdinocluster can't resolve as a version.
func clusterDefYAML(serverVersion string, multiNode bool) string {
	count := 1
	if multiNode {
		count = 3
	}
	image := ""
	if strings.Contains(serverVersion, "/") {
		image = "    docker:\n      image: " + serverVersion + "\n"
	}
	return fmt.Sprintf(`---
nodes:
  - count: %d
    version: %s
    services:
      - kv
      - n1ql
      - index
%sdocker:
  kv-memory: 1200
  index-memory: 1200
`, count, serverVersion, image)
}
