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
	startCBSScript = "./integration-test/start_cbs.py"
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

// initCbdinocluster initializes cbdinocluster once up front, so the parallel start_cbs.py allocations don't
// race to write ~/.cbdinocluster. On colima, init creates the dinonet network itself, but only when colima has
// a host-routable address.
func initCbdinocluster() error {
	if err := runCommand(startCBSScript, "--init-only"); err != nil {
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

// allocateCluster provisions a new Couchbase cluster via start_cbs.py and returns its cluster ID and
// connection string. cbdinocluster must already be initialized.
func allocateCluster(label, serverVersion string, protocol Protocol, multiNode bool) (clusterID, connStr string, err error) {
	f, err := os.CreateTemp("", "cbs-*.env")
	if err != nil {
		return "", "", fmt.Errorf("create env file: %w", err)
	}
	envFile := f.Name()
	defer func() { _ = os.Remove(envFile) }()
	if err := f.Close(); err != nil {
		return "", "", fmt.Errorf("close env file: %w", err)
	}

	nodes := "1"
	if multiNode {
		nodes = "3"
	}
	args := []string{"--no-reuse", "--version", serverVersion, "--nodes", nodes, "--purpose", clusterPurpose, "--env-file", envFile}
	if protocol == ProtocolCouchbases {
		args = append(args, "--tls")
	}
	if err := runLabeledCommand(cbdinoPrefix(label), startCBSScript, args...); err != nil {
		return "", "", fmt.Errorf("start_cbs.py: %w", err)
	}

	vars, err := readEnvFile(envFile)
	if err != nil {
		return "", "", err
	}
	clusterID, connStr = vars["CBS_CLUSTER_ID"], vars["SG_TEST_COUCHBASE_SERVER_URL"]
	if clusterID == "" || connStr == "" {
		return "", "", fmt.Errorf("start_cbs.py env file %s is missing CBS_CLUSTER_ID or SG_TEST_COUCHBASE_SERVER_URL", envFile)
	}
	return clusterID, connStr, nil
}

// readEnvFile parses the "export KEY=value" lines start_cbs.py writes, undoing python's shlex.quote.
func readEnvFile(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read env file: %w", err)
	}
	vars := make(map[string]string)
	for line := range strings.Lines(string(data)) {
		k, v, ok := strings.Cut(strings.TrimPrefix(strings.TrimSpace(line), "export "), "=")
		if !ok {
			continue
		}
		if len(v) >= 2 && strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'") {
			v = strings.ReplaceAll(v[1:len(v)-1], `'"'"'`, "'")
		}
		vars[k] = v
	}
	return vars, nil
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
