// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package sgtest

import (
	"os"
	"os/exec"
	"slices"
	"strings"

	"github.com/couchbase/gocbcore/v10/connstr"
)

// EnvCouchbaseServerDockerName is the environment variable that overrides the docker container name lookup
// performed by GetServerDockerContainer.
const EnvCouchbaseServerDockerName = "SG_TEST_COUCHBASE_SERVER_DOCKER_NAME"

// GetServerDockerContainer returns whether the Couchbase Server under test is running in a local Docker
// container, and if so, the container's name. EnvCouchbaseServerDockerName overrides the lookup when set
// to a non-empty value; if it is unset or blank, the container is found by matching the host of serverURL
// against the docker network IP addresses of currently running containers. If more than one running
// container matches, the result is ambiguous and (\"\", false) is returned rather than guessing.
func GetServerDockerContainer(serverURL string) (string, bool) {
	if name := strings.TrimSpace(os.Getenv(EnvCouchbaseServerDockerName)); name != "" {
		return name, true
	}

	connSpec, err := connstr.Parse(serverURL)
	if err != nil || len(connSpec.Addresses) == 0 {
		return "", false
	}
	host := connSpec.Addresses[0].Host
	if host == "" || host == "localhost" || host == "127.0.0.1" {
		return "", false
	}

	psOutput, err := exec.Command("docker", "ps", "-q").Output()
	if err != nil {
		return "", false
	}
	containerIDs := strings.Fields(string(psOutput))
	if len(containerIDs) == 0 {
		return "", false
	}

	inspectArgs := append([]string{"inspect", "--format", "{{.Name}}||{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}"}, containerIDs...)
	inspectOutput, err := exec.Command("docker", inspectArgs...).Output()
	if err != nil {
		return "", false
	}
	var matches []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(inspectOutput)), "\n") {
		name, ips, ok := strings.Cut(line, "||")
		if !ok {
			continue
		}
		if slices.Contains(strings.Fields(ips), host) {
			matches = append(matches, strings.TrimPrefix(name, "/"))
		}
	}
	if len(matches) != 1 {
		return "", false
	}
	return matches[0], true
}
