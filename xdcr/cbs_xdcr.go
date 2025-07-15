// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package xdcr

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os/exec"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/base"
	dto "github.com/prometheus/client_model/go"
)

const (
	cbsRemoteClustersEndpoint           = "/pools/default/remoteClusters"
	xdcrClusterName                     = "sync_gateway_xdcr" // this is a hardcoded name for the local XDCR cluster
	totalMobileDocsFiltered             = "xdcr_mobile_docs_filtered_total"
	totalDocsWrittenStat                = "xdcr_docs_written_total"
	totalDocsConflictResolutionRejected = "xdcr_docs_failed_cr_source_total"
)

var errNoXDCRMetrics = errors.New("No metric found")

// couchbaseServerManager implements a XDCR setup cluster on Couchbase Server.
type couchbaseServerManager struct {
	fromBucket        *base.GocbV2Bucket
	toBucket          *base.GocbV2Bucket
	replicationID     string
	filter            string
	startingTimestamp string
	mobileSetting     MobileSetting
}

// isClusterPresent returns true if the XDCR cluster is present, false if it is not present, and an error if it could not be determined.
func isClusterPresent(ctx context.Context, bucket *base.GocbV2Bucket) (bool, error) {
	method := http.MethodGet
	url := cbsRemoteClustersEndpoint
	output, statusCode, err := bucket.MgmtRequest(ctx, method, url, "application/x-www-form-urlencoded", nil)
	if err != nil {
		return false, err
	}
	if statusCode != http.StatusOK {
		return false, fmt.Errorf("Could not determine anything about XDCR cluster: %s. %s %s -> (%d) %s", xdcrClusterName, method, url, statusCode, output)
	}
	type clusterOutput struct {
		Name string `json:"name"`
	}
	var clusters []clusterOutput
	err = base.JSONUnmarshal(output, &clusters)
	if err != nil {
		return false, err
	}
	for _, cluster := range clusters {
		if cluster.Name == xdcrClusterName {
			return true, nil
		}
	}
	return false, nil
}

// createCluster creates an XDCR cluster.
func createCluster(ctx context.Context, bucket *base.GocbV2Bucket) error {
	serverURL, err := url.Parse(base.UnitTestUrl())
	if err != nil {
		return err
	}

	method := http.MethodPost
	body := url.Values{}
	body.Add("name", xdcrClusterName)
	body.Add("hostname", serverURL.Hostname())
	body.Add("username", base.TestClusterUsername())
	body.Add("password", base.TestClusterPassword())
	body.Add("secure", "full")
	url := cbsRemoteClustersEndpoint

	output, statusCode, err := bucket.MgmtRequest(ctx, method, url, "application/x-www-form-urlencoded", strings.NewReader(body.Encode()))
	if err != nil {
		return err
	}
	if statusCode != http.StatusOK {
		return fmt.Errorf("Could not create xdcr cluster: %s. %s %s -> (%d) %s", xdcrClusterName, method, url, statusCode, output)
	}
	return nil
}

// newCouchbaseServerManager creates an instance of XDCR backed by Couchbase Server. This is not started until Start is called.
func newCouchbaseServerManager(ctx context.Context, fromBucket *base.GocbV2Bucket, toBucket *base.GocbV2Bucket, opts XDCROptions) (*couchbaseServerManager, error) {
	nodes, err := kvNodes(ctx, fromBucket)
	if err != nil {
		return nil, err
	}
	if len(nodes) != 1 {
		return nil, fmt.Errorf("To run xdcr tests, exactly one kv node is needed to grep the goxdcr.log file. To extend this to multiple nodes (found %s), all nodes would need to be grepped", nodes)
	}
	// there needs to be a global cluster present, this is a hostname + username + password. There can be only one per hostname, so create it lazily.
	isPresent, err := isClusterPresent(ctx, fromBucket)
	if err != nil {
		return nil, err

	}
	if !isPresent {
		err := createCluster(ctx, fromBucket)
		if err != nil {
			return nil, err
		}
	}
	return &couchbaseServerManager{
		fromBucket:    fromBucket,
		toBucket:      toBucket,
		mobileSetting: opts.Mobile,
		filter:        opts.FilterExpression,
	}, nil
}

// kvNodes returns the hostnames of KV nodes in the cluster.
func kvNodes(ctx context.Context, bucket *base.GocbV2Bucket) ([]string, error) {
	url := "/pools/default/"
	method := http.MethodGet
	output, statusCode, err := bucket.MgmtRequest(ctx, method, url, "application/x-www-form-urlencoded", nil)
	if err != nil {
		return nil, err
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("Could not get the number bucket metadata: %s. %s %s -> (%d) %s", xdcrClusterName, method, url, statusCode, output)
	}
	type nodesOutput struct {
		Nodes []struct {
			Hostname string   `json:"hostname"`
			Services []string `json:"services"`
		} `json:"nodes"`
	}
	nodes := nodesOutput{}
	err = base.JSONUnmarshal(output, &nodes)
	if err != nil {
		return nil, err
	}
	var hostnames []string
	for _, node := range nodes.Nodes {
		if slices.Contains(node.Services, "kv") {
			hostnames = append(hostnames, node.Hostname)
		}
	}
	return hostnames, nil
}

// Start starts the XDCR replication.
func (x *couchbaseServerManager) Start(ctx context.Context) error {
	if x.replicationID != "" {
		return ErrReplicationAlreadyRunning
	}
	var err error
	x.startingTimestamp, err = x.lastTimestampOfLogFile(ctx)
	if err != nil {
		return err
	}
	method := http.MethodPost
	body := url.Values{}
	body.Add("name", fmt.Sprintf("%s_%s", x.fromBucket.GetName(), x.toBucket.GetName()))
	body.Add("fromBucket", x.fromBucket.GetName())
	body.Add("toBucket", x.toBucket.GetName())
	body.Add("toCluster", xdcrClusterName)
	body.Add("replicationType", "continuous")
	// set the mobile flag on the replication
	body.Add("mobile", x.mobileSetting.String())
	// add filter is needed
	if x.filter != "" {
		body.Add("filterExpression", x.filter)
	}
	url := "/controller/createReplication"
	output, statusCode, err := x.fromBucket.MgmtRequest(ctx, method, url, "application/x-www-form-urlencoded", strings.NewReader(body.Encode()))
	if err != nil {
		return err
	}
	if statusCode != http.StatusOK {
		return fmt.Errorf("Could not create xdcr replication: %s. %s %s -> (%d) %s", xdcrClusterName, method, url, statusCode, output)
	}
	type replicationOutput struct {
		ID string `json:"id"`
	}
	id := replicationOutput{}
	err = base.JSONUnmarshal(output, &id)
	if err != nil {
		return err
	}
	x.replicationID = id.ID
	if x.replicationID == "" {
		return fmt.Errorf("Could not determine replication ID from output: %s", output)
	}
	return nil
}

// Stop starts the XDCR replication and deletes the replication from Couchbase Server.
func (x *couchbaseServerManager) Stop(ctx context.Context) error {
	// replication is not started
	if x.replicationID == "" {
		return ErrReplicationNotRunning
	}
	method := http.MethodDelete
	url := "/controller/cancelXDCR/" + url.PathEscape(x.replicationID)
	output, statusCode, err := x.fromBucket.MgmtRequest(ctx, method, url, "application/x-www-form-urlencoded", nil)
	if err != nil {
		return fmt.Errorf("Could not %s to %s: %w", method, url, err)
	}
	if statusCode != http.StatusOK {
		return fmt.Errorf("Could not cancel XDCR replication: %s. %s %s -> (%d) %s", x.replicationID, method, url, statusCode, output)
	}
	err = x.waitForStoppedInLogFile(ctx)
	if err != nil {
		return err
	}
	x.replicationID = ""
	x.startingTimestamp = ""
	return err
}

// Stats returns the stats of the XDCR replication.
func (x *couchbaseServerManager) Stats(ctx context.Context) (*Stats, error) {
	mf, err := x.fromBucket.ServerMetrics(ctx)
	if err != nil {
		return nil, err
	}
	stats := &Stats{}

	statMap := map[string]*uint64{
		totalMobileDocsFiltered:             &stats.MobileDocsFiltered,
		totalDocsWrittenStat:                &stats.DocsWritten,
		totalDocsConflictResolutionRejected: &stats.TargetNewerDocs,
	}
	var errs *base.MultiError
	for metricName, stat := range statMap {
		metricFamily, ok := mf[metricName]
		if !ok {
			errs = errs.Append(fmt.Errorf("Could not find %s metric: %+v", metricName, mf))
			continue
		}
		var err error
		*stat, err = x.getValue(metricFamily)
		if err != nil {
			errs = errs.Append(err)
		}
	}
	stats.DocsProcessed = stats.DocsWritten + stats.MobileDocsFiltered + stats.TargetNewerDocs
	return stats, errs.ErrorOrNil()
}

func (x *couchbaseServerManager) getValue(metrics *dto.MetricFamily) (uint64, error) {
outer:
	for _, metric := range metrics.GetMetric() {
		for _, label := range metric.Label {
			if label.GetName() == "pipelineType" && label.GetValue() != "Main" {
				continue outer
			}
			if label.GetName() == "sourceBucketName" && label.GetValue() != x.fromBucket.GetName() {
				continue outer
			}
			if label.GetName() == "targetBucketName" && label.GetValue() != x.toBucket.GetName() {
				continue outer
			}
		}
		switch *metrics.Type {
		case dto.MetricType_COUNTER:
			return uint64(metric.Counter.GetValue()), nil
		case dto.MetricType_GAUGE:
			return uint64(metric.Gauge.GetValue()), nil
		default:
			return 0, fmt.Errorf("Do not have a relevant type for %v", metrics.Type)
		}
	}
	return 0, errNoXDCRMetrics
}

// lineCountOfLogFile returns the number of lines in the goxdcr.log file. This is used to determine the offset when re-reading the log file.
func (x *couchbaseServerManager) lastTimestampOfLogFile(ctx context.Context) (string, error) {
	logFile := x.xdcrLogFilePath()
	// most, but not all lines start with a timestamp, so we need to find the last line that does 2000-01-01T01:01:01.000Z
	cmdLine := fmt.Sprintf(`tail -n100 "%s"`, logFile)
	// If the log file is actively being rotated, we need to sleep to wait for the log file.
	err, timestamp := base.RetryLoop(ctx, "ReadLogFileUntilStopped", func() (shouldRetry bool, err error, timestamp string) {
		// If the log file is being rotated, the goxdcr.log file may not be present, and this command will fail.
		output, err := x.runCommandOnCBS(cmdLine)
		if err != nil {
			return true, fmt.Errorf("Could not read the last 100 lines of %s: %w", logFile, err), ""
		}
		lines := strings.Split(strings.TrimSpace(output), "\n")
		slices.Reverse(lines)
		for _, line := range lines {
			re := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`)
			if re.MatchString(line) {
				timestamp := strings.Split(line, " ")[0]
				return false, nil, timestamp
			}
		}
		// If the log file just got rotated, it might not have a timestamp yet.
		return true, fmt.Errorf("Could not find a timestamp in the last 100 lines of %s: %s", logFile, output), ""
	}, base.CreateLinearSleeperFunc(5*time.Second, 100*time.Millisecond))
	return timestamp, err
}

// xdcrLogFilePath returns the path of the goxdcr.log file.
func (x *couchbaseServerManager) xdcrLogFilePath() string {
	usingDocker, _ := base.TestUseCouchbaseServerDockerName()
	if runtime.GOOS == "darwin" && !usingDocker {
		return "$HOME/Library/Application Support/Couchbase/var/lib/couchbase/logs/goxdcr.log"
	}
	return "/opt/couchbase/var/lib/couchbase/logs/goxdcr.log"
}

// runCommandOnCBS executes a command against in the same machine of couchbase server. This is aware of CBS running locally or locally within docker.
func (x *couchbaseServerManager) runCommandOnCBS(cmdLine string) (string, error) {
	usingDocker, dockerName := base.TestUseCouchbaseServerDockerName()
	var fullCmdLine []string
	if usingDocker {
		fullCmdLine = append(fullCmdLine, []string{"docker", "exec", "-t", dockerName}...)
	}
	fullCmdLine = append(fullCmdLine, []string{"bash", "-c", cmdLine}...)
	cmd := exec.Command(fullCmdLine[0], fullCmdLine[1:]...) // nolint: gosec
	output, err := cmd.CombinedOutput()
	if err != nil {
		suffix := ""
		if !usingDocker {
			suffix = fmt.Sprintf(". If you are running in docker, you may need to set the environment variable %s=<name of the container>", base.TestEnvCouchbaseServerDockerName)
		}
		return string(output), fmt.Errorf("Failed to run %s (%w) Output: %s%s", cmd, err, output, suffix)
	}
	return string(output), nil
}

// waitForStoppedInLogFile waits for the replication to stop by checking the log file.
func (x *couchbaseServerManager) waitForStoppedInLogFile(ctx context.Context) error {
	// magic string to indicate that the replication has stopped
	grepStr := fmt.Sprintf("%s status is finished shutting down", x.replicationID)
	logFile := x.xdcrLogFilePath()
	// look for log files that are goxcdr.log[.[0-9]][.gz], the message may be in a rotated log
	cmdLine := fmt.Sprintf(`zgrep --no-filename "%s" "%s"*`, grepStr, logFile)
	err, _ := base.RetryLoop(ctx, "ReadLogFileUntilStopped", func() (shouldRetry bool, err error, value any) {
		output, err := x.runCommandOnCBS(cmdLine)
		if err != nil {
			return true, err, nil
		}
		for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
			timestamp := strings.Split(line, " ")[0]
			if timestamp > x.startingTimestamp {
				return false, nil, nil
			}
		}
		return true, fmt.Errorf("Could not find line newer than %s in %s", x.startingTimestamp, output), nil
	}, base.CreateLinearSleeperFunc(5*time.Minute, 1*time.Second))
	if err != nil {
		return fmt.Errorf("Could not find %s in %s. %w", grepStr, logFile, err)
	}
	return nil
}

var _ Manager = &couchbaseServerManager{}
