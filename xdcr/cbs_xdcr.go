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
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/couchbase/sync_gateway/base"
	dto "github.com/prometheus/client_model/go"
)

const (
	cbsRemoteClustersEndpoint = "/pools/default/remoteClusters"
	xdcrClusterName           = "sync_gateway_xdcr" // this is a hardcoded name for the local XDCR cluster
	totalDocsFilteredStat     = "xdcr_docs_filtered_total"
	totalDocsWrittenStat      = "xdcr_docs_written_total"
)

// couchbaseServerManager implements a XDCR setup cluster on Couchbase Server.
type couchbaseServerManager struct {
	fromBucket    *base.GocbV2Bucket
	toBucket      *base.GocbV2Bucket
	replicationID string
	filter        string
	mobileSetting MobileSetting
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

// Start starts the XDCR replication.
func (x *couchbaseServerManager) Start(ctx context.Context) error {
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
		return fmt.Errorf("Could not create xdcr cluster: %s. %s %s -> (%d) %s", xdcrClusterName, method, url, statusCode, output)
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
	method := http.MethodDelete
	url := "/controller/cancelXDCR/" + url.PathEscape(x.replicationID)
	output, statusCode, err := x.fromBucket.MgmtRequest(ctx, method, url, "application/x-www-form-urlencoded", nil)
	if err != nil {
		return fmt.Errorf("Could not %s to %s: %w", method, url, err)
	}
	if statusCode != http.StatusOK {
		return fmt.Errorf("Could not cancel XDCR replication: %s. %s %s -> (%d) %s", x.replicationID, method, url, statusCode, output)
	}
	x.replicationID = ""
	return nil
}

// Stats returns the stats of the XDCR replication.
func (x *couchbaseServerManager) Stats(ctx context.Context) (*Stats, error) {
	mf, err := x.fromBucket.ServerMetrics(ctx)
	if err != nil {
		return nil, err
	}
	stats := &Stats{}
	stats.DocsFiltered, err = x.getValue(mf[totalDocsFilteredStat])
	if err != nil {
		return stats, err
	}
	stats.DocsWritten, err = x.getValue(mf[totalDocsWrittenStat])
	if err != nil {
		return stats, err
	}
	return stats, nil
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
	return 0, fmt.Errorf("Could not find relevant value for metrics %v", metrics)
}

var _ Manager = &couchbaseServerManager{}
