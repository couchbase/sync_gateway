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
	"io"
	"net/http"
	"net/url"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	dto "github.com/prometheus/client_model/go"
)

const (
	cbsRemoteClustersEndpoint = "/pools/default/remoteClusters"
	xdcrClusterName           = "sync_gateway_xdcr"
	totalDocsFilteredStat     = "xdcr_docs_filtered_total"
	totalDocsWrittenStat      = "xdcr_docs_written_total"
)

const (
	MobileOff    = "Off"
	MobileActive = "Active"
)

// CouchbaseServerXDCR implements a XDCR setup cluster on Couchbase Server.
type CouchbaseServerXDCR struct {
	fromBucket    *base.GocbV2Bucket
	toBucket      *base.GocbV2Bucket
	replicationID string
	MobileSetting string
}

// mgmtRequest makes a request to the Couchbase Server management API in the format for xdcr.
func mgmtRequest(ctx context.Context, bucket *base.GocbV2Bucket, method, url string, body io.Reader) ([]byte, int, error) {
	resp, err := bucket.MgmtRequest(ctx, method, url, "application/x-www-form-urlencoded", body)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	output, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("Could not read body from %s", url)
	}
	return output, resp.StatusCode, nil
}

// isClusterPresent returns true if the XDCR cluster is present, false if it is not present, and an error if it could not be determined.
func isClusterPresent(ctx context.Context, bucket *base.GocbV2Bucket) (bool, error) {
	method := http.MethodGet
	url := cbsRemoteClustersEndpoint
	output, statusCode, err := mgmtRequest(ctx, bucket, http.MethodGet, url, nil)
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

// deleteCluster deletes an XDCR cluster. The cluster must be present in order to delete it.
func deleteCluster(ctx context.Context, bucket *base.GocbV2Bucket) error {
	method := http.MethodDelete
	url := "/pools/default/remoteClusters/" + xdcrClusterName
	output, statusCode, err := mgmtRequest(ctx, bucket, method, url, nil)
	if err != nil {
		return err
	}

	if statusCode != http.StatusOK {
		return fmt.Errorf("Could not delete xdcr cluster: %s. %s %s -> (%d) %s", xdcrClusterName, http.MethodDelete, method, statusCode, output)
	}
	return nil
}

// createCluster deletes an XDCR cluster. The cluster must be present in order to delete it.
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
	output, statusCode, err := mgmtRequest(ctx, bucket, method, url, strings.NewReader(body.Encode()))
	if err != nil {
		return err
	}
	if statusCode != http.StatusOK {
		return fmt.Errorf("Could not create xdcr cluster: %s. %s %s -> (%d) %s", xdcrClusterName, method, url, statusCode, output)
	}
	return nil
}

// NewCouchbaseServerXDCR creates an instance of XDCR backed by Couchbase Server. This is not started until Start is called.
func NewCouchbaseServerXDCR(ctx context.Context, fromBucket *base.GocbV2Bucket, toBucket *base.GocbV2Bucket, mobileSetting string) (*CouchbaseServerXDCR, error) {
	if mobileSetting == "" {
		return nil, fmt.Errorf("must specify mobile setting in new XDCR clsuter")
	}
	isPresent, err := isClusterPresent(ctx, fromBucket)
	if err != nil {
		return nil, err

	}
	if isPresent {
		err := deleteCluster(ctx, fromBucket)
		if err != nil {
			return nil, err
		}
	}
	err = createCluster(ctx, fromBucket)
	if err != nil {
		return nil, err
	}
	return &CouchbaseServerXDCR{
		fromBucket:    fromBucket,
		toBucket:      toBucket,
		MobileSetting: mobileSetting,
	}, nil
}

// Start starts the XDCR replication.
func (x *CouchbaseServerXDCR) Start(ctx context.Context) error {
	method := http.MethodPost
	body := url.Values{}
	body.Add("name", xdcrClusterName)
	body.Add("fromBucket", x.fromBucket.GetName())
	body.Add("toBucket", x.toBucket.GetName())
	body.Add("toCluster", xdcrClusterName)
	body.Add("replicationType", "continuous")
	// if mobile setting is active, set the setting on thw replication
	if x.MobileSetting != MobileOff {
		body.Add("mobile", x.MobileSetting)
	} else {
		// filters all sync docs, except binary docs (attachments)
		body.Add("filterExpression", fmt.Sprintf("NOT REGEXP_CONTAINS(META().id, \"^%s\") OR REGEXP_CONTAINS(META().id, \"^%s\")", base.SyncDocPrefix, base.Att2Prefix))
	}
	url := "/controller/createReplication"
	output, statusCode, err := mgmtRequest(ctx, x.fromBucket, method, url, strings.NewReader(body.Encode()))
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
func (x *CouchbaseServerXDCR) Stop(ctx context.Context) error {
	method := http.MethodDelete
	url := "/controller/cancelXDCR/" + url.PathEscape(x.replicationID)
	output, statusCode, err := mgmtRequest(ctx, x.fromBucket, method, url, nil)
	if err != nil {
		return err
	}
	if statusCode != http.StatusOK {
		return fmt.Errorf("Could not cancel XDCR replication: %s. %s %s -> (%d) %s", x.replicationID, method, url, statusCode, output)
	}
	x.replicationID = ""
	return nil
}

// Stats returns the stats of the XDCR replication.
func (x *CouchbaseServerXDCR) Stats(ctx context.Context) (*Stats, error) {
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

func (x *CouchbaseServerXDCR) getValue(metrics *dto.MetricFamily) (uint64, error) {
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

func BucketSupportsMobileXDCR(bucket base.Bucket) bool {
	return bucket.IsSupported(sgbucket.BucketStoreFeatureMobileXDCR)
}
