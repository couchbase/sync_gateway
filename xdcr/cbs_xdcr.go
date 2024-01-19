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

	"github.com/couchbase/sync_gateway/base"
)

const (
	cbsRemoteClustersEndpoint = "/pools/default/remoteClusters"
	xdcrClusterName           = "sync_gateway_xdcr"
)

// couchbaseServerXDCR implements a XDCR setup cluster on Couchbase Server.
type CouchbaseServerXDCR struct {
	fromBucket    *base.GocbV2Bucket
	toBucket      *base.GocbV2Bucket
	replicationID string
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
func NewCouchbaseServerXDCR(ctx context.Context, fromBucket *base.GocbV2Bucket, toBucket *base.GocbV2Bucket) (*CouchbaseServerXDCR, error) {
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
		fromBucket: fromBucket,
		toBucket:   toBucket,
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
	// filters all sync docs, except binary docs (attachments)
	body.Add("filterExpression", fmt.Sprintf("NOT REGEXP_CONTAINS(META().id, \"^%s\") OR REGEXP_CONTAINS(META().id, \"^%s\")", base.SyncDocPrefix, base.Att2Prefix))
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