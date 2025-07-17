// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
)

// tbpCluster represents a gocb v2 cluster
type tbpCluster struct {
	// version is the Couchbase Server version
	version ComparableBuildVersion
	// ee is true if the Couchbase Server is Enterprise Edition
	ee bool
	// clusterSpec is the authentication and connection information for the cluster.
	clusterSpec CouchbaseClusterSpec
	// agent is a gocbcore agent for the cluster.
	agent *gocbcore.Agent
}

// newTestCluster returns a cluster based on the driver used by the defaultBucketSpec.
func newTestCluster(ctx context.Context, clusterSpec CouchbaseClusterSpec) (*tbpCluster, error) {
	agent, err := NewClusterAgent(ctx, clusterSpec)
	if err != nil {
		return nil, fmt.Errorf("couldn't create cluster agent: %w", err)
	}
	version, ee, err := getCouchbaseServerVersion(agent, clusterSpec)
	if err != nil {
		err := fmt.Errorf("couldn't get cluster version: %w", err)
		closeErr := agent.Close()
		if closeErr != nil {
			err = fmt.Errorf("%w; couldn't close agent: %v", err, closeErr)
		}
		return nil, err
	}
	return &tbpCluster{
		clusterSpec: clusterSpec,
		agent:       agent,
		version:     *version,
		ee:          ee,
	}, nil
}

// getGocbClusterForTest makes cluster connection. Callers must close. Returns the cluster and the connection string used to connect.
func getGocbClusterForTest(ctx context.Context, clusterSpec CouchbaseClusterSpec) (*gocb.Cluster, string, error) {
	connSpec, err := getGoCBConnSpec(clusterSpec.Server, &GoCBConnStringParams{
		KvPoolSize: DefaultGocbKvPoolSize,
	})
	if err != nil {
		return nil, "", fmt.Errorf("couldn't parse connection string %q: %w", clusterSpec.Server, err)
	}

	securityConfig, err := GoCBv2SecurityConfig(ctx, &clusterSpec.TLSSkipVerify, clusterSpec.CACertpath)
	if err != nil {
		return nil, "", fmt.Errorf("couldn't initialize cluster security config: %w", err)
	}

	authenticatorConfig, authErr := GoCBv2Authenticator(clusterSpec.Username, clusterSpec.Password, clusterSpec.X509Certpath, clusterSpec.X509Keypath)
	if authErr != nil {
		return nil, "", fmt.Errorf("couldn't initialize cluster authenticator config: %w", authErr)
	}

	// use longer timeout than DefaultBucketOpTimeout to avoid timeouts in test harness from using buckets after flush, which takes some time to reinitialize
	bucketOpTimeout := 30 * time.Second
	timeoutsConfig := GoCBv2TimeoutsConfig(&bucketOpTimeout, Ptr(DefaultViewTimeout))

	clusterOptions := gocb.ClusterOptions{
		Authenticator:  authenticatorConfig,
		SecurityConfig: securityConfig,
		TimeoutsConfig: timeoutsConfig,
	}

	connStr := connSpec.String()
	cluster, err := gocb.Connect(connStr, clusterOptions)
	if err != nil {
		return nil, "", fmt.Errorf("couldn't connect to cluster %q: %w", connStr, err)
	}
	const clusterReadyTimeout = 90 * time.Second
	err = cluster.WaitUntilReady(clusterReadyTimeout, nil)
	if err != nil {
		FatalfCtx(ctx, "Cluster not ready after %ds: %v", int(clusterReadyTimeout.Seconds()), err)
	}
	return cluster, connStr, nil
}

// isServerEnterprise returns true if the connected returns true if the connected couchbase server
// instance is Enterprise edition And false for Community edition
func (c *tbpCluster) isServerEnterprise() bool {
	return c.ee
}

// getBucketNames returns the names of all buckets
func (c *tbpCluster) getBucketNames() ([]string, error) {
	output, status, err := c.MgmtRequest(
		http.MethodGet,
		"/pools/default/buckets",
		ContentTypeJSON,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("couldn't get buckets: %w", err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("couldn't get buckets (error code %d) %s", status, output)
	}
	type bucket struct {
		Name string `json:"name"`
	}
	var buckets []bucket
	if err := json.Unmarshal(output, &buckets); err != nil {
		return nil, fmt.Errorf("couldn't unmarshal buckets from %s: %w", output, err)
	}
	names := make([]string, 0, len(buckets))
	for _, b := range buckets {
		names = append(names, b.Name)
	}
	return names, nil
}

// MgmtRequest sends a management request to the cluster and returns the response body, status code, and error.
func (c *tbpCluster) MgmtRequest(method, path string, contentType string, body io.Reader) ([]byte, int, error) {
	mgmtEps := c.agent.MgmtEps()
	if len(mgmtEps) == 0 {
		return nil, 0, fmt.Errorf("no management endpoints available for cluster %q", c.clusterSpec.Server)
	}
	return MgmtRequest(
		c.agent.HTTPClient(),
		mgmtEps[0],
		method,
		path,
		contentType,
		c.clusterSpec.Username,
		c.clusterSpec.Password,
		body,
	)
}

// getCouchbaseServerVersion retrieves the Couchbase Server version via a gocbcore.Agent
func getCouchbaseServerVersion(agent *gocbcore.Agent, clusterSpec CouchbaseClusterSpec) (version *ComparableBuildVersion, ee bool, err error) {
	mgmtEps := agent.MgmtEps()
	if len(mgmtEps) == 0 {
		return nil, false, fmt.Errorf("no management endpoints available")
	}
	output, status, err := MgmtRequest(
		agent.HTTPClient(),
		mgmtEps[0],
		http.MethodGet,
		"/pools/default",
		"application/x-www-form-urlencoded",
		clusterSpec.Username,
		clusterSpec.Password,
		nil,
	)
	if err != nil {
		return nil, false, fmt.Errorf("couldn't get Couchbase Server version: %w", err)
	}
	if status != http.StatusOK {
		return nil, false, fmt.Errorf("couldn't get Couchbase Server version (error code %d): %s", status, output)
	}
	type nodeMetadata struct {
		ClusterCompatibility int    `json:"clusterCompatibility"`
		Version              string `json:"version"`
	}
	clusterCfg := struct {
		Nodes []nodeMetadata `json:"nodes"`
	}{}
	if err := json.Unmarshal(output, &clusterCfg); err != nil {
		return nil, false, fmt.Errorf("couldn't unmarshal cluster metadata: %w", err)
	}
	if len(clusterCfg.Nodes) == 0 {
		return nil, false, fmt.Errorf("no nodes found in cluster metadata")
	}
	// string is x.y.z-aaaa-enterprise or x.y.z-aaaa-community
	// convert to a comparable string that Sync Gateway understands x.y.z@aaaa
	components := strings.Split(clusterCfg.Nodes[0].Version, "-")
	vrs := components[0]
	if len(components) > 1 {
		vrs += "@" + components[1]
	}

	version, err = NewComparableBuildVersionFromString(vrs)
	if err != nil {
		return nil, false, fmt.Errorf("couldn't parse Couchbase Server version %q: %w", vrs, err)
	}
	// convert the above string into a comparable string
	return version, strings.Contains(clusterCfg.Nodes[0].Version, "enterprise"), nil
}

// insertBucket creates a bucket into Couchbase Server. This operation is asynchronous, so will continue after this function is called.
func (c *tbpCluster) insertBucket(name string, quotaMB int, conflictResolution XDCRConflictResolutionStrategy) error {
	numReplicas, err := tbpNumReplicas()
	if err != nil {
		return err
	}
	body := url.Values{}
	body.Set("bucketType", "couchbase")
	if c.isServerEnterprise() {
		// default is MWW which is "seqno" str
		// LWW is only supported on Enterprise Edition
		switch conflictResolution {
		case XDCRConflictResolutionStrategyLWW:
			body.Set("conflictResolutionType", "lww")
		case XDCRConflictResolutionStrategyMWW:
			body.Set("conflictResolutionType", "seqno")
		default:
			return fmt.Errorf("unsupported conflict resolution strategy %q", conflictResolution)
		}
	}
	body.Set("flushEnabled", "1")
	body.Set("name", name)
	body.Set("numReplicas", strconv.Itoa(numReplicas))
	body.Set("ramQuotaMB", fmt.Sprintf("%d", quotaMB))
	output, status, err := c.MgmtRequest(
		http.MethodPost,
		"/pools/default/buckets",
		ContentTypeFormEncoded,
		strings.NewReader(body.Encode()),
	)
	if err != nil {
		return fmt.Errorf("couldn't create bucket %q: %w", name, err)
	}
	if status != http.StatusAccepted {
		return fmt.Errorf("couldn't create bucket %q: (error code %d) %s", name, status, output)
	}
	return nil
}

// removeBucket deletes a bucket from Couchbase Server. This operation is asynchronous, so will continue after this function is called.
func (c *tbpCluster) removeBucket(name string) error {
	output, status, err := c.MgmtRequest(
		http.MethodDelete,
		"/pools/default/buckets/"+name,
		ContentTypeJSON,
		nil,
	)
	if err != nil {
		return fmt.Errorf("couldn't remove bucket: %w", err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("couldn't remove bucket (error code %d): %s", status, output)
	}
	return nil
}

// openTestBucket opens the bucket of the given name for the gocb cluster in the given TestBucketPool.
func (c *tbpCluster) openTestBucket(ctx context.Context, testBucketName tbpBucketName, waitUntilReady time.Duration) (Bucket, error) {

	bucketCluster, connstr, err := getGocbClusterForTest(ctx, c.clusterSpec)
	if err != nil {
		return nil, fmt.Errorf("couldn't get gocb cluster for test: %w", err)
	}

	bucketSpec := getTestBucketSpec(c.clusterSpec, testBucketName)

	bucketFromSpec, err := GetGocbV2BucketFromCluster(ctx, bucketCluster, bucketSpec, connstr, waitUntilReady, false)
	if err != nil {
		return nil, err
	}

	// add whether bucket is mobile XDCR ready to bucket object
	bucketFromSpec.supportsHLV = true

	return bucketFromSpec, nil
}

// close shuts down the running gocbcore agent for the cluster.
func (c *tbpCluster) close() error {
	return c.agent.Close()
}

// supportsMobileRBAC is true if running couchbase server with all Sync Gateway roles
func (c *tbpCluster) supportsMobileRBAC() bool {
	return c.isServerEnterprise()
}

// getTestClusterSpec returns the connection parameters to connect to a Couchbase Server during an integration test.
func getTestClusterSpec() (*CouchbaseClusterSpec, error) {
	clusterSpecPath := os.Getenv("SG_TEST_CLUSTER_SPEC")
	var spec CouchbaseClusterSpec
	if clusterSpecPath != "" {
		contents, err := os.ReadFile(clusterSpecPath)
		if err != nil {
			return nil, fmt.Errorf("couldn't read cluster spec file %q: %w", clusterSpecPath, err)
		}
		err = json.Unmarshal(contents, &spec)
		if err != nil {
			return nil, fmt.Errorf("couldn't unmarshal cluster spec file %q: %w", clusterSpecPath, err)
		}
	} else {
		spec.Username = TestClusterUsername()
		spec.Password = TestClusterPassword()
		tlsSkipVerify, isSet := os.LookupEnv(TestEnvTLSSkipVerify)
		if isSet {
			val, err := strconv.ParseBool(tlsSkipVerify)
			if err != nil {
				return nil, err
			}
			spec.TLSSkipVerify = val
		} else {
			spec.TLSSkipVerify = DefaultTestTLSSkipVerify
		}
		spec.Server = UnitTestUrl()
	}
	return &spec, nil
}
