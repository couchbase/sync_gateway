// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterCompatRootEndpoint(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	// Trigger a refresh now that a database/bucket exists
	ccm, ok := rt.ServerContext().ClusterCompat.(*clusterCompatManager)
	require.True(t, ok)
	ccm.Refresh(base.TestCtx(t))

	// Admin port should include cluster_compat_version
	resp := rt.SendAdminRequest(http.MethodGet, "/", "")
	RequireStatus(t, resp, http.StatusOK)

	var rootResp rootResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &rootResp)
	require.NoError(t, err)
	assert.Equal(t, base.NodeClusterCompatVersion.String(), rootResp.ClusterCompatVersion)

	// Public port also shows cluster_compat_version by default (HideProductVersion is false)
	pubResp := rt.SendRequest(http.MethodGet, "/", "")
	RequireStatus(t, pubResp, http.StatusOK)
	var pubRootResp rootResponse
	err = base.JSONUnmarshal(pubResp.BodyBytes(), &pubRootResp)
	require.NoError(t, err)
	assert.Equal(t, base.NodeClusterCompatVersion.String(), pubRootResp.ClusterCompatVersion)
}

func TestClusterCompatEndpoint(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	// Trigger a refresh now that a database/bucket exists
	ccm, ok := rt.ServerContext().ClusterCompat.(*clusterCompatManager)
	require.True(t, ok)
	ccm.Refresh(base.TestCtx(t))

	resp := rt.SendAdminRequest(http.MethodGet, "/_cluster_compat", "")
	RequireStatus(t, resp, http.StatusOK)

	var ccResp clusterCompatResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &ccResp)
	require.NoError(t, err)

	// Should have a cluster compat version matching this node's version
	require.NotNil(t, ccResp.ClusterCompatVersion)
	assert.Equal(t, base.NodeClusterCompatVersion, *ccResp.ClusterCompatVersion)

	// Should have at least one node (this node)
	assert.GreaterOrEqual(t, len(ccResp.Nodes), 1)

	// Should have at least one bucket with node registrations
	assert.GreaterOrEqual(t, len(ccResp.Buckets), 1)

	// Verify this node appears in at least one bucket
	nodeUUID := rt.ServerContext().NodeUUID
	foundNode := false
	for _, bucket := range ccResp.Buckets {
		if _, ok := bucket.Nodes[nodeUUID]; ok {
			foundNode = true
			break
		}
	}
	assert.True(t, foundNode, "This node's UUID should appear in at least one bucket's node registrations")
}

func TestClusterCompatEndpointRequiresAdmin(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	resp := rt.SendRequest(http.MethodGet, "/_cluster_compat", "")
	assert.Equal(t, http.StatusNotFound, resp.Code, "/_cluster_compat should not be accessible on public port")
}
