// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build !race

package rest

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterCompatFreezeRESTRoundTrip exercises the admin REST endpoints end-to-end and
// verifies each one emits its expected audit event. Audit logging is EE-only, so this test
// is skipped on CE.
func TestClusterCompatFreezeRESTRoundTrip(t *testing.T) {
	rt := createAuditLoggingRestTester(t)
	defer rt.Close()
	RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)

	ctx := base.TestCtx(t)
	ccm := rt.ServerContext().ClusterCompat
	require.NotNil(t, ccm)
	ccm.Refresh(ctx)

	// GET before freeze: cluster_compat_version + nodes, no frozen_cluster_compat_version.
	var initial ClusterCompatVersionState
	output := base.AuditLogContents(t, func(t testing.TB) {
		resp := rt.SendAdminRequest(http.MethodGet, "/_cluster_compat_version", "")
		RequireStatus(t, resp, http.StatusOK)
		require.NoError(t, base.JSONUnmarshal(resp.BodyBytes(), &initial))
	})
	require.NotNil(t, initial.ClusterCompatVersion)
	assert.Equal(t, base.NodeClusterCompatVersion, *initial.ClusterCompatVersion)
	assert.NotEmpty(t, initial.Nodes)
	assert.Nil(t, initial.FrozenClusterCompatVersion)
	requireAuditEventPresent(t, output, base.AuditIDClusterCompatVersionRead)

	// POST /freeze: returns 200 with frozen_cluster_compat_version populated.
	var frozen ClusterCompatVersionState
	output = base.AuditLogContents(t, func(t testing.TB) {
		resp := rt.SendAdminRequest(http.MethodPost, "/_cluster_compat_version/freeze", "")
		RequireStatus(t, resp, http.StatusOK)
		require.NoError(t, base.JSONUnmarshal(resp.BodyBytes(), &frozen))
	})
	require.NotNil(t, frozen.FrozenClusterCompatVersion)
	assert.Equal(t, base.NodeClusterCompatVersion, *frozen.FrozenClusterCompatVersion)
	requireAuditEventPresent(t, output, base.AuditIDClusterCompatVersionFreeze)

	// GET after freeze: same shape.
	resp := rt.SendAdminRequest(http.MethodGet, "/_cluster_compat_version", "")
	RequireStatus(t, resp, http.StatusOK)
	var afterFreeze ClusterCompatVersionState
	require.NoError(t, base.JSONUnmarshal(resp.BodyBytes(), &afterFreeze))
	require.NotNil(t, afterFreeze.FrozenClusterCompatVersion)

	// POST /unfreeze: 200 with frozen_cluster_compat_version omitted.
	var unfrozen ClusterCompatVersionState
	output = base.AuditLogContents(t, func(t testing.TB) {
		resp := rt.SendAdminRequest(http.MethodPost, "/_cluster_compat_version/unfreeze", "")
		RequireStatus(t, resp, http.StatusOK)
		require.NoError(t, base.JSONUnmarshal(resp.BodyBytes(), &unfrozen))
	})
	assert.Nil(t, unfrozen.FrozenClusterCompatVersion)
	requireAuditEventPresent(t, output, base.AuditIDClusterCompatVersionUnfreeze)
}

// requireAuditEventPresent asserts that the given audit log buffer contains at least one
// event with the expected audit ID.
func requireAuditEventPresent(t testing.TB, output []byte, expected base.AuditID) {
	t.Helper()
	for _, event := range jsonLines(t, output) {
		id, ok := event["id"]
		if !ok {
			continue
		}
		if base.AuditID(id.(float64)) == expected {
			return
		}
	}
	t.Fatalf("expected audit event %d (%s) not found in audit log", expected, base.AuditEvents[expected].Name)
}
