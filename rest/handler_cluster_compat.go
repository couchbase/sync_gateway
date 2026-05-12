// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"errors"
	"net/http"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// ClusterCompatVersionState is the response payload for /_cluster_compat_version. It
// describes the cluster-wide cluster compatibility version, the per-node versions
// registered in the cluster, and (only when set) the frozen value pinning it.
type ClusterCompatVersionState struct {
	ClusterCompatVersion       *base.ClusterCompatVersion           `json:"cluster_compat_version,omitempty"`
	Nodes                      map[string]base.ClusterCompatVersion `json:"nodes,omitempty"`
	FrozenClusterCompatVersion *base.ClusterCompatVersion           `json:"frozen_cluster_compat_version,omitempty"`
}

// handleGetClusterCompatVersion returns the cluster compatibility version state.
func (h *handler) handleGetClusterCompatVersion() error {
	mgr, err := h.requireClusterCompatManager()
	if err != nil {
		return err
	}
	state := buildClusterCompatVersionState(mgr)
	base.Audit(h.ctx(), base.AuditIDClusterCompatVersionRead, nil)
	h.writeJSON(state)
	return nil
}

// handleFreezeClusterCompatVersion captures the current cluster compatibility version and
// pins the cluster to it. Success requires every tracked bucket accept the freeze; on
// partial failure the current state is written as the body of a 503 response so the admin
// can see which version (if any) ended up pinned.
//
// Audits only on full success — partial-failure attempts are still captured by the standard
// admin HTTP API request audit (AuditIDAdminHTTPAPIRequest).
func (h *handler) handleFreezeClusterCompatVersion() error {
	mgr, err := h.requireClusterCompatManager()
	if err != nil {
		return err
	}
	freeze, err := mgr.Freeze(h.ctx())
	if err != nil {
		switch {
		case errors.Is(err, ErrFreezeNoVersion):
			return base.HTTPErrorf(http.StatusServiceUnavailable, "cluster compatibility version not yet computed; retry once GET /_cluster_compat_version returns a version")
		case errors.Is(err, ErrFreezeNoBucketsWritten):
			return base.HTTPErrorf(http.StatusServiceUnavailable, "could not freeze cluster compatibility version: no tracked bucket registries")
		case errors.Is(err, ErrFreezePartial):
			h.writeJSONStatus(http.StatusServiceUnavailable, buildClusterCompatVersionState(mgr))
			return nil
		default:
			return base.HTTPErrorf(http.StatusInternalServerError, "failed to freeze cluster compatibility version: %v", err)
		}
	}
	base.Audit(h.ctx(), base.AuditIDClusterCompatVersionFreeze, base.AuditFields{
		base.AuditFieldClusterCompatVersion: freeze.Version.String(),
		base.AuditFieldFrozenAt:             freeze.FrozenAt.Format(time.RFC3339),
	})
	state := buildClusterCompatVersionState(mgr)
	h.writeJSON(state)
	return nil
}

// handleUnfreezeClusterCompatVersion clears the cluster compatibility version freeze.
// Strict contract: success only if the cluster is fully unfrozen. On partial failure the
// current state is written as the body of a 503 response so the admin can see what is
// still held back.
//
// Audits only on full success — matching freeze, where the audit records the action
// having taken effect. Partial-failure attempts are still captured by the standard admin
// HTTP API request audit (AuditIDAdminHTTPAPIRequest).
func (h *handler) handleUnfreezeClusterCompatVersion() error {
	mgr, err := h.requireClusterCompatManager()
	if err != nil {
		return err
	}
	cleared, _, err := mgr.Unfreeze(h.ctx())
	if err != nil {
		if errors.Is(err, ErrUnfreezePartial) {
			h.writeJSONStatus(http.StatusServiceUnavailable, buildClusterCompatVersionState(mgr))
			return nil
		}
		return base.HTTPErrorf(http.StatusInternalServerError, "failed to clear cluster compatibility version freeze: %v", err)
	}
	auditFields := base.AuditFields{}
	if cleared != nil {
		auditFields[base.AuditFieldClusterCompatVersion] = cleared.Version.String()
		auditFields[base.AuditFieldFrozenAt] = cleared.FrozenAt.Format(time.RFC3339)
	}
	base.Audit(h.ctx(), base.AuditIDClusterCompatVersionUnfreeze, auditFields)
	state := buildClusterCompatVersionState(mgr)
	h.writeJSON(state)
	return nil
}

// requireClusterCompatManager returns the running clusterCompatManager, or a 503 if none is
// installed (e.g. when running without persistent config and CCV is not initialised).
func (h *handler) requireClusterCompatManager() (*clusterCompatManager, error) {
	mgr := h.server.ClusterCompat
	if mgr == nil {
		return nil, base.HTTPErrorf(http.StatusServiceUnavailable, "cluster compatibility version tracking is not enabled on this node")
	}
	return mgr, nil
}

// buildClusterCompatVersionState assembles the response payload from the manager's
// currently-cached state. Per-node timestamps are deliberately not exposed.
func buildClusterCompatVersionState(mgr *clusterCompatManager) ClusterCompatVersionState {
	state := ClusterCompatVersionState{
		ClusterCompatVersion: mgr.ClusterCompatVersion(),
		Nodes:                mgr.NodeVersions(),
	}
	if freeze := mgr.getCachedFreeze(); freeze != nil {
		v := freeze.Version
		state.FrozenClusterCompatVersion = &v
	}
	return state
}
