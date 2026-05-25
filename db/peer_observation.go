// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"context"

	"github.com/couchbase/sync_gateway/base"
)

// ObservePreCCVAwarePeers collects observations of pre-CCV-aware Sync Gateway peers visible to
// this database through the two side-channels that pre-4.1 nodes populate:
//
//  1. cbgt.NODE_DEFS_KNOWN — peers running sharded import (EE).
//  2. SGRCluster.Nodes — peers participating in inter-SG replication (ISGR).
//
// Discrimination is by self-published version on the side-channel entry itself: a peer whose
// version is at least ccvAwareMajor.ccvAwareMinor is treated as CCV-aware and skipped (it
// self-registers in GatewayRegistry.Nodes via its own RegisterNodeVersion call). Peers below
// the threshold are emitted as pre-CCV-aware-peer entries. Entries whose UUID is this database's
// local db.UUID are excluded as defence in depth — the local node's own cbgt/ISGR entries
// are version-tagged and would already be filtered, but excluding by UUID guards against any
// transient pre-write window.
//
// The cbgt side-channel always yields a non-nil version: pre-3.1 peers (which predate cbgt
// extras Version stamping, CBG-2213) are stamped as a fake 3.0 by base.getNodeVersion. The
// SGRCluster side-channel can still carry nil for pre-4.1 peers that wrote SGNode entries
// without the Version field — those entries are substituted with PreSGNodeVersionFallback at
// this layer so the downstream registry always carries a concrete ClusterCompatVersion. CCV
// decisions don't need precision beyond major.minor, so the observed build version is
// collapsed at write time.
//
// Returns a map keyed by pre-CCV-aware peer UUID, deduped across side-channels. When the same
// peer appears in both cbgt and SGRCluster, the lower-versioned reading wins — observed
// inputs are upgrade-fan-in, so the more conservative reading is the correct CCV input.
//
// Errors from either side-channel enumeration are logged at Warn and the partial result is
// returned — the caller's CCV cap still functions correctly with whichever observations did
// succeed.
func (db *DatabaseContext) ObservePreCCVAwarePeers(ctx context.Context, ccvAwareMajor, ccvAwareMinor uint8) map[string]base.RegistryPreCCVAwareNode {
	out := make(map[string]base.RegistryPreCCVAwareNode)

	upsert := func(uuid string, version *base.ComparableBuildVersion) {
		if uuid == db.UUID {
			return
		}
		if version != nil && version.AtLeastMinorVersion(ccvAwareMajor, ccvAwareMinor) {
			return
		}
		var ccv base.ClusterCompatVersion
		if version != nil {
			ccv = version.ClusterCompatVersion()
		} else {
			ccv = base.PreSGNodeVersionFallback
		}
		if existing, ok := out[uuid]; ok {
			if existing.Version.GreaterThan(ccv) {
				existing.Version = ccv
				out[uuid] = existing
			}
			return
		}
		out[uuid] = base.RegistryPreCCVAwareNode{Version: ccv}
	}

	if db.ImportListener != nil && db.ImportListener.cbgtContext != nil {
		cbgtNodes, err := base.ListCbgtNodes(ctx, db.ImportListener.cbgtContext.Cfg)
		if err != nil {
			base.WarnfCtx(ctx, "Failed to enumerate cbgt nodes for pre-CCV-aware peer observation on database %s: %v", base.MD(db.Name), err)
		} else {
			for _, node := range cbgtNodes {
				upsert(node.UUID, node.Version)
			}
		}
	}

	if db.SGReplicateMgr != nil {
		sgrNodes, err := db.SGReplicateMgr.getNodes()
		if err != nil {
			base.WarnfCtx(ctx, "Failed to enumerate SGRCluster nodes for pre-CCV-aware peer observation on database %s: %v", base.MD(db.Name), err)
		} else {
			for _, node := range sgrNodes {
				if node == nil {
					continue
				}
				upsert(node.UUID, node.Version)
			}
		}
	}

	if len(out) == 0 {
		return nil
	}
	return out
}
