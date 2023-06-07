/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"context"
	"fmt"
	"net/http"

	"github.com/couchbase/sync_gateway/db"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

// HTTP handler for incoming BLIP sync WebSocket request (/db/_blipsync)
func (h *handler) handleBLIPSync() error {
	needRelease, err := h.server.incrementConcurrentReplications(h.rqCtx)
	if err != nil {
		h.db.DbStats.Database().NumReplicationsRejectedLimit.Add(1)
		return err
	}
	// if we haven't incremented the active replicator due to MaxConcurrentReplications being 0, we don't need to decrement it
	if needRelease {
		defer h.server.decrementConcurrentReplications(h.rqCtx)
	}

	// Exit early when the connection can't be switched to websocket protocol.
	if _, ok := h.response.(http.Hijacker); !ok {
		base.DebugfCtx(h.ctx(), base.KeyHTTP, "Non-upgradable request received for BLIP+WebSocket protocol")
		return base.HTTPErrorf(http.StatusUpgradeRequired, "Can't upgrade this request to websocket connection")
	}

	h.db.DatabaseContext.DbStats.Database().NumReplicationsActive.Add(1)
	h.db.DatabaseContext.DbStats.Database().NumReplicationsTotal.Add(1)
	defer h.db.DatabaseContext.DbStats.Database().NumReplicationsActive.Add(-1)

	if c := h.server.Config.Replicator.BLIPCompression; c != nil {
		blip.CompressionLevel = *c
	}

	// Create a BLIP context:
	blipContext, err := db.NewSGBlipContext(h.ctx(), "")
	if err != nil {
		return err
	}

	// Overwrite the existing logging context with the blip context ID
	h.rqCtx = base.LogContextWith(h.ctx(), &base.LogContext{CorrelationID: base.FormatBlipContextID(blipContext.ID)})

	// Create a new BlipSyncContext attached to the given blipContext.
	ctx := db.NewBlipSyncContext(h.rqCtx, blipContext, h.db, h.formatSerialNumber(), db.BlipSyncStatsForCBL(h.db.DbStats))
	defer ctx.Close()

	if string(db.BLIPClientTypeSGR2) == h.getQuery(db.BLIPSyncClientTypeQueryParam) {
		ctx.SetClientType(db.BLIPClientTypeSGR2)
	} else {
		ctx.SetClientType(db.BLIPClientTypeCBL2)
	}

	// Create a BLIP WebSocket handler and have it handle the request:
	server := blipContext.WebSocketServer()

	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				h.logStatus(http.StatusSwitchingProtocols, fmt.Sprintf("[%s] Upgraded to WebSocket protocol %s+%s%s", blipContext.ID, blip.WebSocketSubProtocolPrefix, blipContext.ActiveSubprotocol(), h.formattedEffectiveUserName()))
				defer base.InfofCtx(h.ctx(), base.KeyHTTP, "%s:    --> BLIP+WebSocket connection closed", h.formatSerialNumber())
				next.ServeHTTP(w, r)
			})
	}

	middleware(server).ServeHTTP(h.response, h.rq)

	return nil
}

// incrementConcurrentReplications increments the number of active replications (if there is capacity to do so)
// and rejects calls if no capacity is available
func (sc *ServerContext) incrementConcurrentReplications(ctx context.Context) (bool, error) {
	// lock replications config limit + the active replications counter
	sc.ActiveReplicationsCounter.lock.Lock()
	defer sc.ActiveReplicationsCounter.lock.Unlock()
	// if max concurrent replications is 0 then we don't need to keep track of concurrent replications
	if sc.ActiveReplicationsCounter.activeReplicatorLimit == 0 {
		return false, nil
	}

	capacity := sc.ActiveReplicationsCounter.activeReplicatorLimit
	count := sc.ActiveReplicationsCounter.activeReplicatorCount

	if count >= capacity {
		base.InfofCtx(ctx, base.KeyHTTP, "Replication limit exceeded (active: %d limit: %d)", count, capacity)
		return false, base.ErrReplicationLimitExceeded
	}
	sc.ActiveReplicationsCounter.activeReplicatorCount++
	base.TracefCtx(ctx, base.KeyHTTP, "Acquired replication slot (active: %d/%d)", sc.ActiveReplicationsCounter.activeReplicatorCount, capacity)

	return true, nil
}

// decrementConcurrentReplications decrements the number of active replications on the server context
func (sc *ServerContext) decrementConcurrentReplications(ctx context.Context) {
	// lock replications config limit + the active replications counter
	sc.ActiveReplicationsCounter.lock.Lock()
	defer sc.ActiveReplicationsCounter.lock.Unlock()
	connections := sc.ActiveReplicationsCounter.activeReplicatorLimit
	sc.ActiveReplicationsCounter.activeReplicatorCount--
	base.TracefCtx(ctx, base.KeyHTTP, "Released replication slot (active: %d/%d)", sc.activeReplicatorCount, connections)
}
