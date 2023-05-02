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
	if release, err := h.server.limitConcurrentReplications(h.rqCtx); err != nil {
		h.db.DbStats.Database().NumReplicationsRejected.Add(1)
		return err
	} else if release != nil {
		defer release(h.server)
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

func (sc *ServerContext) limitConcurrentReplications(ctx context.Context) (func(sc *ServerContext), error) {
	if sc.Config.Replicator.MaxConcurrentConnections == 0 {
		return nil, nil
	}
	sc.lock.Lock()
	defer sc.lock.Unlock()

	capacity := sc.Config.Replicator.MaxConcurrentConnections
	count := sc.activeReplicatorCount
	//base.InfofCtx(ctx, base.KeyHTTP, "count %v + capacity %v", count, capacity)

	release := func(sc *ServerContext) {
		sc.lock.Lock()
		defer sc.lock.Unlock()
		connections := sc.Config.Replicator.MaxConcurrentConnections
		sc.activeReplicatorCount--
		base.TracefCtx(ctx, base.KeyHTTP, "Released replication slot (remaining: %d/%d)", connections-sc.activeReplicatorCount, connections)
	}

	if count >= capacity {
		base.InfofCtx(ctx, base.KeyHTTP, "Replication limit exceeded (active: %d)", count)
		return nil, base.ErrReplicationLimitExceeded
	} else if count < capacity {
		sc.activeReplicatorCount++
		base.TracefCtx(ctx, base.KeyHTTP, "Acquired replication slot (remaining: %d/%d)", capacity-sc.activeReplicatorCount, capacity)
	}
	return release, nil
}
