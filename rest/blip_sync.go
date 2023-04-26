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

var ErrReplicationLimitExceeded = base.HTTPErrorf(http.StatusServiceUnavailable, "Replication limit exceeded. Try again later.")

func (sc *ServerContext) limitConcurrentReplications(ctx context.Context) (release func(), err error) {
	if sc.activeReplicationLimiter == nil {
		// allow everything
		return nil, nil
	}

	capacity := cap(sc.activeReplicationLimiter)

	select {
	case <-ctx.Done():
		// request was already cancelled
		base.TracefCtx(ctx, base.KeyHTTP, "Request cancelled before replication slot acquired")
		return nil, nil
	case sc.activeReplicationLimiter <- struct{}{}:
		base.TracefCtx(ctx, base.KeyHTTP, "Acquired replication slot (remaining: %d/%d)", capacity-len(sc.activeReplicationLimiter), capacity)
		return func() {
			<-sc.activeReplicationLimiter
			base.TracefCtx(ctx, base.KeyHTTP, "Released replication slot (remaining: %d/%d)", capacity-len(sc.activeReplicationLimiter), capacity)
		}, nil
	default:
		base.InfofCtx(ctx, base.KeyHTTP, "Replication limit exceeded (active: %d)", len(sc.activeReplicationLimiter))
		return nil, ErrReplicationLimitExceeded
	}
}

// HTTP handler for incoming BLIP sync WebSocket request (/db/_blipsync)
func (h *handler) handleBLIPSync() error {
	if release, err := h.server.limitConcurrentReplications(h.rqCtx); err != nil {
		return err
	} else if release != nil {
		defer release()
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
