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
	"net/url"

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
	if !h.response.isHijackable() {
		base.InfofCtx(h.ctx(), base.KeyHTTP, "Non-upgradable request received for BLIP+WebSocket protocol")
		return base.HTTPErrorf(http.StatusUpgradeRequired, "Can't upgrade this request to websocket connection")
	}

	h.db.DatabaseContext.DbStats.Database().NumReplicationsActive.Add(1)
	h.db.DatabaseContext.DbStats.Database().NumReplicationsTotal.Add(1)
	defer h.db.DatabaseContext.DbStats.Database().NumReplicationsActive.Add(-1)

	if c := h.server.Config.Replicator.BLIPCompression; c != nil {
		blip.CompressionLevel = *c
	}

	// error is checked at the time of database load, and ignored at this time
	originPatterns, _ := hostOnlyCORS(h.db.CORS.Origin)

	// Create a BLIP context:
	blipContext, err := db.NewSGBlipContext(h.ctx(), "", originPatterns)
	if err != nil {
		return err
	}

	// Overwrite the existing logging context with the blip context ID
	h.rqCtx = base.CorrelationIDLogCtx(h.ctx(), base.FormatBlipContextID(blipContext.ID))
	h.response.Header().Set(db.BLIPCorrelationIDResponseHeader, blipContext.ID)

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

	server.PostHandshakeCallback = func(err error) {
		if err != nil {
			base.InfofCtx(h.ctx(), base.KeyHTTP, "%s:    --> BLIP+WebSocket handshake failed: %v", h.formatSerialNumber(), err)
			return
		}
		// ActiveSubprotocol only available after handshake via ServeHTTP(), so have to get go-blip to invoke callback between handshake and serving BLIP messages
		subprotocol := blipContext.ActiveSubprotocol()
		h.logStatus(http.StatusSwitchingProtocols, fmt.Sprintf("[%s] Upgraded to WebSocket protocol %s+%s%s", blipContext.ID, blip.WebSocketSubProtocolPrefix, subprotocol, h.formattedEffectiveUserName()))
		err = ctx.SetActiveCBMobileSubprotocol(subprotocol)
		if err != nil {
			base.WarnfCtx(h.ctx(), "Couldn't set active CB Mobile Subprotocol: %v", err)
		}
	}

	server.ServeHTTP(h.response, h.rq)
	base.InfofCtx(h.ctx(), base.KeyHTTP, "%s:    --> BLIP+WebSocket connection closed", h.formatSerialNumber())

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

// hostOnlyCORS returns the host portion of the origin URL, suitable for passing to websocket library.
func hostOnlyCORS(originPatterns []string) ([]string, error) {
	var origins []string
	var multiError *base.MultiError
	for _, origin := range originPatterns {
		// this is a special pattern for allowing all origins
		if origin == "*" {
			origins = append(origins, origin)
			continue
		}
		u, err := url.Parse(origin)
		if err != nil {
			multiError = multiError.Append(fmt.Errorf("%s is not a valid pattern for CORS config", err))
			continue
		}
		origins = append(origins, u.Host)
	}
	return origins, multiError.ErrorOrNil()
}
