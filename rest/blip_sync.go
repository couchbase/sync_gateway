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
	// Exit early when the connection can't be switched to websocket protocol.
	if _, ok := h.response.(http.Hijacker); !ok {
		base.DebugfCtx(h.db.Ctx, base.KeyHTTP, "Non-upgradable request received for BLIP+WebSocket protocol")
		return base.HTTPErrorf(http.StatusUpgradeRequired, "Can't upgrade this request to websocket connection")
	}

	h.db.DatabaseContext.DbStats.Database().NumReplicationsActive.Add(1)
	h.db.DatabaseContext.DbStats.Database().NumReplicationsTotal.Add(1)
	defer h.db.DatabaseContext.DbStats.Database().NumReplicationsActive.Add(-1)

	if c := h.server.config.Replicator.BLIPCompression; c != nil {
		blip.CompressionLevel = *c
	}

	// Create a BLIP context:
	blipContext, err := db.NewSGBlipContext(h.db.Ctx, "")
	if err != nil {
		return err
	}

	// Overwrite the existing logging context with the blip context ID
	h.db.Ctx = context.WithValue(h.db.Ctx, base.LogContextKey{},
		base.LogContext{CorrelationID: base.FormatBlipContextID(blipContext.ID)},
	)

	// Create a new BlipSyncContext attached to the given blipContext.
	ctx := db.NewBlipSyncContext(blipContext, h.db, h.formatSerialNumber(), db.BlipSyncStatsForCBL(h.db.DbStats))
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
				defer base.InfofCtx(h.db.Ctx, base.KeyHTTP, "%s:    --> BLIP+WebSocket connection closed", h.formatSerialNumber())
				next.ServeHTTP(w, r)
			})
	}

	middleware(server).ServeHTTP(h.response, h.rq)

	return nil
}
