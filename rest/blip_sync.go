package rest

import (
	"context"
	"fmt"
	"net/http"

	"github.com/couchbase/sync_gateway/db"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"golang.org/x/net/websocket"
)

// HTTP handler for incoming BLIP sync WebSocket request (/db/_blipsync)
func (h *handler) handleBLIPSync() error {

	h.db.DatabaseContext.DbStats.StatsDatabase().Add(base.StatKeyNumReplicationsActive, 1)
	h.db.DatabaseContext.DbStats.StatsDatabase().Add(base.StatKeyNumReplicationsTotal, 1)
	defer h.db.DatabaseContext.DbStats.StatsDatabase().Add(base.StatKeyNumReplicationsActive, -1)

	if c := h.server.GetConfig().ReplicatorCompression; c != nil {
		blip.CompressionLevel = *c
	}

	// Create a BLIP context:
	blipContext := db.NewSGBlipContext(h.db.Ctx, "")

	// Overwrite the existing logging context with the blip context ID
	h.db.Ctx = context.WithValue(h.db.Ctx, base.LogContextKey{},
		base.LogContext{CorrelationID: base.FormatBlipContextID(blipContext.ID)},
	)

	// Create a new BlipSyncContext attached to the given blipContext.
	ctx := db.NewBlipSyncContext(blipContext, h.db, h.formatSerialNumber(), db.BlipSyncStatsForCBL(h.db.DbStats))
	defer ctx.Close()

	// Create a BLIP WebSocket handler and have it handle the request:
	server := blipContext.WebSocketServer()
	defaultHandler := server.Handler
	server.Handler = func(conn *websocket.Conn) {
		h.logStatus(http.StatusSwitchingProtocols, fmt.Sprintf("[%s] Upgraded to BLIP+WebSocket protocol%s", blipContext.ID, h.formattedEffectiveUserName()))
		defer func() {
			_ = conn.Close() // in case it wasn't closed already
			base.InfofCtx(h.db.Ctx, base.KeyHTTP, "%s:    --> BLIP+WebSocket connection closed", h.formatSerialNumber())
		}()
		defaultHandler(conn)
	}

	server.ServeHTTP(h.response, h.rq)
	return nil
}
