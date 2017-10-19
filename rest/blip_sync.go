package rest

import (
	"github.com/couchbase/sync_gateway/base"
	"golang.org/x/net/websocket"
	"net/http"
	"github.com/couchbaselabs/go-blip-sync"
	"github.com/couchbase/go-blip"
)

// Maps the profile (verb) of an incoming request to the method that handles it.
var kHandlersByProfile = map[string]func(*blipsync.BlipSyncHandler, *blip.Message) error{
	"getCheckpoint": (*blipsync.BlipSyncHandler).HandleGetCheckpoint,
	"setCheckpoint": (*blipsync.BlipSyncHandler).HandleSetCheckpoint,
	"subChanges":    (*blipsync.BlipSyncHandler).HandleSubscribeToChanges,
	"changes":       (*blipsync.BlipSyncHandler).HandlePushedChanges,
	"rev":           (*blipsync.BlipSyncHandler).HandleAddRevision,
	"getAttachment": (*blipsync.BlipSyncHandler).HandleGetAttachment,
}

// HTTP handler for incoming BLIP sync WebSocket request (/db/_blipsync)
func (h *handler) handleBLIPSync() error {
	if !h.server.GetDatabaseConfig(h.db.Name).Unsupported.Replicator2 {
		return base.HTTPErrorf(http.StatusNotFound, "feature not enabled")
	}

	ctx := blipsync.BlipSyncContext{
		BlipContext:       blip.NewContext(),
		Dbc:               h.db.DatabaseContext,
		User:              h.user,
		EffectiveUsername: h.currentEffectiveUserName(),
	}
	ctx.BlipContext.DefaultHandler = ctx.NotFound
	for profile, handlerFn := range kHandlersByProfile {
		ctx.Register(profile, handlerFn)
	}

	ctx.BlipContext.Logger = func(fmt string, params ...interface{}) {
		base.LogTo("BLIP", fmt, params...)
	}
	ctx.BlipContext.LogMessages = base.LogEnabledExcludingLogStar("BLIP+")
	ctx.BlipContext.LogFrames = base.LogEnabledExcludingLogStar("BLIP++")

	// Start a WebSocket client and connect it to the BLIP handler:
	wsHandler := func(conn *websocket.Conn) {
		h.logStatus(101, "Upgraded to BLIP+WebSocket protocol")
		defer func() {
			conn.Close()
			base.LogTo("HTTP+", "#%03d:     --> BLIP+WebSocket connection closed", h.serialNumber)
		}()
		ctx.BlipContext.WebSocketHandler()(conn)
	}
	server := websocket.Server{
		Handshake: func(*websocket.Config, *http.Request) error { return nil },
		Handler:   wsHandler,
	}
	server.ServeHTTP(h.response, h.rq)
	return nil
}

