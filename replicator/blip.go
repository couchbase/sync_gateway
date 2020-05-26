package replicator

import (
	"context"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

const (
	// blipCBMobileReplication is the AppProtocolId part of the BLIP websocket subprotocol.  Must match identically with the peer (typically CBLite / LiteCore).
	// At some point this will need to be able to support an array of protocols.  See go-blip/issues/27.
	blipCBMobileReplication = "CBMobile_2"
)

// NewSGBlipContext returns a go-blip context with the given ID, initialized for use in Sync Gateway.
func NewSGBlipContext(ctx context.Context, id string) (bc *blip.Context) {
	if id == "" {
		bc = blip.NewContext(blipCBMobileReplication)
	} else {
		bc = blip.NewContextCustomID(id, blipCBMobileReplication)
	}

	bc.LogMessages = base.LogDebugEnabled(base.KeyWebSocket)
	bc.LogFrames = base.LogDebugEnabled(base.KeyWebSocketFrame)
	bc.Logger = defaultBlipLogger(ctx)

	return bc
}

// defaultBlipLogger returns a function that can be set as the blip.Context.Logger for Sync Gateway integrated go-blip logging.
func defaultBlipLogger(ctx context.Context) blip.LogFn {
	return func(eventType blip.LogEventType, format string, params ...interface{}) {
		switch eventType {
		case blip.LogFrame:
			base.DebugfCtx(ctx, base.KeyWebSocketFrame, format, params...)
		case blip.LogMessage:
			base.DebugfCtx(ctx, base.KeyWebSocket, format, params...)
		default:
			base.InfofCtx(ctx, base.KeyWebSocket, format, params...)
		}
	}
}
