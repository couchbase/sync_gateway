package db

import (
	"context"
	"strings"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

const (
	// blipCBMobileReplication is the AppProtocolId part of the BLIP websocket subprotocol.  Must match identically with the peer (typically CBLite / LiteCore).
	// At some point this will need to be able to support an array of protocols.  See go-blip/issues/27.
	blipCBMobileReplication = "CBMobile_2"
)

// NewSGBlipContext returns a go-blip context with the given ID, initialized for use in Sync Gateway.
func NewSGBlipContext(ctx context.Context, id string) (bc *blip.Context, err error) {
	if id == "" {
		bc, err = blip.NewContext(blipCBMobileReplication)
		if err != nil {
			return nil, err
		}
	} else {
		bc, err = blip.NewContextCustomID(id, blipCBMobileReplication)
		if err != nil {
			return nil, err
		}
	}

	bc.LogMessages = base.LogDebugEnabled(base.KeyWebSocket)
	bc.LogFrames = base.LogDebugEnabled(base.KeyWebSocketFrame)
	bc.Logger = defaultBlipLogger(ctx)

	return bc, nil
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

// blipRevMessageProperties returns a set of BLIP message properties for the given parameters.
func blipRevMessageProperties(revisionHistory []string, deleted bool, seq SequenceID) blip.Properties {
	properties := make(blip.Properties)

	// TODO: Assert? db.SequenceID.MarshalJSON can never error
	seqJSON, _ := base.JSONMarshal(seq)
	properties[RevMessageSequence] = string(seqJSON)

	if len(revisionHistory) > 0 {
		properties[RevMessageHistory] = strings.Join(revisionHistory, ",")
	}

	if deleted {
		properties[RevMessageDeleted] = "1"
	}

	return properties
}

// Returns true if this attachment is worth trying to compress.
func isCompressible(filename string, meta map[string]interface{}) bool {
	if meta["encoding"] != nil {
		return false
	} else if kBadFilenames.MatchString(filename) {
		return false
	} else if mimeType, ok := meta["content_type"].(string); ok && mimeType != "" {
		return !kCompressedTypes.MatchString(mimeType) &&
			(kGoodTypes.MatchString(mimeType) ||
				!kBadTypes.MatchString(mimeType))
	}
	return true // be optimistic by default
}
