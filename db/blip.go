/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"strings"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

const (
	// BlipCBMobileReplicationV2 / BlipCBMobileReplicationV3 is the AppProtocolId part of the BLIP websocket
	// sub protocol.  One must match identically with one provided by the peer (CBLite / ISGR)
	BlipCBMobileReplicationV2 = "CBMobile_2"
	BlipCBMobileReplicationV3 = "CBMobile_3"
)

// NewSGBlipContext returns a go-blip context with the given ID, initialized for use in Sync Gateway.
func NewSGBlipContext(ctx context.Context, id string) (bc *blip.Context, err error) {
	// V3 is first here as it is the preferred communication method
	// In the host case this means SGW can accept both V3 and V2 clients
	// In the client case this means we prefer V3 but can fallback to V2
	return NewSGBlipContextWithProtocols(ctx, id, BlipCBMobileReplicationV3, BlipCBMobileReplicationV2)
}

func NewSGBlipContextWithProtocols(ctx context.Context, id string, protocol ...string) (bc *blip.Context, err error) {
	if id == "" {
		bc, err = blip.NewContext(protocol...)
	} else {
		bc, err = blip.NewContextCustomID(id, protocol...)
	}

	bc.LogMessages = base.LogDebugEnabled(base.KeyWebSocket)
	bc.LogFrames = base.LogDebugEnabled(base.KeyWebSocketFrame)
	bc.Logger = defaultBlipLogger(ctx)

	return bc, err
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
