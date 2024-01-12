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
	"regexp"
	"strings"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

var (
	// compressedTypes are MIME types that explicitly indicate they're compressed:
	compressedTypes = regexp.MustCompile(`(?i)\bg?zip\b`)

	// goodTypes are MIME types that are compressible:
	goodTypes = regexp.MustCompile(`(?i)(^text)|(xml\b)|(\b(html|json|yaml)\b)`)

	// badTypes are MIME types that are generally incompressible:
	badTypes = regexp.MustCompile(`(?i)^(audio|image|video)/`)
	// An interesting type is SVG (image/svg+xml) which matches _both_! (It's compressible.)
	// See <http://www.iana.org/assignments/media-types/media-types.xhtml>

	// badFilenames are filename extensions of incompressible types:
	badFilenames = regexp.MustCompile(`(?i)\.(zip|t?gz|rar|7z|jpe?g|png|gif|svgz|mp3|m4a|ogg|wav|aiff|mp4|mov|avi|theora)$`)
)

// NewSGBlipContext returns a go-blip context with the given ID, initialized for use in Sync Gateway.
func NewSGBlipContext(ctx context.Context, id string, origin []string) (bc *blip.Context, err error) {
	return NewSGBlipContextWithProtocols(ctx, id, origin, supportedSubprotocols())
}

func NewSGBlipContextWithProtocols(ctx context.Context, id string, origin []string, protocols []string) (bc *blip.Context, err error) {
	opts := blip.ContextOptions{
		Origin:      origin,
		ProtocolIds: protocols,
	}
	if id == "" {
		bc, err = blip.NewContext(opts)
	} else {
		bc, err = blip.NewContextCustomID(id, opts)
	}

	bc.LogMessages = base.LogDebugEnabled(ctx, base.KeyWebSocket)
	bc.LogFrames = base.LogDebugEnabled(ctx, base.KeyWebSocketFrame)
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
func (bsc *BlipSyncContext) blipRevMessageProperties(revisionHistory []string, deleted bool, seq SequenceID) blip.Properties {
	properties := make(blip.Properties)

	// TODO: Assert? db.SequenceID.MarshalJSON can never error
	seqJSON, _ := base.JSONMarshal(seq)
	properties[RevMessageSequence] = string(seqJSON)

	if len(revisionHistory) > 0 {
		if bsc.activeCBMobileSubprotocol <= CBMobileReplicationV3 {
			properties[RevMessageHistory] = strings.Join(revisionHistory, ",")
		} else {
			// v4 and above will already be formatted
			properties[RevMessageHistory] = revisionHistory[0]
		}
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
	} else if badFilenames.MatchString(filename) {
		return false
	} else if mimeType, ok := meta["content_type"].(string); ok && mimeType != "" {
		return !compressedTypes.MatchString(mimeType) &&
			(goodTypes.MatchString(mimeType) ||
				!badTypes.MatchString(mimeType))
	}
	return true // be optimistic by default
}
