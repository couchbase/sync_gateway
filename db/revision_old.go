// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"bytes"
	"context"

	"github.com/couchbase/sync_gateway/base"
)

// nonJSONPrefix is a byte prepended to documents to ensure old revision bodies are hidden from N1QL/Views. This can be used as additional identifier to determine the type of data stored.
type nonJSONPrefix byte

const (
	// nonJSONPrefixKindUnknown is an unused byte value but here for clarity between the zero value
	nonJSONPrefixKindUnknown nonJSONPrefix = iota
	// nonJSONPrefixKindRevBody is used for old revision bodies and denotes that everything following is a standard JSON document.
	nonJSONPrefixKindRevBody
	// nonJSONPrefixKindRevPtr is used for old revision RevTreeID->CV pointers. The bytes following this are the CV string to be used to fetch the actual body.
	nonJSONPrefixKindRevPtr
)

// metadataInformationSeperator is a byte used to separate document body and metadata in backup revisions.
type metadataInformationSeperator byte

const (
	// channelInformationSeperatorByte used to separate channel information from the body in backup revisions
	channelInformationSeperatorByte metadataInformationSeperator = iota
)

// withNonJSONPrefix returns a new byte slice prefixed with a non-JSON byte. The input slice is not modified.
func withNonJSONPrefix(kind nonJSONPrefix, body []byte) []byte {
	buf := make([]byte, 1, len(body)+1)
	buf[0] = byte(kind)
	buf = append(buf, body...)
	return buf
}

func generateMainBackupBodyWithNonJSONPrefix(body []byte, channelInfo []byte) []byte {
	buf := make([]byte, 1, len(body)+len(channelInfo)+2) // +2 for prefix and separator
	buf[0] = byte(nonJSONPrefixKindRevBody)
	buf = append(buf, body...)
	buf = append(buf, byte(channelInformationSeperatorByte))
	buf = append(buf, channelInfo...)
	return buf
}

// stripNonJSONPrefix removes the non-JSON prefix byte and returns the kind and the remainder of the byte slice.
func stripNonJSONPrefix(data []byte) (kind nonJSONPrefix, body []byte, channels []byte) {
	if len(data) <= 1 {
		return nonJSONPrefixKindUnknown, nil, channels
	}
	kind = nonJSONPrefix(data[0])
	body, channels, _ = bytes.Cut(data[1:], []byte{byte(channelInformationSeperatorByte)})
	return kind, body, channels
}

// setOldRevisionJSONPtr stores a pointer from the old revision's RevTreeID to the Current Version hash used to fetch the body.
func (db *DatabaseCollectionWithUser) setOldRevisionJSONPtr(ctx context.Context, doc *Document, expiry uint32) error {
	ptrKey := oldRevisionKey(doc.ID, doc.GetRevTreeID())
	revHash := base.Crc32cHashString([]byte(doc.HLV.GetCurrentVersionString()))
	ptrBytes := withNonJSONPrefix(nonJSONPrefixKindRevPtr, []byte(revHash))
	err := db.dataStore.SetRaw(ptrKey, expiry, nil, ptrBytes)
	if err == nil {
		base.DebugfCtx(ctx, base.KeyCRUD, "Backed up revision body legacy RevTree ID pointer %q -> %q/%q (%d bytes, ttl:%d)", doc.GetRevTreeID(), base.UD(doc.ID), revHash, len(ptrBytes), db.deltaSyncRevMaxAgeSeconds())
	} else {
		base.WarnfCtx(ctx, "Failed to write legacy rev pointer for %q rev %q: %v", base.UD(doc.ID), doc.GetRevTreeID(), err)
	}
	return err
}
