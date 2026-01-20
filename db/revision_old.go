// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"context"

	"github.com/couchbase/sync_gateway/base"
)

// nonJSONPrefix is a byte prepended to documents to ensure old revision bodies are hidden from N1QL/Views. This can be used as additional identifier to determine the type of data stored.
type nonJSONPrefix byte

const (
	// nonJSONPrefixKindUnknown is an unused byte value but here for clarity between the zero value
	nonJSONPrefixKindUnknown nonJSONPrefix = iota
	// nonJSONPrefixKindRevBody is used for old revision bodies and denotes that everything following is a standard JSON document. (This matches the byte value used by SG < 4.0)
	nonJSONPrefixKindRevBody
	// nonJSONPrefixKindRevPtr is used for old revision RevTreeID->CV pointers. The bytes following this are the CV string to be used to fetch the actual body. (SG >= 4.0)
	nonJSONPrefixKindRevPtr
	// nonJSONPrefixKindRevWithMeta is used for old revision bodies that also have a set of XATTRs. (SG => 4.0.3+)
	nonJSONPrefixKindRevWithMeta
)

// withNonJSONPrefix returns a new byte slice prefixed with a non-JSON byte. The input slice is not modified.
func withNonJSONPrefix(kind nonJSONPrefix, body []byte) []byte {
	buf := make([]byte, 1, len(body)+1)
	buf[0] = byte(kind)
	buf = append(buf, body...)
	return buf
}

// stripNonJSONPrefix removes the non-JSON prefix byte and returns the kind and the remainder of the byte slice.
func stripNonJSONPrefix(data []byte) (kind nonJSONPrefix, body []byte) {
	if len(data) <= 1 {
		return nonJSONPrefixKindUnknown, nil
	}
	return nonJSONPrefix(data[0]), data[1:]
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
