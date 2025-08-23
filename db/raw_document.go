// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

// RawDocument respresents a raw document with unmarshalled metadata for _sync, and _vv.
type RawDocument struct {
	// SyncData represents _sync xattr of the document.
	SyncData *SyncData
	// HLV represents _vv xattr of the document.
	HLV *HybridLogicalVector
	// Xattrs is the raw xattrs of the document.
	Xattrs map[string][]byte
	// Body is the raw body of the document.
	Body []byte
}

// HasValidSyncDataForImport checks if the RawDocument is importable.
func (doc *RawDocument) HasValidSyncDataForImport() bool {
	return doc.SyncData.HasValidSyncData() || doc.HLV != nil
}
