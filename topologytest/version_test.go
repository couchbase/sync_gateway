// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package topologytest

import (
	"fmt"

	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
)

// DocMetadata is a struct that contains metadata about a document. It contains the relevant information for testing versions of documents, as well as debugging information.
type DocMetadata struct {
	DocID      string                  // DocID is the document ID
	RevTreeID  string                  // RevTreeID is the rev treee ID of a document, may be empty not present
	HLV        *db.HybridLogicalVector // HLV is the hybrid logical vector of the document, may not be present
	Mou        *db.MetadataOnlyUpdate  // Mou is the metadata only update of the document, may not be present
	Cas        uint64                  // Cas is the cas value of the document
	ImplicitCV *db.Version             // ImplicitCV is the version of the document, if there was no HLV
}

// CV returns the current version of the document.
func (v DocMetadata) CV() db.Version {
	if v.HLV == nil {
		// If there is no HLV, then the version is implicit from the current ver@sourceID
		if v.ImplicitCV == nil {
			return db.Version{}
		}
		return *v.ImplicitCV
	}
	return db.Version{
		SourceID: v.HLV.SourceID,
		Value:    v.HLV.Version,
	}
}

// DocMetadataFromDocument returns a DocVersion from the given document.
func DocMetadataFromDocument(doc *db.Document) DocMetadata {
	return DocMetadata{
		DocID:     doc.ID,
		RevTreeID: doc.CurrentRev,
		Mou:       doc.MetadataOnlyUpdate,
		Cas:       doc.Cas,
		HLV:       doc.HLV,
	}
}

func (v DocMetadata) GoString() string {
	return fmt.Sprintf("DocMetadata{\nDocID:%s\n\tRevTreeID:%s\n\tHLV:%+v\n\tMou:%+v\n\tCas:%d\n\tImplicitCV:%+v\n}", v.DocID, v.RevTreeID, v.HLV, v.Mou, v.Cas, v.ImplicitCV)
}

// DocMetadataFromDocVersion returns metadata DocVersion from the given document and version.
func DocMetadataFromDocVersion(docID string, version rest.DocVersion) DocMetadata {
	return DocMetadata{
		DocID:      docID,
		RevTreeID:  version.RevTreeID,
		ImplicitCV: &version.CV,
	}
}
