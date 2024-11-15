package topologytest

import (
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

func (v DocMetadata) CV() string {
	if v.HLV == nil {
		if v.ImplicitCV == nil {
			return ""
		}
		return v.ImplicitCV.String()
	}
	return v.HLV.GetCurrentVersionString()
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

// DocMetadataFromDocVersion returns metadata DocVersion from the given document and version.
func DocMetadataFromDocVersion(docID string, version rest.DocVersion) DocMetadata {
	return DocMetadata{
		DocID:      docID,
		RevTreeID:  version.RevTreeID,
		ImplicitCV: &version.CV,
	}
}
