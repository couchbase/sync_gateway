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
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DocMetadata is a struct that contains metadata about a document. It contains the relevant information for testing versions of documents, as well as debugging information.
type DocMetadata struct {
	DocID       string                  // DocID is the document ID
	RevTreeID   string                  // RevTreeID is the rev treee ID of a document, may be empty not present
	HLV         *db.HybridLogicalVector // HLV is the hybrid logical vector of the document, may not be present
	Mou         *db.MetadataOnlyUpdate  // Mou is the metadata only update of the document, may not be present
	Cas         uint64                  // Cas is the cas value of the document
	ImplicitHLV *db.HybridLogicalVector // ImplicitHLV is the version of the document, if there was no HLV
}

// CV returns the current version of the document.
func (v DocMetadata) CV(t assert.TestingT) db.Version {
	if v.ImplicitHLV != nil {
		return *v.ImplicitHLV.ExtractCurrentVersionFromHLV()
	} else if v.HLV != nil {
		return *v.HLV.ExtractCurrentVersionFromHLV()
	}
	assert.FailNowf(t, "no hlv available", "%#v", v)
	return db.Version{}
}

func (v DocMetadata) IsHLVEqual(other DocMetadata) bool {
	if v.ImplicitHLV != nil {
		return other.hlvEqual(v.ImplicitHLV)
	} else if v.HLV != nil {
		return other.hlvEqual(v.HLV)
	}
	return other.ImplicitHLV == nil && other.HLV == nil
}

func (v DocMetadata) hlvEqual(hlv *db.HybridLogicalVector) bool {
	if v.ImplicitHLV != nil {
		return v.ImplicitHLV.Equal(hlv)
	} else if v.HLV != nil {
		return v.HLV.Equal(hlv)
	}
	return hlv == nil
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
	return fmt.Sprintf("DocMetadata{\nDocID:%s\n\tRevTreeID:%s\n\tHLV:%+v\n\tMou:%+v\n\tCas:%d\n\tImplicitHLV:%+v\n}", v.DocID, v.RevTreeID, v.HLV, v.Mou, v.Cas, v.ImplicitHLV)
}

// DocMetadataFromDocVersion returns metadata DocVersion from the given document and version.
func DocMetadataFromDocVersion(t testing.TB, docID string, hlv *db.HybridLogicalVector, version rest.DocVersion) DocMetadata {
	m := DocMetadata{
		DocID:       docID,
		RevTreeID:   version.RevTreeID,
		ImplicitHLV: hlv,
	}
	if hlv != nil {
		m.HLV = hlv
	} else {
		m.HLV = db.NewHybridLogicalVector()
		require.NoError(t, m.HLV.AddVersion(version.CV))
	}
	return m
}

// assertHLVEqual asserts that the HLV of the version is equal to the expected HLV.
func assertHLVEqual(t assert.TestingT, dsName sgbucket.DataStoreName, docID string, p string, version DocMetadata, body []byte, expected DocMetadata, topology Topology) {
	assert.True(t, version.IsHLVEqual(expected), "Actual HLV does not match expected on %s for peer %s.  Expected: %#v, Actual: %#v\nActual Body: %s\nReplications:\n%s", docID, p, expected, version, body, topology.GetDocState(t, dsName, docID))
}

// assertCV asserts that CV of the version is equal to the expected CV.
func assertCVEqual(t assert.TestingT, dsName sgbucket.DataStoreName, docID string, p string, version DocMetadata, body []byte, expected DocMetadata, topology Topology) {
	assert.Equal(t, expected.CV(t), version.CV(t), "Actual HLV's CV does not match expected on %s for peer %s.  Expected: %#v, Actual: %#v\nActual Body: %s\n%s", docID, p, expected, version, body, topology.GetDocState(t, dsName, docID))
}
