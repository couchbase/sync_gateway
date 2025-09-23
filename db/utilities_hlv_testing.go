/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/require"
)

// HLVAgent performs HLV updates directly (not via SG) for simulating/testing interaction with non-SG HLV agents
type HLVAgent struct {
	t         *testing.T
	datastore base.DataStore
	Source    string // All writes by the HLVHelper are done as this source
	xattrName string // xattr name to store the HLV
}

var defaultHelperBody = map[string]interface{}{"version": 1}

func NewHLVAgent(t *testing.T, datastore base.DataStore, source string, xattrName string) *HLVAgent {
	return &HLVAgent{
		t:         t,
		datastore: datastore,
		Source:    EncodeSource(source), // all writes by the HLVHelper are done as this source
		xattrName: xattrName,
	}
}

// InsertWithHLV inserts a new document into the bucket with a populated HLV (matching a write from
// a different, non-SGW HLV-aware peer)
func (h *HLVAgent) InsertWithHLV(ctx context.Context, key string) (casOut uint64) {
	hlv := &HybridLogicalVector{}
	err := hlv.AddVersion(CreateVersion(h.Source, expandMacroCASValueUint64))
	require.NoError(h.t, err)
	hlv.CurrentVersionCAS = expandMacroCASValueUint64

	vvDataBytes := base.MustJSONMarshal(h.t, hlv)
	mutateInOpts := &sgbucket.MutateInOptions{
		MacroExpansion: hlv.computeMacroExpansions(),
	}

	docBody := base.MustJSONMarshal(h.t, defaultHelperBody)
	xattrData := map[string][]byte{
		h.xattrName: vvDataBytes,
	}

	cas, err := h.datastore.WriteWithXattrs(ctx, key, 0, 0, docBody, xattrData, nil, mutateInOpts)
	require.NoError(h.t, err)
	return cas
}

// UpdateWithHLV will update and existing doc in bucket mocking write from another hlv aware peer
func (h *HLVAgent) UpdateWithHLV(ctx context.Context, key string, inputCas uint64, hlv *HybridLogicalVector) (casOut uint64) {
	err := hlv.AddVersion(CreateVersion(h.Source, expandMacroCASValueUint64))
	require.NoError(h.t, err)
	hlv.CurrentVersionCAS = expandMacroCASValueUint64

	vvXattr, err := hlv.MarshalJSON()
	require.NoError(h.t, err)
	mutateInOpts := &sgbucket.MutateInOptions{
		MacroExpansion: hlv.computeMacroExpansions(),
	}

	docBody := base.MustJSONMarshal(h.t, defaultHelperBody)
	xattrData := map[string][]byte{
		h.xattrName: vvXattr,
	}
	cas, err := h.datastore.WriteWithXattrs(ctx, key, 0, inputCas, docBody, xattrData, nil, mutateInOpts)
	require.NoError(h.t, err)
	return cas
}

// CreateDocNoHLV is a test only function to create a document without an HLV, this is useful for testing scenarios
// where documents have not yet been updated post upgrade (pre upgraded doc so no HLV is given to it yet).
func (db *DatabaseCollectionWithUser) CreateDocNoHLV(t testing.TB, ctx context.Context, docid string, body Body) (newRevID string, doc *Document) {
	delete(body, BodyId)

	// Get the revision ID to match, and the new generation number:
	matchRev, _ := body[BodyRev].(string)
	generation, _ := ParseRevID(ctx, matchRev)
	if generation < 0 {
		require.FailNow(t, "Invalid revision ID")
	}
	generation++
	delete(body, BodyRev)

	// remove CV before RevTreeID generation
	matchCV, _ := body[BodyCV].(string)
	delete(body, BodyCV)

	// Not extracting it yet because we need this property around to generate a RevTreeID
	deleted, _ := body[BodyDeleted].(bool)

	expiry, err := body.ExtractExpiry()
	require.NoError(t, err)

	// Create newDoc which will be used to pass around Body
	newDoc := &Document{
		ID: docid,
	}

	// Pull out attachments
	newDoc.SetAttachments(GetBodyAttachments(body))
	delete(body, BodyAttachments)

	delete(body, BodyRevisions)

	err = validateAPIDocUpdate(body)
	require.NoError(t, err)

	docUpdateEvent := NoHLVUpdateForTest
	allowImport := db.UseXattrs()
	// we cannot use rev cache for legacy rev writes, the rev cache is architected to always expect a CV on a Put
	// here we don't thus we end up with inconsistent rev cache state
	updateRevCache := false

	doc, newRevID, err = db.updateAndReturnDoc(ctx, newDoc.ID, allowImport, &expiry, nil, docUpdateEvent, nil, false, updateRevCache, func(doc *Document) (resultDoc *Document, resultAttachmentData updatedAttachments, createNewRevIDSkipped bool, updatedExpiry *uint32, resultErr error) {
		var isSgWrite bool
		var crc32Match bool

		// Is this doc an sgWrite?
		if doc != nil {
			isSgWrite, crc32Match, _ = db.IsSGWrite(ctx, doc, nil)
			if crc32Match {
				db.dbStats().Database().Crc32MatchCount.Add(1)
			}
		}

		// (Be careful: this block can be invoked multiple times if there are races!)
		// If the existing doc isn't an SG write, import prior to updating
		if doc != nil && !isSgWrite && db.UseXattrs() {
			err := db.OnDemandImportForWrite(ctx, newDoc.ID, doc, deleted)
			if err != nil {
				if db.ForceAPIForbiddenErrors() {
					base.InfofCtx(ctx, base.KeyCRUD, "Importing doc %q prior to write caused error", base.UD(newDoc.ID))
					return nil, nil, false, nil, ErrForbidden
				}
				return nil, nil, false, nil, err
			}
		}

		var conflictErr error

		// OCC check of matchCV against CV on the doc
		if matchCV != "" {
			if matchCV == doc.HLV.GetCurrentVersionString() {
				// set matchRev to the current revision ID and allow existing codepaths to perform RevTree-based update.
				matchRev = doc.GetRevTreeID()
				// bump generation based on retrieved RevTree ID
				generation, _ = ParseRevID(ctx, matchRev)
				generation++
			} else if doc.hasFlag(channels.Conflict | channels.Hidden) {
				// Can't use CV as an OCC Value when a document is in conflict, or we're updating the non-winning leaf
				// There's no way to get from a given old CV to a RevTreeID to perform the update correctly, since we don't maintain linear history for a given SourceID.
				// Reject the request and force the user to resolve the conflict using RevTree IDs which does have linear history available.
				conflictErr = base.HTTPErrorf(http.StatusBadRequest, "Cannot use CV to modify a document in conflict - resolve first with RevTree ID")
			} else {
				conflictErr = base.HTTPErrorf(http.StatusConflict, "Document revision conflict")
			}
		}

		if conflictErr == nil {
			// Make sure matchRev matches an existing leaf revision:
			if matchRev == "" {
				matchRev = doc.GetRevTreeID()
				if matchRev != "" {
					// PUT with no parent rev given, but there is an existing current revision.
					// This is OK as long as the current one is deleted.
					if !doc.History[matchRev].Deleted {
						conflictErr = base.HTTPErrorf(http.StatusConflict, "Document exists")
					} else {
						generation, _ = ParseRevID(ctx, matchRev)
						generation++
					}
				}
			} else if !doc.History.isLeaf(matchRev) || db.IsIllegalConflict(ctx, doc, matchRev, deleted, false, nil) {
				conflictErr = base.HTTPErrorf(http.StatusConflict, "Document revision conflict")
			}
		}

		// Make up a new _rev, and add it to the history:
		bodyWithoutInternalProps, wasStripped := StripInternalProperties(body)
		canonicalBytesForRevID, err := base.JSONMarshalCanonical(bodyWithoutInternalProps)
		if err != nil {
			return nil, nil, false, nil, err
		}

		// We needed to keep _deleted around in the body until we generated a rev ID, but now we can ditch it.
		_, isDeleted := body[BodyDeleted]
		if isDeleted {
			delete(body, BodyDeleted)
		}

		// and now we can finally update the newDoc body to be without any special properties
		newDoc.UpdateBody(body)

		// If no special properties were stripped and document wasn't deleted, the canonical bytes represent the current
		// body.  In this scenario, store canonical bytes as newDoc._rawBody
		if !wasStripped && !isDeleted {
			newDoc._rawBody = canonicalBytesForRevID
		}

		// Handle telling the user if there is a conflict
		if conflictErr != nil {
			if db.ForceAPIForbiddenErrors() {
				// Make sure the user has permission to modify the document before confirming doc existence
				mutableBody, metaMap, newRevID, err := db.prepareSyncFn(doc, newDoc)
				if err != nil {
					base.InfofCtx(ctx, base.KeyCRUD, "Failed to prepare to run sync function: %v", err)
					return nil, nil, false, nil, ErrForbidden
				}

				_, _, _, _, _, err = db.runSyncFn(ctx, doc, mutableBody, metaMap, newRevID)
				if err != nil {
					base.DebugfCtx(ctx, base.KeyCRUD, "Could not modify doc %q due to %s and sync func rejection: %v", base.UD(doc.ID), conflictErr, err)
					return nil, nil, false, nil, ErrForbidden
				}
			}
			return nil, nil, false, nil, conflictErr
		}

		// Process the attachments, and populate _sync with metadata. This alters 'body' so it has to
		// be done before calling CreateRevID (the ID is based on the digest of the body.)
		newAttachments, err := db.storeAttachments(ctx, doc, newDoc.Attachments(), generation, matchRev, nil)
		if err != nil {
			return nil, nil, false, nil, err
		}

		newRev := CreateRevIDWithBytes(generation, matchRev, canonicalBytesForRevID)

		if err := doc.History.addRevision(newDoc.ID, RevInfo{ID: newRev, Parent: matchRev, Deleted: deleted}); err != nil {
			base.InfofCtx(ctx, base.KeyCRUD, "Failed to add revision ID: %s, for doc: %s, error: %v", newRev, base.UD(docid), err)
			return nil, nil, false, nil, base.ErrRevTreeAddRevFailure
		}

		newDoc.RevID = newRev
		newDoc.Deleted = deleted

		return newDoc, newAttachments, false, nil, nil
	})
	require.NoError(t, err, "error from updateAndReturnDoc")

	return newRevID, doc
}

// EncodeTestVersion converts a simplified string version of the form 1@abc to a hex-encoded version and base64 encoded
// source, like 169a05acd705ffc0@YWJj.  Allows use of simplified versions in tests for readability, ease of use.
func EncodeTestVersion(versionString string) (encodedString string) {
	timestampString, source, found := strings.Cut(versionString, "@")
	if !found {
		return versionString
	}
	if len(timestampString) > 0 && timestampString[0] == ' ' {
		timestampString = timestampString[1:]
	}
	timestampUint, err := strconv.ParseUint(timestampString, 10, 64)
	if err != nil {
		return ""
	}
	hexTimestamp := strconv.FormatUint(timestampUint, 16)
	base64Source := EncodeSource(source)
	return hexTimestamp + "@" + base64Source
}

// GetHelperBody returns the body contents of a document written by HLVAgent.
func (h *HLVAgent) GetHelperBody() string {
	return string(base.MustJSONMarshal(h.t, defaultHelperBody))
}

// SourceID returns the encoded source ID for the HLVAgent
func (h *HLVAgent) SourceID() string {
	return h.Source
}

// EncodeTestHistory converts a simplified version history of the form "1@abc,2@def;3@ghi" to use hex-encoded versions and
// base64 encoded sources
func EncodeTestHistory(historyString string) (encodedString string) {
	// possible versionSets:
	// 	mv,mv;pv,pv
	// 	mv,mv;
	// 	pv,pv
	versionSets := strings.Split(historyString, ";")
	if len(versionSets) == 0 {
		return ""
	}
	for index, versionSet := range versionSets {
		// versionSet delimiter
		if index > 0 {
			encodedString += ";"
		}
		versions := strings.Split(versionSet, ",")
		for index, version := range versions {
			// version delimiter
			if index > 0 {
				encodedString += ","
			}
			encodedString += EncodeTestVersion(version)
		}
	}
	return encodedString
}

// ParseTestHistory takes a string test history in the form 1@abc,2@def;3@ghi,4@jkl and formats this
// as pv and mv maps keyed by encoded source, with encoded values
func ParseTestHistory(t *testing.T, historyString string) (pv HLVVersions, mv HLVVersions) {
	versionSets := strings.Split(historyString, ";")

	pv = make(HLVVersions)
	mv = make(HLVVersions)

	var pvString, mvString string
	switch len(versionSets) {
	case 1:
		pvString = versionSets[0]
	case 2:
		mvString = versionSets[0]
		pvString = versionSets[1]
	default:
		return pv, mv
	}

	// pv
	for _, versionStr := range strings.Split(pvString, ",") {
		version, err := ParseVersion(versionStr)
		require.NoError(t, err)
		pv[EncodeSource(version.SourceID)] = version.Value
	}

	// mv
	if mvString != "" {
		for _, versionStr := range strings.Split(mvString, ",") {
			version, err := ParseVersion(versionStr)
			require.NoError(t, err)
			mv[EncodeSource(version.SourceID)] = version.Value
		}
	}
	return pv, mv
}

// RequireCVEqual fails tests if provided HLV does not have expected CV (sent in blip wire format)
func RequireCVEqual(t *testing.T, hlv *HybridLogicalVector, expectedCV string) {
	testVersion, err := ParseVersion(expectedCV)
	require.NoError(t, err)
	require.Equal(t, EncodeSource(testVersion.SourceID), hlv.SourceID)
	require.Equal(t, testVersion.Value, hlv.Version)
}

// hlvAsBlipString returns the full HLV as a string in the format seen over Blip
func hlvAsBlipString(_ testing.TB, hlv *HybridLogicalVector) string {
	s := &strings.Builder{}
	s.WriteString(hlv.GetCurrentVersionString())
	history := hlv.toHistoryForHLV(HLVVersions.sorted)
	if history != "" {
		if len(hlv.MergeVersions) > 0 {
			fmt.Fprintf(s, ",")
		} else if len(hlv.PreviousVersions) > 0 {
			fmt.Fprintf(s, ";")
		}
		s.WriteString(history)
	}
	return s.String()
}

// AlterHLVForTest will alter the HLV of an existing document in the bucket, setting it to the provided HLV. Used for
// testing purposes to set up specific HLV scenarios.
func AlterHLVForTest(t *testing.T, ctx context.Context, dataStore base.DataStore, key string, hlv *HybridLogicalVector, docBody map[string]interface{}) uint64 {
	cas, err := dataStore.Get(key, nil)
	require.NoError(t, err)

	hlv.CurrentVersionCAS = math.MaxUint64 // macro expand this

	vvDataBytes := base.MustJSONMarshal(t, hlv)
	xattrData := map[string][]byte{
		base.VvXattrName: vvDataBytes,
	}

	bodyBytes := base.MustJSONMarshal(t, docBody)

	mutateInOpts := &sgbucket.MutateInOptions{
		MacroExpansion: hlv.computeMacroExpansions(),
	}

	cas, err = dataStore.WriteWithXattrs(ctx, key, 0, cas, bodyBytes, xattrData, nil, mutateInOpts)
	require.NoError(t, err)
	return cas
}
