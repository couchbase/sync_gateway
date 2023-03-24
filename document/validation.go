// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package document

import (
	"net/http"
	"strings"

	"github.com/couchbase/sync_gateway/base"
)

// ValidateNewBody validates any new body being received (i.e. through blip, import, and API)
func ValidateNewBody(body Body) error {
	// Reject a body that contains the "_removed" property, this means that the user
	// is trying to update a document they do not have read access to.
	if body[BodyRemoved] != nil {
		return base.HTTPErrorf(http.StatusNotFound, "Document revision is not accessible")
	}

	// Reject bodies that contains the "_purged" property.
	if _, ok := body[BodyPurged]; ok {
		return base.HTTPErrorf(http.StatusBadRequest, "user defined top-level property '_purged' is not allowed in document body")
	}

	for key := range body {
		if strings.HasPrefix(key, BodyInternalPrefix) {
			return base.HTTPErrorf(http.StatusBadRequest, "user defined top-level properties that start with '_sync_' are not allowed in document body")
		}
	}
	return nil
}

// ValidateAPIDocUpdate finds disallowed document properties that are allowed in through blip and/or import but not through
// the REST API
func ValidateAPIDocUpdate(body Body) error {
	// VaLidation for disallowed properties for blip and import should be done in validateNewBody
	// _rev, _attachments, _id are validated before reaching this function (due to endpoint specific behaviour)
	if _, ok := body[base.SyncPropertyName]; ok {
		return base.HTTPErrorf(http.StatusBadRequest, "document-top level property '_sync' is a reserved internal property")
	}
	return nil
}

// ValidateImportBody validates incoming import bodies
func ValidateImportBody(body Body) error {
	if body == nil {
		return base.ErrEmptyDocument
	}

	if isPurged, ok := body[BodyPurged].(bool); ok && isPurged {
		return base.ErrImportCancelledPurged
	}

	// Prevent disallowed internal properties from being used
	disallowed := []string{BodyId, BodyRev, BodyExpiry, BodyRevisions}
	for _, prop := range disallowed {
		if _, ok := body[prop]; ok {
			return base.HTTPErrorf(http.StatusBadRequest, "top-level property '"+prop+"' is a reserved internal property therefore cannot be imported")
		}
	}
	// TODO: Validate attachment data to ensure user is not setting invalid attachments

	return nil
}

func ValidateExistingDoc(doc *Document, importAllowed, docExists bool) error {
	if !importAllowed && docExists && !doc.HasValidSyncData() {
		return base.HTTPErrorf(409, "Not imported")
	}
	return nil
}