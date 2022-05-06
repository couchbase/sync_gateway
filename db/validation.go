package db

import (
	"bytes"
	"net/http"
	"strings"

	"github.com/couchbase/sync_gateway/base"
)

// validateNewBody validates any new body being received (i.e. through blip, import, and API)
func validateNewBody(body Body) error {
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
			base.Warnf("user defined top-level properties that start with '_sync_' should not be in the document body")
		}
	}
	return nil
}

// validateAPIDocUpdate finds disallowed document properties that are allowed in through blip and/or import but not through
// the REST API
func validateAPIDocUpdate(body Body) error {
	// Validation for disallowed properties for blip and import should be done in validateNewBody
	// _rev, _attachments, _id are validated before reaching this function (due to endpoint specific behaviour)
	if _, ok := body[base.SyncPropertyName]; ok {
		base.Warnf("document-top level property '_sync' is an internal property and should not be set")
	}
	return nil
}

// validateImportBody validates incoming import bodies
func validateImportBody(body Body) error {
	if body == nil {
		return base.ErrEmptyDocument
	}

	if isPurged, ok := body[BodyPurged].(bool); ok && isPurged {
		return base.ErrImportCancelledPurged
	}

	// Warn when an internal properties is used
	disallowed := []string{BodyId, BodyRev, BodyExpiry, BodyRevisions}
	for _, prop := range disallowed {
		if _, ok := body[prop]; ok {
			base.Warnf("top-level property '" + prop + "' is a reserved internal property and should not be set")
		}
	}

	return nil
}

// validateBlipBody validates incoming blip rev bodies
// Takes a rawBody to avoid an unnecessary call to doc.BodyBytes()
func validateBlipBody(rawBody []byte, doc *Document) error {
	// Warn when an internal properties is used
	disallowed := []string{base.SyncPropertyName, BodyId, BodyRev, BodyDeleted, BodyRevisions}
	for _, prop := range disallowed {
		// Only unmarshal if raw body contains the disallowed property
		if bytes.Contains(rawBody, []byte(`"`+prop+`"`)) {
			if _, ok := doc.Body()[prop]; ok {
				base.Warnf("top-level property '" + prop + "' is an internal property and should not be set")
			}
		}
	}
	return nil
}

func (db *Database) validateExistingDoc(doc *Document, importAllowed, docExists bool) error {
	if !importAllowed && docExists && !doc.HasValidSyncData() {
		return base.HTTPErrorf(409, "Not imported")
	}
	return nil
}
