// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbltestclient

import (
	"errors"
	"fmt"
)

// Error domains the test server reports.
const (
	ErrorDomainTestServer = "TESTSERVER"
	ErrorDomainCBL        = "CBL"
	ErrorDomainPOSIX      = "POSIX"
	ErrorDomainSQLite     = "SQLITE"
	ErrorDomainFleece     = "FLEECE"
)

// APIError is an error the test server reported, either as a failed request or inside a
// replicator status.
type APIError struct {
	// StatusCode is the HTTP status the request failed with, or 0 when the error was carried
	// inside a successful response such as a replicator status.
	StatusCode int    `json:"-"`
	Domain     string `json:"domain"`
	Code       int    `json:"code"`
	Message    string `json:"message,omitempty"`
}

func (e *APIError) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("cbltestclient: HTTP %d: %s error %d: %s", e.StatusCode, e.Domain, e.Code, e.Message)
	}
	return fmt.Sprintf("cbltestclient: %s error %d: %s", e.Domain, e.Code, e.Message)
}

// ErrDocumentNotFound is returned by GetDocument when the document does not exist.  A tombstone is
// indistinguishable from an absent document over this API - the test server reads through
// CBLCollection_GetDocument, which returns nothing for a deleted document - so callers that need
// to tell the two apart have to use SnapshotDocuments and VerifyDocuments instead.
var ErrDocumentNotFound = errors.New("cbltestclient: document not found")

// IsDocumentNotFound reports whether err means the document was absent or deleted, rather than the
// request having failed.
func IsDocumentNotFound(err error) bool {
	return errors.Is(err, ErrDocumentNotFound)
}
