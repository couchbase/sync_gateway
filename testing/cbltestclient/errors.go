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

// ErrDocumentNotFound is returned by GetDocument when the document does not exist.  The test
// server reports a tombstone exactly as it reports an absent document, so a deleted document
// returns this too.
var ErrDocumentNotFound = errors.New("cbltestclient: document not found")

// IsDocumentNotFound reports whether err means the document was absent or deleted, rather than the
// request having failed.
func IsDocumentNotFound(err error) bool {
	return errors.Is(err, ErrDocumentNotFound)
}
