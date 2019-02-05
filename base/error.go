//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/couchbase/gocb"
	"github.com/couchbase/gomemcached"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
)

type sgError struct {
	message string
}

var (
	ErrRevTreeAddRevFailure  = &sgError{"Failure adding Rev to RevTree"}
	ErrImportCancelled       = &sgError{"Import cancelled"}
	ErrAlreadyImported       = &sgError{"Document already imported"}
	ErrImportCasFailure      = &sgError{"CAS failure during import"}
	ErrViewTimeoutError      = &sgError{"Timeout performing Query"}
	ErrImportCancelledFilter = &sgError{"Import cancelled based on import filter"}
	ErrDocumentMigrated      = &sgError{"Document migrated"}
	ErrFatalBucketConnection = &sgError{"Fatal error connecting to bucket"}
	ErrEmptyMetadata         = &sgError{"Empty Sync Gateway metadata"}
	ErrCasFailureShouldRetry = &sgError{"CAS failure should retry"}
	ErrIndexerError          = &sgError{"Indexer error"}
	ErrIndexAlreadyExists    = &sgError{"Index already exists"}
	ErrNotFound              = &sgError{"Not Found"}
	ErrUpdateCancel          = &sgError{"Cancel update"}
	ErrImportIgnored         = &sgError{"Ignore Import"}

	// ErrPartialViewErrors is returned if the view call contains any partial errors.
	// This is more of a warning, and inspecting ViewResult.Errors is required for detail.
	ErrPartialViewErrors = &sgError{"Partial errors in view"}

	// ErrEmptyDocument is returned when trying to insert a document with a null body.
	ErrEmptyDocument = &sgError{"Document body is empty"}
)

func (e *sgError) Error() string {
	if e.message != "" {
		return e.message
	}
	return "Unknown error"
}

// Simple error implementation wrapping an HTTP response status.
type HTTPError struct {
	Status  int
	Message string
}

func (err *HTTPError) Error() string {
	return fmt.Sprintf("%d %s", err.Status, err.Message)
}

func HTTPErrorf(status int, format string, args ...interface{}) *HTTPError {
	return &HTTPError{status, fmt.Sprintf(format, args...)}
}

// Attempts to map an error to an HTTP status code and message.
// Defaults to 500 if it doesn't recognize the error. Returns 200 for a nil error.
func ErrorAsHTTPStatus(err error) (int, string) {
	if err == nil {
		return 200, "OK"
	}

	unwrappedErr := pkgerrors.Cause(err)

	// Check for SGErrors
	switch unwrappedErr {
	case gocb.ErrKeyNotFound:
		return http.StatusNotFound, "missing"
	case gocb.ErrKeyExists:
		return http.StatusConflict, "Conflict"
	case gocb.ErrTimeout:
		return http.StatusServiceUnavailable, "Database timeout error (gocb.ErrTimeout)"
	case gocb.ErrOverload:
		return http.StatusServiceUnavailable, "Database server is over capacity (gocb.ErrOverload)"
	case gocb.ErrBusy:
		return http.StatusServiceUnavailable, "Database server is over capacity (gocb.ErrBusy)"
	case gocb.ErrTmpFail:
		return http.StatusServiceUnavailable, "Database server is over capacity (gocb.ErrTmpFail)"
	case ErrViewTimeoutError:
		return http.StatusServiceUnavailable, unwrappedErr.Error()
	}

	switch unwrappedErr := unwrappedErr.(type) {
	case *HTTPError:
		return unwrappedErr.Status, unwrappedErr.Message
	case *gomemcached.MCResponse:
		switch unwrappedErr.Status {
		case gomemcached.KEY_ENOENT:
			return http.StatusNotFound, "missing"
		case gomemcached.KEY_EEXISTS:
			return http.StatusConflict, "Conflict"
		case gomemcached.E2BIG:
			return http.StatusRequestEntityTooLarge, "Too Large: " + string(unwrappedErr.Body)
		case gomemcached.TMPFAIL:
			return http.StatusServiceUnavailable, "Database server is over capacity (gomemcached.TMPFAIL)"
		default:
			return http.StatusBadGateway, fmt.Sprintf("%s (%s)",
				string(unwrappedErr.Body), unwrappedErr.Status.String())
		}
	case sgbucket.MissingError:
		return http.StatusNotFound, "missing"
	case *sgError:
		switch unwrappedErr {
		case ErrNotFound:
			return http.StatusNotFound, "missing"
		case ErrEmptyDocument:
			return http.StatusBadRequest, "Document body is empty"
		}
	case *json.SyntaxError, *json.UnmarshalTypeError:
		return http.StatusBadRequest, fmt.Sprintf("Invalid JSON: \"%v\"", unwrappedErr)
	}
	return http.StatusInternalServerError, fmt.Sprintf("Internal error: %v", unwrappedErr)
}

// Returns the standard CouchDB error string for an HTTP error status.
// These are important for compatibility, as some REST APIs don't show numeric statuses,
// only these strings.
func CouchHTTPErrorName(status int) string {
	switch status {
	case 400:
		return "bad_request"
	case 401:
		return "unauthorized"
	case 404:
		return "not_found"
	case 403:
		return "forbidden"
	case 406:
		return "not_acceptable"
	case 409:
		return "conflict"
	case 412:
		return "file_exists"
	case 415:
		return "bad_content_type"
	}
	return fmt.Sprintf("%d", status)
}

// Returns true if an error is a doc-not-found error
func IsDocNotFoundError(err error) bool {

	unwrappedErr := pkgerrors.Cause(err)

	if unwrappedErr != nil && unwrappedErr == gocb.ErrKeyNotFound {
		return true
	}

	switch unwrappedErr := unwrappedErr.(type) {
	case *gomemcached.MCResponse:
		return unwrappedErr.Status == gomemcached.KEY_ENOENT || unwrappedErr.Status == gomemcached.NOT_STORED
	case sgbucket.MissingError:
		return true
	case *HTTPError:
		return unwrappedErr.Status == http.StatusNotFound
	default:
		return false
	}
}
