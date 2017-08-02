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
)

type sgErrorCode uint16

const (
	alreadyImported  = sgErrorCode(0x00)
	importCancelled  = sgErrorCode(0x01)
	importCasFailure = sgErrorCode(0x02)

	revTreeAddRevFailure  = sgErrorCode(0x04)
	importCancelledFilter = sgErrorCode(0x05)
)

type SGError struct {
	code sgErrorCode
}

var (
	ErrRevTreeAddRevFailure  = &SGError{revTreeAddRevFailure}
	ErrImportCancelled       = &SGError{importCancelled}
	ErrAlreadyImported       = &SGError{alreadyImported}
	ErrImportCasFailure      = &SGError{importCasFailure}
	ErrImportCancelledFilter = &SGError{importCancelledFilter}
)

func (e SGError) Error() string {
	switch e.code {
	case alreadyImported:
		return "Document already imported"
	case importCancelled:
		return "Import cancelled"
	case importCancelledFilter:
		return "Import cancelled based on import filter"
	case importCasFailure:
		return "CAS failure during import"
	case revTreeAddRevFailure:
		return "Failure adding Rev to RevTree"
	default:
		return "Unknown error"
	}
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

	switch err {
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
	}

	switch err := err.(type) {
	case *HTTPError:
		return err.Status, err.Message
	case *gomemcached.MCResponse:
		switch err.Status {
		case gomemcached.KEY_ENOENT:
			return http.StatusNotFound, "missing"
		case gomemcached.KEY_EEXISTS:
			return http.StatusConflict, "Conflict"
		case gomemcached.E2BIG:
			return http.StatusRequestEntityTooLarge, "Too Large: " + string(err.Body)
		case gomemcached.TMPFAIL:
			return http.StatusServiceUnavailable, "Database server is over capacity (gomemcached.TMPFAIL)"
		default:
			return http.StatusBadGateway, fmt.Sprintf("%s (%s)",
				string(err.Body), err.Status.String())
		}
	case sgbucket.MissingError:
		return http.StatusNotFound, "missing"
	case *json.SyntaxError, *json.UnmarshalTypeError:
		return http.StatusBadRequest, fmt.Sprintf("Invalid JSON: \"%v\"", err)
	}
	return http.StatusInternalServerError, fmt.Sprintf("Internal error: %v", err)
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

	if err != nil && err == gocb.ErrKeyNotFound {
		return true
	}

	switch err := err.(type) {
	case *gomemcached.MCResponse:
		return err.Status == gomemcached.KEY_ENOENT || err.Status == gomemcached.NOT_STORED
	case sgbucket.MissingError:
		return true
	case *HTTPError:
		return err.Status == http.StatusNotFound
	default:
		return false
	}
}
