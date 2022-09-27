//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbase/gomemcached"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/walrus"
	pkgerrors "github.com/pkg/errors"
	gocbv1 "gopkg.in/couchbase/gocb.v1"
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
	ErrAuthError             = &sgError{"Authentication failure"}
	ErrEmptyMetadata         = &sgError{"Empty Sync Gateway metadata"}
	ErrCasFailureShouldRetry = &sgError{"CAS failure should retry"}
	ErrIndexerError          = &sgError{"Indexer error"}
	ErrAlreadyExists         = &sgError{"Already exists"}
	ErrNotFound              = &sgError{"Not Found"}
	ErrUpdateCancel          = &sgError{"Cancel update"}
	ErrImportCancelledPurged = &sgError{"Import Cancelled Due to Purge"}
	ErrChannelFeed           = &sgError{"Error while building channel feed"}
	ErrXattrNotFound         = &sgError{"Xattr Not Found"}
	ErrTimeout               = &sgError{"Operation timed out"}

	// ErrPartialViewErrors is returned if the view call contains any partial errors.
	// This is more of a warning, and inspecting ViewResult.Errors is required for detail.
	ErrPartialViewErrors = &sgError{"Partial errors in view"}

	// ErrEmptyDocument is returned when trying to insert a document with a null body.
	ErrEmptyDocument = &sgError{"Document body is empty"}

	// ErrDeltaSourceIsTombstone is returned to indicate delta sync should do a full body replication due to the
	// delta source being a tombstone (therefore having an empty body)
	ErrDeltaSourceIsTombstone = &sgError{"From rev is a tombstone"}
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
	case gocbv1.ErrKeyNotFound, ErrNotFound:
		return http.StatusNotFound, "missing"
	case gocbv1.ErrKeyExists, ErrAlreadyExists:
		return http.StatusConflict, "Conflict"
	case gocbv1.ErrTimeout, gocb.ErrTimeout:
		return http.StatusServiceUnavailable, "Database timeout error (gocb.ErrTimeout)"
	case gocbv1.ErrOverload, gocb.ErrOverload:
		return http.StatusServiceUnavailable, "Database server is over capacity (gocb.ErrOverload)"
	case gocbv1.ErrBusy:
		return http.StatusServiceUnavailable, "Database server is over capacity (gocb.ErrBusy)"
	case gocbv1.ErrTmpFail, gocb.ErrTemporaryFailure:
		return http.StatusServiceUnavailable, "Database server is over capacity (gocb.ErrTmpFail)"
	case gocbv1.ErrTooBig:
		return http.StatusRequestEntityTooLarge, "Document too large!"
	case ErrViewTimeoutError:
		return http.StatusServiceUnavailable, unwrappedErr.Error()
	}

	// gocb V2 errors
	if errors.Is(unwrappedErr, gocb.ErrDocumentNotFound) {
		return http.StatusNotFound, "missing"
	}
	if errors.Is(unwrappedErr, gocb.ErrDocumentExists) {
		return http.StatusConflict, "Conflict"
	}
	if errors.Is(unwrappedErr, gocb.ErrTimeout) {
		return http.StatusServiceUnavailable, "Database timeout error (gocb.ErrTimeout)"
	}
	if isKVError(unwrappedErr, memd.StatusTooBig) {
		return http.StatusRequestEntityTooLarge, "Document too large!"
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
	case walrus.DocTooBigErr:
		return http.StatusRequestEntityTooLarge, "Document too large!"
	case sgbucket.MissingError:
		return http.StatusNotFound, "missing"
	case *sgError:
		switch unwrappedErr {
		case ErrNotFound:
			return http.StatusNotFound, "missing"
		case ErrEmptyDocument:
			return http.StatusBadRequest, "Document body is empty"
		}
	case *json.SyntaxError, *json.UnmarshalTypeError, *JSONIterError:
		return http.StatusBadRequest, fmt.Sprintf("Invalid JSON: \"%v\"", unwrappedErr)
	case *RetryTimeoutError:
		return http.StatusGatewayTimeout, unwrappedErr.Error()
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
	if unwrappedErr == nil {
		return false
	}

	if unwrappedErr == ErrNotFound {
		return true
	}

	if unwrappedErr == gocbv1.ErrKeyNotFound {
		return true
	}

	if errors.Is(err, gocb.ErrDocumentNotFound) {
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

// MultiError manages a set of errors.  Callers must use ErrorOrNil when returning MultiError to callers
// in order to properly handle nil checks on the returned MultiError
//  Sample usage:
//    func ErrorSet(error1, error2 err) err {
//      var myErrors *MultiError
//      myErrors.Append(error1)
//      myErrors.Append(error2)
//      return myErrors.ErrorOrNil()
//    }

type MultiError struct {
	Errors []error
}

// Append adds an error to the set.  If err is a *MultiError, the inner errors are added to
// the set individually
func (me *MultiError) Append(err error) *MultiError {
	if me == nil {
		me = &MultiError{
			Errors: make([]error, 0),
		}
	}
	switch typedErr := err.(type) {
	case nil:
		return me
	case *MultiError:
		for _, e := range typedErr.Errors {
			me.Errors = append(me.Errors, e)
		}
	default:
		me.Errors = append(me.Errors, err)
	}
	return me
}

// Error implements the error interface, and formats the errors one per line
func (me *MultiError) Error() string {
	if me == nil {
		return ""
	}
	message := fmt.Sprintf("%d errors:\n", len(me.Errors))
	delimiter := ""
	for _, err := range me.Errors {
		message += delimiter + err.Error()
		delimiter = "\n"
	}
	return message
}

// Len returns length of the inner error slice
func (me *MultiError) Len() int {
	if me == nil {
		return 0
	}
	return len(me.Errors)
}

// ErrorOrNil
func (me *MultiError) ErrorOrNil() error {
	if me == nil || len(me.Errors) == 0 {
		return nil
	}
	return me
}
