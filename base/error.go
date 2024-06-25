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
	ErrCasFailureShouldRetry = sgbucket.ErrCasFailureShouldRetry
	ErrIndexerError          = &sgError{"Indexer error"}
	ErrAlreadyExists         = &sgError{"Already exists"}
	ErrNotFound              = &sgError{"Not Found"}
	ErrUpdateCancel          = &sgError{"Cancel update"}
	ErrImportCancelledPurged = HTTPErrorf(http.StatusNotFound, "Import Cancelled Due to Purge")
	ErrChannelFeed           = &sgError{"Error while building channel feed"}
	ErrTimeout               = &sgError{"Operation timed out"}
	ErrPathNotFound          = sgbucket.ErrPathNotFound
	ErrPathExists            = sgbucket.ErrPathExists

	// ErrXattrNotFound is returned if all requested xattrs are not present
	ErrXattrNotFound = &sgError{"Xattr Not Found"}

	// ErrXattrPartialFound is returned if only a subset of requested xattrs are found
	ErrXattrPartialFound = &sgError{"Some Requested Xattrs Not Found"}

	// ErrPartialViewErrors is returned if the view call contains any partial errors.
	// This is more of a warning, and inspecting ViewResult.Errors is required for detail.
	ErrPartialViewErrors = &sgError{"Partial errors in view"}

	// ErrEmptyDocument is returned when trying to insert a document with a null body.
	ErrEmptyDocument = &sgError{"Document body is empty"}

	// ErrDeltaSourceIsTombstone is returned to indicate delta sync should do a full body replication due to the
	// delta source being a tombstone (therefore having an empty body)
	ErrDeltaSourceIsTombstone = &sgError{"From rev is a tombstone"}

	// ErrConfigVersionMismatch is returned when the db config document doesn't match the requested version
	ErrConfigVersionMismatch = &sgError{"Config version mismatch"}

	// ErrConfigRegistryRollback is returned when a db config fetch triggered a registry rollback based on version mismatch (config is older)
	ErrConfigRegistryRollback = &sgError{"Config registry rollback"}

	// ErrConfigRegistryReloadRequired is returned when a db config fetch requires a registry reload based on version mismatch (config is newer)
	ErrConfigRegistryReloadRequired = &sgError{"Config registry reload required"}

	// ErrReplicationLimitExceeded is returned when then replication connection threshold is exceeded
	ErrReplicationLimitExceeded = &sgError{"Replication limit exceeded. Try again later."}

	// ErrSkippedSequencesMissing is returned when attempting to remove a sequence range form the skipped sequence list and at least one sequence in that range is not present
	ErrSkippedSequencesMissing = &sgError{"Sequence range has sequences that aren't present in skipped list"}
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

	if errors.Is(err, ErrNotFound) || errors.Is(err, gocb.ErrDocumentNotFound) {
		return http.StatusNotFound, "missing"
	} else if errors.Is(err, gocb.ErrOverload) {
		return http.StatusServiceUnavailable, "Database server is over capacity (gocb.ErrOverload)"
	} else if errors.Is(err, gocb.ErrTemporaryFailure) {
		return http.StatusServiceUnavailable, "Database server is over capacity (gocb.ErrTemporaryFailure)"
	} else if errors.Is(err, gocb.ErrValueTooLarge) {
		return http.StatusRequestEntityTooLarge, "Document too large!"
	} else if errors.Is(err, ErrViewTimeoutError) {
		return http.StatusServiceUnavailable, err.Error()
	} else if errors.Is(err, ErrEmptyDocument) {
		return http.StatusBadRequest, "Document body is empty"
	} else if errors.Is(err, gocb.ErrDocumentExists) || errors.Is(err, ErrAlreadyExists) {
		return http.StatusConflict, "Conflict"
	} else if errors.Is(err, gocb.ErrTimeout) {
		return http.StatusServiceUnavailable, "Database timeout error (gocb.ErrTimeout)"
	} else if errors.Is(err, ErrReplicationLimitExceeded) {
		return http.StatusServiceUnavailable, ErrReplicationLimitExceeded.Error()
	}

	if isKVError(err, memd.StatusTooBig) {
		return http.StatusRequestEntityTooLarge, "Document too large!"
	}

	var httpError *HTTPError
	if errors.As(err, &httpError) {
		return httpError.Status, httpError.Message
	}
	var mcResponseErr *gomemcached.MCResponse
	if errors.As(err, &mcResponseErr) {
		switch mcResponseErr.Status {
		case gomemcached.KEY_ENOENT:
			return http.StatusNotFound, "missing"
		case gomemcached.KEY_EEXISTS:
			return http.StatusConflict, "Conflict"
		case gomemcached.E2BIG:
			return http.StatusRequestEntityTooLarge, "Too Large: " + string(mcResponseErr.Body)
		case gomemcached.TMPFAIL:
			return http.StatusServiceUnavailable, "Database server is over capacity (gomemcached.TMPFAIL)"
		default:
			return http.StatusBadGateway, fmt.Sprintf("%s (%s)",
				string(mcResponseErr.Body), mcResponseErr.Status.String())
		}
	}
	var docTooBigErr sgbucket.DocTooBigErr
	if errors.As(err, &docTooBigErr) {
		return http.StatusRequestEntityTooLarge, "Document too large!"
	}
	var missingError sgbucket.MissingError
	if errors.As(err, &missingError) {
		return http.StatusNotFound, "missing"
	}
	var casMismatchErr sgbucket.CasMismatchErr
	if errors.As(err, &casMismatchErr) {
		return http.StatusConflict, "Conflict"
	}
	var xattrMissingError sgbucket.XattrMissingError
	if errors.As(err, &xattrMissingError) {
		return http.StatusNotFound, "missing"
	}
	var jsonSyntaxError *json.SyntaxError
	if errors.As(err, &jsonSyntaxError) {
		return http.StatusBadRequest, fmt.Sprintf("Invalid JSON: \"%v\"", jsonSyntaxError)
	}
	var jsonUnmarshalTypeError *json.UnmarshalTypeError
	if errors.As(err, &jsonUnmarshalTypeError) {
		return http.StatusBadRequest, fmt.Sprintf("Invalid JSON: \"%v\"", jsonUnmarshalTypeError)
	}
	var jsonIterError *JSONIterError
	if errors.As(err, &jsonIterError) {
		return http.StatusBadRequest, fmt.Sprintf("Invalid JSON: \"%v\"", jsonIterError)
	}
	var retryTimeoutError *RetryTimeoutError
	if errors.As(err, &retryTimeoutError) {
		return http.StatusGatewayTimeout, retryTimeoutError.Error()
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

// IsDocNotFoundError returns true if an error is a doc-not-found error
func IsDocNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNotFound) {
		return true
	}

	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return true
	}

	var missingError sgbucket.MissingError
	if errors.As(err, &missingError) {
		return true
	}
	var mcResponseErr *gomemcached.MCResponse
	if errors.As(err, &mcResponseErr) {
		return mcResponseErr.Status == gomemcached.KEY_ENOENT || mcResponseErr.Status == gomemcached.NOT_STORED
	}

	var httpError *HTTPError
	if errors.As(err, &httpError) {
		return httpError.Status == http.StatusNotFound
	}
	return false
}

func IsXattrNotFoundError(err error) bool {
	if err == nil {
		return false
	} else if errors.Is(err, ErrXattrNotFound) {
		return true
	}
	var xattrMissingError sgbucket.XattrMissingError
	if errors.As(err, &xattrMissingError) {
		return true
	}
	return false
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
