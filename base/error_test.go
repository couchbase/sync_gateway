/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbase/gomemcached"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
)

func TestError(t *testing.T) {
	message := "Sync Gateway is not listening on 4984!"
	sgError := sgError{message: message}
	assert.Equal(t, message, sgError.Error())

	message = "Unknown error"
	sgError.message = ""
	assert.Equal(t, message, sgError.Error())
}

func TestErrorAsHTTPStatus(t *testing.T) {
	testCases := []struct {
		name             string
		err              error
		code             int
		text             string
		wrappedErrorText string
	}{
		{
			name: "nil",
			err:  nil,
			code: http.StatusOK,
			text: http.StatusText(http.StatusOK),
		},
		{
			name: "gocb.ErrDocumentNotFound",
			err:  gocb.ErrDocumentNotFound,
			code: http.StatusNotFound,
			text: "missing",
		},
		{
			name: "gocb.ErrDocumentExists",
			err:  gocb.ErrDocumentExists,
			code: http.StatusConflict,
			text: "Conflict",
		},
		{
			name: "ErrAlreadyExists",
			err:  ErrAlreadyExists,
			code: http.StatusConflict,
			text: "Conflict",
		},
		{
			name: "gocb.ErrTimeout",
			err:  gocb.ErrTimeout,
			code: http.StatusServiceUnavailable,
			text: "Database timeout error (gocb.ErrTimeout)",
		},
		{
			name: "gocb.ErrOverload",
			err:  gocb.ErrOverload,
			code: http.StatusServiceUnavailable,
			text: "Database server is over capacity (gocb.ErrOverload)",
		},
		{
			name: "gocb.ErrTemporaryFailure",
			err:  gocb.ErrTemporaryFailure,
			code: http.StatusServiceUnavailable,
			text: "Database server is over capacity (gocb.ErrTemporaryFailure)",
		},
		{
			name: "gocb.ErrValueTooLarge",
			err:  gocb.ErrValueTooLarge,
			code: http.StatusRequestEntityTooLarge,
			text: "Document too large!",
		},
		{
			name:             "ErrViewTimeoutError",
			err:              ErrViewTimeoutError,
			code:             http.StatusServiceUnavailable,
			text:             ErrViewTimeoutError.Error(),
			wrappedErrorText: fmt.Sprintf("wrapped error: %s", ErrViewTimeoutError.Error()),
		},
		{
			name: "ErrReplicationLimitExceeded",
			err:  ErrReplicationLimitExceeded,
			code: http.StatusServiceUnavailable,
			text: ErrReplicationLimitExceeded.Error(),
		},
		{
			name: "HTTPError",
			err:  &HTTPError{Status: http.StatusForbidden, Message: http.StatusText(http.StatusForbidden)},
			code: http.StatusForbidden,
			text: http.StatusText(http.StatusForbidden),
		},
		{
			name: "gomemcached.MCResponse KEY_ENOENT",
			err:  &gomemcached.MCResponse{Status: gomemcached.KEY_ENOENT},
			code: http.StatusNotFound,
			text: "missing",
		},
		{
			name: "gomemcached.MCResponse KEY_EEXISTS",
			err:  &gomemcached.MCResponse{Status: gomemcached.KEY_EEXISTS},
			code: http.StatusConflict,
			text: "Conflict",
		},
		{
			name: "gomemcached.MCResponse E2BIG",
			err:  &gomemcached.MCResponse{Status: gomemcached.E2BIG, Body: []byte("Document too large!")},
			code: http.StatusRequestEntityTooLarge,
			text: "Too Large: Document too large!",
		},
		{
			name: "gomemcached.MCResponse TMPFAIL",
			err:  &gomemcached.MCResponse{Status: gomemcached.TMPFAIL},
			code: http.StatusServiceUnavailable,
			text: "Database server is over capacity (gomemcached.TMPFAIL)",
		},
		{
			name: "gomemcached.MCResponse ROLLBACK",
			err:  &gomemcached.MCResponse{Status: gomemcached.ROLLBACK, Body: []byte("Rollback error")},
			code: http.StatusBadGateway,
			text: "Rollback error (ROLLBACK)",
		},
		{
			name: "sgbucket.DocTooBigErr",
			err:  sgbucket.DocTooBigErr{},
			code: http.StatusRequestEntityTooLarge,
			text: "Document too large!",
		},
		{
			name: "sgbucket.MissingError",
			err:  sgbucket.MissingError{},
			code: http.StatusNotFound,
			text: "missing",
		},
		{
			name: "sgbucket.CasMismatchErr",
			err:  sgbucket.CasMismatchErr{},
			code: http.StatusConflict,
			text: "Conflict",
		},
		{
			name: "sgbucket.XattrMissingError",
			err:  sgbucket.XattrMissingError{},
			code: http.StatusNotFound,
			text: "missing",
		},
		{
			name: "ErrNotFound",
			err:  ErrNotFound,
			code: http.StatusNotFound,
			text: "missing",
		},
		{
			name: "ErrEmptyDocument",
			err:  ErrEmptyDocument,
			code: http.StatusBadRequest,
			text: "Document body is empty",
		},
		{
			name: "json.SyntaxError",
			err:  &json.SyntaxError{},
			code: http.StatusBadRequest,
			text: "Invalid JSON: \"\"",
		},
		{
			name: "json.UnmarshalTypeError",
			err:  &json.UnmarshalTypeError{Value: "FakeValue", Type: reflect.TypeOf(1)},
			code: http.StatusBadRequest,
			text: "Invalid JSON: \"json: cannot unmarshal FakeValue into Go value of type int\"",
		},
		{
			name: "JSONIterError",
			err:  &JSONIterError{E: &json.SyntaxError{}},
			code: http.StatusBadRequest,
			text: "Invalid JSON: \"\"",
		},
		{
			name: "RetryTimeoutError",
			err:  &RetryTimeoutError{},
			code: http.StatusGatewayTimeout,
			text: (&RetryTimeoutError{}).Error(),
		},
		{
			name:             "json.UnsupportedTypeError",
			err:              &json.UnsupportedTypeError{Type: reflect.TypeOf(3.14)},
			code:             http.StatusInternalServerError,
			text:             "Internal error: json: unsupported type: float64",
			wrappedErrorText: "Internal error: wrapped error: json: unsupported type: float64",
		},
		{
			name: "&gocb.KeyValueError, memd.StatusTooBig",
			err:  &gocb.KeyValueError{StatusCode: memd.StatusTooBig, InnerError: fmt.Errorf("fake inner error")},
			code: http.StatusRequestEntityTooLarge,
			text: "Document too large!",
		},
		{
			name: "gocb.KeyValueError, memd.StatusTooBig",
			err:  gocb.KeyValueError{StatusCode: memd.StatusTooBig, InnerError: fmt.Errorf("fake inner error")},
			code: http.StatusRequestEntityTooLarge,
			text: "Document too large!",
		},
		{
			name: "&gocbcore.KeyValueError, memd.StatusTooBig",
			err:  &gocbcore.KeyValueError{StatusCode: memd.StatusTooBig, InnerError: fmt.Errorf("fake inner error")},
			code: http.StatusRequestEntityTooLarge,
			text: "Document too large!",
		},
		{
			name: "gocbcore.KeyValueError, memd.StatusTooBig",
			err:  gocbcore.KeyValueError{StatusCode: memd.StatusTooBig, InnerError: fmt.Errorf("fake inner error")},
			code: http.StatusRequestEntityTooLarge,
			text: "Document too large!",
		},
		{
			name: "&gocbcore.SubdocumentError{gocbcore.KeyValueError, memd.StatusTooBig}",
			err:  &gocbcore.SubDocumentError{InnerError: gocbcore.KeyValueError{StatusCode: memd.StatusTooBig, InnerError: fmt.Errorf("fake inner error")}},
			code: http.StatusRequestEntityTooLarge,
			text: "Document too large!",
		},
		{
			name: "gocbcore.SubdocumentError{gocbcore.KeyValueError, memd.StatusTooBig}",
			err:  gocbcore.SubDocumentError{InnerError: gocbcore.KeyValueError{StatusCode: memd.StatusTooBig, InnerError: fmt.Errorf("fake inner error")}},
			code: http.StatusRequestEntityTooLarge,
			text: "Document too large!",
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			code, text := ErrorAsHTTPStatus(test.err)
			assert.Equal(t, test.code, code)
			assert.Equal(t, test.text, text)

			if test.err == nil {
				return
			}
			wrappedErr := fmt.Errorf("wrapped error: %w", test.err)
			code, text = ErrorAsHTTPStatus(wrappedErr)
			assert.Equal(t, test.code, code)
			if test.wrappedErrorText != "" {
				assert.Equal(t, test.wrappedErrorText, text)
			} else {
				assert.Equal(t, test.text, text)
			}
		})
	}
}

func TestCouchHTTPErrorName(t *testing.T) {
	assert.Equal(t, "bad_request", CouchHTTPErrorName(http.StatusBadRequest))
	assert.Equal(t, "unauthorized", CouchHTTPErrorName(http.StatusUnauthorized))
	assert.Equal(t, "not_found", CouchHTTPErrorName(http.StatusNotFound))
	assert.Equal(t, "forbidden", CouchHTTPErrorName(http.StatusForbidden))
	assert.Equal(t, "not_acceptable", CouchHTTPErrorName(http.StatusNotAcceptable))
	assert.Equal(t, "conflict", CouchHTTPErrorName(http.StatusConflict))
	assert.Equal(t, "file_exists", CouchHTTPErrorName(http.StatusPreconditionFailed))
	assert.Equal(t, "bad_content_type", CouchHTTPErrorName(http.StatusUnsupportedMediaType))
	assert.Equal(t, "500", CouchHTTPErrorName(http.StatusInternalServerError))
}

func TestIsDocNotFoundError(t *testing.T) {
	testCases := []struct {
		name          string
		err           error
		isDocNotFound bool
	}{
		{
			name:          "gomemcached.MCResponse KEY_ENOENT",
			err:           &gomemcached.MCResponse{Status: gomemcached.KEY_ENOENT},
			isDocNotFound: true,
		},
		{
			name:          "gomemcached.MCResponse NOT_STORED",
			err:           &gomemcached.MCResponse{Status: gomemcached.NOT_STORED},
			isDocNotFound: true,
		},
		{
			name:          "gomemcached.MCResponse ROLLBACK",
			err:           &gomemcached.MCResponse{Status: gomemcached.ROLLBACK},
			isDocNotFound: false,
		},
		{
			name:          "sgbucket.MissingError",
			err:           sgbucket.MissingError{},
			isDocNotFound: true,
		},
		{
			name:          "HTTPError StatusNotFound",
			err:           &HTTPError{Status: http.StatusNotFound},
			isDocNotFound: true,
		},
		{
			name:          "HTTPError StatusForbidden",
			err:           &HTTPError{Status: http.StatusForbidden},
			isDocNotFound: false,
		},
		{
			name:          "json.SyntaxError",
			err:           &json.SyntaxError{},
			isDocNotFound: false,
		},
		{
			name:          "nil",
			err:           nil,
			isDocNotFound: false,
		},
		{
			name:          "other error",
			err:           fmt.Errorf("some error"),
			isDocNotFound: false,
		},
		{
			name:          "sgbucket.MissingError with values",
			err:           sgbucket.MissingError{Key: "key"},
			isDocNotFound: true,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			if test.isDocNotFound {
				assert.True(t, IsDocNotFoundError(test.err))
			} else {
				assert.False(t, IsDocNotFoundError(test.err))
			}
			wrappedErr := fmt.Errorf("wrapped error: %w", test.err)
			if test.isDocNotFound {
				assert.True(t, IsDocNotFoundError(wrappedErr))
			} else {
				assert.False(t, IsDocNotFoundError(wrappedErr))
			}
		})
	}
}

func TestIsCasMismatch(t *testing.T) {
	testCases := []struct {
		name          string
		err           error
		isCasMismatch bool
	}{
		{
			name:          "nil",
			err:           nil,
			isCasMismatch: false,
		},
		{
			name:          "&gomemcached.MCResponse KEY_ENOENT",
			err:           &gomemcached.MCResponse{Status: gomemcached.KEY_ENOENT},
			isCasMismatch: false,
		},
		{
			name:          "go-couchbase error",
			err:           fmt.Errorf("CAS mismatch error"),
			isCasMismatch: true,
		},
		{
			name:          "sgbucket.CasMismatchErr",
			err:           sgbucket.CasMismatchErr{},
			isCasMismatch: true,
		},
		{
			name:          "&gocb.KeyValueError, memd.StatusTooBig",
			err:           &gocb.KeyValueError{StatusCode: memd.StatusTooBig, InnerError: fmt.Errorf("fake error")},
			isCasMismatch: false,
		},
		{
			name:          "gocb.KeyValueError, memd.StatusTooBig",
			err:           gocb.KeyValueError{StatusCode: memd.StatusTooBig, InnerError: fmt.Errorf("fake error")},
			isCasMismatch: false,
		},
		{
			name:          "gocb.KeyValueError, memd.StatusKeyExists",
			err:           gocb.KeyValueError{StatusCode: memd.StatusKeyExists, InnerError: fmt.Errorf("fake inner error")},
			isCasMismatch: true,
		},
		{
			name:          "&gocb.KeyValueError, memd.StatusKeyExists",
			err:           &gocb.KeyValueError{StatusCode: memd.StatusKeyExists, InnerError: fmt.Errorf("fake inner error")},
			isCasMismatch: true,
		},
		{
			name:          "gocbcore.KeyValueError, memd.StatusNotStored",
			err:           gocbcore.KeyValueError{StatusCode: memd.StatusNotStored, InnerError: fmt.Errorf("fake inner error")},
			isCasMismatch: true,
		},
		{
			name:          "&gocbcore.KeyValueError, memd.StatusNotStored",
			err:           &gocbcore.KeyValueError{StatusCode: memd.StatusNotStored, InnerError: fmt.Errorf("fake inner error")},
			isCasMismatch: true,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			if test.isCasMismatch {
				assert.True(t, IsCasMismatch(test.err))
			} else {
				assert.False(t, IsCasMismatch(test.err))
			}
			wrappedErr := fmt.Errorf("wrapped error: %w", test.err)
			if test.isCasMismatch {
				assert.True(t, IsCasMismatch(wrappedErr))
			} else {
				assert.False(t, IsCasMismatch(wrappedErr))
			}
		})
	}
}

func TestIsXattrNotFoundError(t *testing.T) {
	testCases := []struct {
		name            string
		err             error
		isXattrNotFound bool
	}{
		{
			name:            "nil",
			err:             nil,
			isXattrNotFound: false,
		},
		{
			name:            "ErrXattrNotFound",
			err:             ErrXattrNotFound,
			isXattrNotFound: true,
		},
		{
			name:            "sgbucket.XattrMissingError",
			err:             sgbucket.XattrMissingError{},
			isXattrNotFound: true,
		},
		{
			name:            "custom error",
			err:             fmt.Errorf("custom error"),
			isXattrNotFound: false,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			if test.isXattrNotFound {
				assert.True(t, IsXattrNotFoundError(test.err), "Expected %+v to be IsXattrNotFoundError", test.err)
			} else {
				assert.False(t, IsXattrNotFoundError(test.err), "Expected %+v to not be IsXattrNotFoundError", test.err)
			}

			wrappedErr := fmt.Errorf("wrapped error: %w", test.err)
			if test.isXattrNotFound {
				assert.True(t, IsXattrNotFoundError(wrappedErr), "Expected %+v to be IsXattrNotFoundError", wrappedErr)
			} else {
				assert.False(t, IsXattrNotFoundError(wrappedErr), "Expected %+v to not be IsXattrNotFoundError", wrappedErr)
			}
		})
	}
}

func TestMultiError(t *testing.T) {
	var m *MultiError
	m = m.Append(fmt.Errorf("first error"))
	m = m.Append(fmt.Errorf("second error"))
	assert.Equal(t, 2, m.Len())
	assert.NotNil(t, m.ErrorOrNil())

	var moreErrors *MultiError
	moreErrors = moreErrors.Append(m)
	assert.Equal(t, 2, moreErrors.Len())
	assert.NotNil(t, moreErrors.ErrorOrNil())

	var moreNonEmptyErrors *MultiError
	moreNonEmptyErrors = moreNonEmptyErrors.Append(fmt.Errorf("another error"))
	moreNonEmptyErrors = moreNonEmptyErrors.Append(moreErrors)
	assert.Equal(t, 3, moreNonEmptyErrors.Len())
	assert.NotNil(t, moreNonEmptyErrors.ErrorOrNil())
	log.Printf("%s", moreNonEmptyErrors)

	var nilError *MultiError
	assert.Nil(t, nilError.ErrorOrNil())

}

func TestIsGocbQueryTimeoutError(t *testing.T) {
	testCases := []struct {
		name                    string
		err                     error
		isGoCBQueryTimeoutError bool
	}{
		{
			name:                    "nil",
			err:                     nil,
			isGoCBQueryTimeoutError: false,
		},
		{
			name:                    "ErrXattrNotFound",
			err:                     ErrXattrNotFound,
			isGoCBQueryTimeoutError: false,
		},
		{
			name:                    "url.Error, connection refused",
			err:                     &url.Error{Op: "Get", URL: "http://localhost:8091", Err: fmt.Errorf("connection refused")},
			isGoCBQueryTimeoutError: false,
		},
		{
			name:                    "url.Error, connection refused",
			err:                     &url.Error{Op: "Get", URL: "http://localhost:8091", Err: fmt.Errorf("request canceled")},
			isGoCBQueryTimeoutError: true,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			if test.isGoCBQueryTimeoutError {
				assert.True(t, isGoCBQueryTimeoutError(test.err), "Expected %+v to be isGoCBQueryTimeoutError", test.err)
			} else {
				assert.False(t, isGoCBQueryTimeoutError(test.err), "Expected %+v to not be isGoCBQueryTimeoutError", test.err)
			}

			wrappedErr := fmt.Errorf("wrapped error: %w", test.err)
			if test.isGoCBQueryTimeoutError {
				assert.True(t, isGoCBQueryTimeoutError(wrappedErr), "Expected %+v to be isGoCBQueryTimeoutError", wrappedErr)
			} else {
				assert.False(t, isGoCBQueryTimeoutError(wrappedErr), "Expected %+v to not be isGoCBQueryTimeoutError", wrappedErr)
			}
		})
	}

}
