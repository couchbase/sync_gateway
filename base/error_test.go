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
	"reflect"
	"testing"

	"github.com/couchbase/gocb/v2"
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
	code, text := ErrorAsHTTPStatus(nil)
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, http.StatusText(http.StatusOK), text)

	code, text = ErrorAsHTTPStatus(gocb.ErrDocumentNotFound)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "missing", text)

	code, text = ErrorAsHTTPStatus(gocb.ErrDocumentExists)
	assert.Equal(t, http.StatusConflict, code)
	assert.Equal(t, "Conflict", text)

	code, text = ErrorAsHTTPStatus(gocb.ErrTimeout)
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "Database timeout error (gocb.ErrTimeout)", text)

	code, text = ErrorAsHTTPStatus(gocb.ErrOverload)
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "Database server is over capacity (gocb.ErrOverload)", text)

	code, text = ErrorAsHTTPStatus(gocb.ErrTemporaryFailure)
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "Database server is over capacity (gocb.ErrTemporaryFailure)", text)

	code, text = ErrorAsHTTPStatus(gocb.ErrValueTooLarge)
	assert.Equal(t, http.StatusRequestEntityTooLarge, code)
	assert.Equal(t, "Document too large!", text)

	code, text = ErrorAsHTTPStatus(ErrViewTimeoutError)
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, ErrViewTimeoutError.Error(), text)

	fakeHTTPError := &HTTPError{Status: http.StatusForbidden, Message: http.StatusText(http.StatusForbidden)}
	code, text = ErrorAsHTTPStatus(fakeHTTPError)
	assert.Equal(t, http.StatusForbidden, code)
	assert.Equal(t, http.StatusText(http.StatusForbidden), text)

	fakeMCResponse := &gomemcached.MCResponse{Status: gomemcached.KEY_ENOENT}
	code, text = ErrorAsHTTPStatus(fakeMCResponse)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "missing", text)

	fakeMCResponse = &gomemcached.MCResponse{Status: gomemcached.KEY_EEXISTS}
	code, text = ErrorAsHTTPStatus(fakeMCResponse)
	assert.Equal(t, http.StatusConflict, code)
	assert.Equal(t, "Conflict", text)

	fakeMCResponse = &gomemcached.MCResponse{Status: gomemcached.E2BIG}
	code, text = ErrorAsHTTPStatus(fakeMCResponse)
	assert.Equal(t, http.StatusRequestEntityTooLarge, code)
	assert.Equal(t, "Too Large: "+string(fakeMCResponse.Body), text)

	fakeMCResponse = &gomemcached.MCResponse{Status: gomemcached.TMPFAIL}
	code, text = ErrorAsHTTPStatus(fakeMCResponse)
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "Database server is over capacity (gomemcached.TMPFAIL)", text)

	fakeMCResponse = &gomemcached.MCResponse{Status: gomemcached.ROLLBACK}
	code, text = ErrorAsHTTPStatus(fakeMCResponse)
	assert.Equal(t, http.StatusBadGateway, code)
	assert.Equal(t, fmt.Sprintf("%s (%s)", string(fakeMCResponse.Body), fakeMCResponse.Status.String()), text)

	fakeDocTooBigErr := sgbucket.DocTooBigErr{}
	code, text = ErrorAsHTTPStatus(fakeDocTooBigErr)
	assert.Equal(t, http.StatusRequestEntityTooLarge, code)
	assert.Equal(t, "Document too large!", text)

	fakeMissingError := sgbucket.MissingError{}
	code, text = ErrorAsHTTPStatus(fakeMissingError)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "missing", text)

	code, text = ErrorAsHTTPStatus(ErrNotFound)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "missing", text)

	code, text = ErrorAsHTTPStatus(ErrEmptyDocument)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "Document body is empty", text)

	fakeSyntaxError := &json.SyntaxError{}
	code, text = ErrorAsHTTPStatus(fakeSyntaxError)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, fmt.Sprintf("Invalid JSON: \"%v\"", fakeSyntaxError.Error()), text)

	fakeUnmarshalTypeError := &json.UnmarshalTypeError{Value: "FakeValue", Type: reflect.TypeOf(1)}
	code, text = ErrorAsHTTPStatus(fakeUnmarshalTypeError)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, fmt.Sprintf("Invalid JSON: \"%v\"", fakeUnmarshalTypeError.Error()), text)

	fakeJSONIterError := &JSONIterError{E: fakeSyntaxError}
	code, text = ErrorAsHTTPStatus(fakeJSONIterError)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, fmt.Sprintf("Invalid JSON: \"%v\"", fakeJSONIterError.Error()), text)

	fakeRetryTimeoutError := &RetryTimeoutError{}
	code, text = ErrorAsHTTPStatus(fakeRetryTimeoutError)
	assert.Equal(t, http.StatusGatewayTimeout, code)
	assert.Equal(t, fakeRetryTimeoutError.Error(), text)

	fakeUnsupportedTypeError := &json.UnsupportedTypeError{Type: reflect.TypeOf(3.14)}
	code, text = ErrorAsHTTPStatus(fakeUnsupportedTypeError)
	assert.Equal(t, http.StatusInternalServerError, code)
	assert.Equal(t, fmt.Sprintf("Internal error: %v", fakeUnsupportedTypeError.Error()), text)
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
