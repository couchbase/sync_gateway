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

	"github.com/couchbase/gomemcached"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/walrus"
	"github.com/stretchr/testify/assert"
	"gopkg.in/couchbase/gocb.v1"
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

	code, text = ErrorAsHTTPStatus(gocb.ErrKeyNotFound)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "missing", text)

	code, text = ErrorAsHTTPStatus(gocb.ErrKeyExists)
	assert.Equal(t, http.StatusConflict, code)
	assert.Equal(t, "Conflict", text)

	code, text = ErrorAsHTTPStatus(gocb.ErrTimeout)
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "Database timeout error (gocb.ErrTimeout)", text)

	code, text = ErrorAsHTTPStatus(gocb.ErrOverload)
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "Database server is over capacity (gocb.ErrOverload)", text)

	code, text = ErrorAsHTTPStatus(gocb.ErrBusy)
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "Database server is over capacity (gocb.ErrBusy)", text)

	code, text = ErrorAsHTTPStatus(gocb.ErrTmpFail)
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "Database server is over capacity (gocb.ErrTmpFail)", text)

	code, text = ErrorAsHTTPStatus(gocb.ErrTooBig)
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

	fakeDocTooBigErr := walrus.DocTooBigErr{}
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
	fakeErrKeyNotFound := gocb.ErrKeyNotFound
	assert.True(t, IsDocNotFoundError(fakeErrKeyNotFound))

	fakeMCResponse := &gomemcached.MCResponse{Status: gomemcached.KEY_ENOENT}
	assert.True(t, IsDocNotFoundError(fakeMCResponse))

	fakeMCResponse = &gomemcached.MCResponse{Status: gomemcached.NOT_STORED}
	assert.True(t, IsDocNotFoundError(fakeMCResponse))

	fakeMCResponse = &gomemcached.MCResponse{Status: gomemcached.ROLLBACK}
	assert.False(t, IsDocNotFoundError(fakeMCResponse))

	fakeMissingError := sgbucket.MissingError{}
	assert.True(t, IsDocNotFoundError(fakeMissingError))

	fakeHTTPError := &HTTPError{Status: http.StatusNotFound}
	assert.True(t, IsDocNotFoundError(fakeHTTPError))

	fakeHTTPError = &HTTPError{Status: http.StatusForbidden}
	assert.False(t, IsDocNotFoundError(fakeHTTPError))

	fakeSyntaxError := &json.SyntaxError{}
	assert.False(t, IsDocNotFoundError(fakeSyntaxError))
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
