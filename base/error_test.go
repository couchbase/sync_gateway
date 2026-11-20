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
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"testing"

	"github.com/couchbase/gocb/v2"
	gocbcore "github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbase/gomemcached"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbaselabs/rosmar"
	pkgerrors "github.com/pkg/errors"
)

func TestError(t *testing.T) {
	message := "Sync Gateway is not listening on 4984!"
	sgError := sgError{message: message}
	assert.Equal(t, message, sgError.Error())

	message = "Unknown error"
	sgError.message = ""
	assert.Equal(t, message, sgError.Error())
}

type errorAsHTTPStatusTestCase struct {
	name         string
	err          error
	expectedCode int
	expectedText string
}

func errorAsHTTPStatusTestCases() []errorAsHTTPStatusTestCase {
	return []errorAsHTTPStatusTestCase{
		{
			name:         "nil",
			err:          nil,
			expectedCode: http.StatusOK,
			expectedText: http.StatusText(http.StatusOK),
		},
		{
			name:         "gocb.ErrDocumentNotFound",
			err:          gocb.ErrDocumentNotFound,
			expectedCode: http.StatusNotFound,
			expectedText: "missing",
		},
		{
			name:         "gocb.ErrDocumentExists",
			err:          gocb.ErrDocumentExists,
			expectedCode: http.StatusConflict,
			expectedText: "Conflict",
		},
		{
			name:         "gocb.ErrTimeout",
			err:          gocb.ErrTimeout,
			expectedCode: http.StatusServiceUnavailable,
			expectedText: "Database timeout error (gocb.ErrTimeout)",
		},
		{
			name:         "gocb.ErrOverload",
			err:          gocb.ErrOverload,
			expectedCode: http.StatusServiceUnavailable,
			expectedText: "Database server is over capacity (gocb.ErrOverload)",
		},
		{
			name:         "gocb.ErrTemporaryFailure",
			err:          gocb.ErrTemporaryFailure,
			expectedCode: http.StatusServiceUnavailable,
			expectedText: "Database server is over capacity (gocb.ErrTemporaryFailure)",
		},
		{
			name:         "gocb.ErrValueTooLarge",
			err:          gocb.ErrValueTooLarge,
			expectedCode: http.StatusRequestEntityTooLarge,
			expectedText: "Document too large!",
		},
		{
			name:         "ErrViewTimeoutError",
			err:          ErrViewTimeoutError,
			expectedCode: http.StatusServiceUnavailable,
			expectedText: ErrViewTimeoutError.Error(),
		},
		{
			name:         "HTTPError",
			err:          &HTTPError{Status: http.StatusForbidden, Message: http.StatusText(http.StatusForbidden)},
			expectedCode: http.StatusForbidden,
			expectedText: http.StatusText(http.StatusForbidden),
		},
		{
			name:         "MCResponse KEY_ENOENT",
			err:          &gomemcached.MCResponse{Status: gomemcached.KEY_ENOENT},
			expectedCode: http.StatusNotFound,
			expectedText: "missing",
		},
		{
			name:         "MCResponse KEY_EEXISTS",
			err:          &gomemcached.MCResponse{Status: gomemcached.KEY_EEXISTS},
			expectedCode: http.StatusConflict,
			expectedText: "Conflict",
		},
		{
			name:         "MCResponse E2BIG",
			err:          &gomemcached.MCResponse{Status: gomemcached.E2BIG},
			expectedCode: http.StatusRequestEntityTooLarge,
			expectedText: "Too Large: ",
		},
		{
			name:         "MCResponse TMPFAIL",
			err:          &gomemcached.MCResponse{Status: gomemcached.TMPFAIL},
			expectedCode: http.StatusServiceUnavailable,
			expectedText: "Database server is over capacity (gomemcached.TMPFAIL)",
		},
		{
			name:         "MCResponse unhandled status",
			err:          &gomemcached.MCResponse{Status: gomemcached.ROLLBACK},
			expectedCode: http.StatusBadGateway,
			expectedText: fmt.Sprintf(" (%s)", gomemcached.ROLLBACK.String()),
		},
		{
			name:         "sgbucket.DocTooBigErr",
			err:          sgbucket.DocTooBigErr{},
			expectedCode: http.StatusRequestEntityTooLarge,
			expectedText: "Document too large!",
		},
		{
			name:         "sgbucket.MissingError",
			err:          sgbucket.MissingError{},
			expectedCode: http.StatusNotFound,
			expectedText: "missing",
		},
		{
			name:         "ErrNotFound",
			err:          ErrNotFound,
			expectedCode: http.StatusNotFound,
			expectedText: "missing",
		},
		{
			name:         "ErrEmptyDocument",
			err:          ErrEmptyDocument,
			expectedCode: http.StatusBadRequest,
			expectedText: "Document body is empty",
		},
		{
			name:         "json.SyntaxError",
			err:          &json.SyntaxError{},
			expectedCode: http.StatusBadRequest,
			expectedText: fmt.Sprintf("Invalid JSON: \"%v\"", (&json.SyntaxError{}).Error()),
		},
		{
			name:         "json.UnmarshalTypeError",
			err:          &json.UnmarshalTypeError{Value: "FakeValue", Type: reflect.TypeOf(1)},
			expectedCode: http.StatusBadRequest,
			expectedText: fmt.Sprintf("Invalid JSON: \"%v\"", (&json.UnmarshalTypeError{Value: "FakeValue", Type: reflect.TypeOf(1)}).Error()),
		},
		{
			name:         "JSONIterError",
			err:          &JSONIterError{E: &json.SyntaxError{}},
			expectedCode: http.StatusBadRequest,
			expectedText: fmt.Sprintf("Invalid JSON: \"%v\"", (&JSONIterError{E: &json.SyntaxError{}}).Error()),
		},
		{
			name:         "RetryTimeoutError",
			err:          &RetryTimeoutError{},
			expectedCode: http.StatusGatewayTimeout,
			expectedText: (&RetryTimeoutError{}).Error(),
		},
		{
			name:         "ErrAlreadyExists",
			err:          ErrAlreadyExists,
			expectedCode: http.StatusConflict,
			expectedText: "Conflict",
		},
		{
			name:         "ErrReplicationLimitExceeded",
			err:          ErrReplicationLimitExceeded,
			expectedCode: http.StatusServiceUnavailable,
			expectedText: ErrReplicationLimitExceeded.Error(),
		},
		{
			name:         "wrapped gocb.ErrDocumentNotFound",
			err:          fmt.Errorf("wrapped: %w", gocb.ErrDocumentNotFound),
			expectedCode: http.StatusNotFound,
			expectedText: "missing",
		},
		{
			name:         "wrapped gocb.ErrDocumentExists",
			err:          fmt.Errorf("wrapped: %w", gocb.ErrDocumentExists),
			expectedCode: http.StatusConflict,
			expectedText: "Conflict",
		},
		{
			name:         "wrapped gocb.ErrTimeout",
			err:          fmt.Errorf("wrapped: %w", gocb.ErrTimeout),
			expectedCode: http.StatusServiceUnavailable,
			expectedText: "Database timeout error (gocb.ErrTimeout)",
		},
		{
			name:         "*gocb.KeyValueError StatusTooBig",
			err:          &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrValueTooLarge), StatusCode: memd.StatusTooBig},
			expectedCode: http.StatusRequestEntityTooLarge,
			expectedText: "Document too large!",
		},
		{
			name:         "gocb.KeyValueError StatusTooBig",
			err:          gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrValueTooLarge), StatusCode: memd.StatusTooBig},
			expectedCode: http.StatusRequestEntityTooLarge,
			expectedText: "Document too large!",
		},
		{
			name:         "*gocbcore.KeyValueError StatusTooBig",
			err:          &gocbcore.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocbcore.ErrValueTooLarge), StatusCode: memd.StatusTooBig},
			expectedCode: http.StatusRequestEntityTooLarge,
			expectedText: "Document too large!",
		},
		{
			name:         "gocbcore.KeyValueError StatusTooBig",
			err:          gocbcore.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocbcore.ErrValueTooLarge), StatusCode: memd.StatusTooBig},
			expectedCode: http.StatusRequestEntityTooLarge,
			expectedText: "Document too large!",
		},
		{
			name:         "*gocb.KeyValueError ErrDocumentNotFound",
			err:          &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrDocumentNotFound), StatusCode: memd.StatusKeyNotFound},
			expectedCode: http.StatusNotFound,
			expectedText: "missing",
		},
		{
			name:         "*gocb.KeyValueError ErrDocumentExists",
			err:          &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrDocumentExists), StatusCode: memd.StatusKeyExists},
			expectedCode: http.StatusConflict,
			expectedText: "Conflict",
		},
		{
			name:         "*gocb.KeyValueError ErrTemporaryFailure",
			err:          &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrTemporaryFailure), StatusCode: memd.StatusTmpFail},
			expectedCode: http.StatusServiceUnavailable,
			expectedText: "Database server is over capacity (gocb.ErrTemporaryFailure)",
		},
		{
			name:         "gocb.ErrOverload in ncError",
			err:          fmt.Errorf("ncError: %w", gocb.ErrOverload),
			expectedCode: http.StatusServiceUnavailable,
			expectedText: "Database server is over capacity (gocb.ErrOverload)",
		},
		{
			name:         "*gocb.TimeoutError ErrAmbiguousTimeout",
			err:          &gocb.TimeoutError{InnerError: fmt.Errorf("ncError: %w", error(gocb.ErrAmbiguousTimeout))},
			expectedCode: http.StatusServiceUnavailable,
			expectedText: "Database timeout error (gocb.ErrTimeout)",
		},
		{
			name:         "gocbcore.SubDocumentError StatusSubDocPathNotFound",
			err:          gocbcore.SubDocumentError{InnerError: &gocbcore.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocbcore.ErrPathNotFound), StatusCode: memd.StatusSubDocPathNotFound}},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", gocbcore.SubDocumentError{InnerError: &gocbcore.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocbcore.ErrPathNotFound), StatusCode: memd.StatusSubDocPathNotFound}}),
		},
		{
			name:         "sgbucket.CasMismatchErr",
			err:          sgbucket.CasMismatchErr{},
			expectedCode: http.StatusConflict,
			expectedText: "Conflict",
		},
		{
			name:         "sgbucket.XattrMissingError",
			err:          sgbucket.XattrMissingError{},
			expectedCode: http.StatusNotFound,
			expectedText: "missing",
		},
		{
			name:         "pkgerrors wrapped ErrNotFound",
			err:          pkgerrors.Wrap(ErrNotFound, "wrapped"),
			expectedCode: http.StatusNotFound,
			expectedText: "missing",
		},
		{
			name:         "unhandled sgError",
			err:          ErrUpdateCancel,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrUpdateCancel),
		},
		{
			name:         "*gocb.KeyValueError ErrCasMismatch",
			err:          &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrCasMismatch), StatusCode: memd.StatusKeyExists},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrCasMismatch), StatusCode: memd.StatusKeyExists}),
		},
		{
			name:         "*gocb.KeyValueError ErrPathNotFound",
			err:          &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrPathNotFound), StatusCode: memd.StatusSubDocPathNotFound},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrPathNotFound), StatusCode: memd.StatusSubDocPathNotFound}),
		},
		{
			name:         "*gocb.KeyValueError ErrPathExists",
			err:          &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrPathExists), StatusCode: memd.StatusSubDocPathExists},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrPathExists), StatusCode: memd.StatusSubDocPathExists}),
		},
		{
			name:         "*gocb.KeyValueError ErrPathMismatch",
			err:          &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrPathMismatch), StatusCode: memd.StatusSubDocPathMismatch},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrPathMismatch), StatusCode: memd.StatusSubDocPathMismatch}),
		},
		{
			name:         "*gocb.KeyValueError ErrDeltaInvalid",
			err:          &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrDeltaInvalid), StatusCode: memd.StatusBadDelta},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrDeltaInvalid), StatusCode: memd.StatusBadDelta}),
		},
		{
			name:         "*gocb.KeyValueError ErrCollectionNotFound",
			err:          &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrCollectionNotFound), StatusCode: memd.StatusCollectionUnknown},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrCollectionNotFound), StatusCode: memd.StatusCollectionUnknown}),
		},
		{
			name:         "*gocb.KeyValueError ErrScopeNotFound",
			err:          &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrScopeNotFound), StatusCode: memd.StatusScopeUnknown},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrScopeNotFound), StatusCode: memd.StatusScopeUnknown}),
		},
		{
			name:         "*gocb.KeyValueError ErrAuthenticationFailure",
			err:          &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrAuthenticationFailure), StatusCode: memd.StatusAccessError},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrAuthenticationFailure), StatusCode: memd.StatusAccessError}),
		},
		{
			name:         "*gocb.KeyValueError ErrInternalServerFailure",
			err:          &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrInternalServerFailure), StatusCode: memd.StatusInternalError},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrInternalServerFailure), StatusCode: memd.StatusInternalError}),
		},
		{
			// TODO: CBG-6046 transient error should map to 503
			name:         "*gocb.KeyValueError ErrRateLimitedFailure",
			err:          &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrRateLimitedFailure), StatusCode: memd.StatusRateLimitedMaxCommands},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrRateLimitedFailure), StatusCode: memd.StatusRateLimitedMaxCommands}),
		},
		{
			name:         "*gocb.KeyValueError ErrQuotaLimitedFailure",
			err:          &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrQuotaLimitedFailure), StatusCode: memd.StatusRateLimitedScopeSizeLimitExceeded},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &gocb.KeyValueError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrQuotaLimitedFailure), StatusCode: memd.StatusRateLimitedScopeSizeLimitExceeded}),
		},
		{
			// TODO: CBG-6046 transient error should map to 503
			name:         "gocb.ErrCircuitBreakerOpen in ncError",
			err:          fmt.Errorf("ncError: %w", gocb.ErrCircuitBreakerOpen),
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", fmt.Errorf("ncError: %w", gocb.ErrCircuitBreakerOpen)),
		},
		{
			name:         "gocb.ErrRequestCanceled in ncError",
			err:          fmt.Errorf("ncError: %w", gocb.ErrRequestCanceled),
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", fmt.Errorf("ncError: %w", gocb.ErrRequestCanceled)),
		},
		{
			name:         "gocb.ErrServiceNotAvailable in ncError",
			err:          fmt.Errorf("ncError: %w", gocb.ErrServiceNotAvailable),
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", fmt.Errorf("ncError: %w", gocb.ErrServiceNotAvailable)),
		},
		{
			name:         "gocb.ErrShutdown",
			err:          gocb.ErrShutdown,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", gocb.ErrShutdown),
		},
		{
			name:         "*gocb.TimeoutError ErrUnambiguousTimeout",
			err:          &gocb.TimeoutError{InnerError: fmt.Errorf("ncError: %w", error(gocb.ErrUnambiguousTimeout))},
			expectedCode: http.StatusServiceUnavailable,
			expectedText: "Database timeout error (gocb.ErrTimeout)",
		},
		{
			name:         "*gocb.QueryError ErrIndexNotFound",
			err:          &gocb.QueryError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrIndexNotFound)},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &gocb.QueryError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrIndexNotFound)}),
		},
		{
			name:         "*gocb.QueryError ErrPlanningFailure",
			err:          &gocb.QueryError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrPlanningFailure)},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &gocb.QueryError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrPlanningFailure)}),
		},
		{
			name:         "*gocb.QueryError ErrIndexFailure",
			err:          &gocb.QueryError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrIndexFailure)},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &gocb.QueryError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrIndexFailure)}),
		},
		{
			name:         "*gocb.ViewError ErrViewNotFound",
			err:          &gocb.ViewError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrViewNotFound)},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &gocb.ViewError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrViewNotFound)}),
		},
		{
			name:         "*gocb.ViewError ErrDesignDocumentNotFound",
			err:          &gocb.ViewError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrDesignDocumentNotFound)},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &gocb.ViewError{InnerError: fmt.Errorf("ncError: %w", gocb.ErrDesignDocumentNotFound)}),
		},
		{
			name:         "sgbucket.ViewError",
			err:          sgbucket.ViewError{From: "view", Reason: "reason"},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", sgbucket.ViewError{From: "view", Reason: "reason"}),
		},
		{
			name:         "rosmar.ErrBucketClosed",
			err:          rosmar.ErrBucketClosed,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", rosmar.ErrBucketClosed),
		},
		{
			name:         "sgbucket.ErrCasFailureShouldRetry",
			err:          sgbucket.ErrCasFailureShouldRetry,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", sgbucket.ErrCasFailureShouldRetry),
		},
		{
			name:         "sgbucket.ErrPathNotFound",
			err:          sgbucket.ErrPathNotFound,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", sgbucket.ErrPathNotFound),
		},
		{
			name:         "sgbucket.ErrPathExists",
			err:          sgbucket.ErrPathExists,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", sgbucket.ErrPathExists),
		},
		{
			name:         "sgbucket.ErrPathMismatch",
			err:          sgbucket.ErrPathMismatch,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", sgbucket.ErrPathMismatch),
		},
		{
			name:         "ErrImportCancelledPurged",
			err:          ErrImportCancelledPurged,
			expectedCode: http.StatusNotFound,
			expectedText: "Import Cancelled Due to Purge",
		},
		{
			// TODO: CBG-6046 transient error should map to 503
			name:         "ErrTimeout",
			err:          ErrTimeout,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrTimeout),
		},
		{
			name:         "ErrRevTreeAddRevFailure",
			err:          ErrRevTreeAddRevFailure,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrRevTreeAddRevFailure),
		},
		{
			name:         "ErrImportCancelled",
			err:          ErrImportCancelled,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrImportCancelled),
		},
		{
			name:         "ErrAlreadyImported",
			err:          ErrAlreadyImported,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrAlreadyImported),
		},
		{
			name:         "ErrImportCasFailure",
			err:          ErrImportCasFailure,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrImportCasFailure),
		},
		{
			name:         "ErrImportCancelledFilter",
			err:          ErrImportCancelledFilter,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrImportCancelledFilter),
		},
		{
			name:         "ErrDocumentMigrated",
			err:          ErrDocumentMigrated,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrDocumentMigrated),
		},
		{
			name:         "ErrFatalBucketConnection",
			err:          ErrFatalBucketConnection,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrFatalBucketConnection),
		},
		{
			name:         "ErrAuthError",
			err:          ErrAuthError,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrAuthError),
		},
		{
			name:         "ErrIndexerError",
			err:          ErrIndexerError,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrIndexerError),
		},
		{
			name:         "ErrChannelFeed",
			err:          ErrChannelFeed,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrChannelFeed),
		},
		{
			name:         "ErrXattrNotFound",
			err:          ErrXattrNotFound,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrXattrNotFound),
		},
		{
			name:         "ErrXattrPartialFound",
			err:          ErrXattrPartialFound,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrXattrPartialFound),
		},
		{
			name:         "ErrPartialViewErrors",
			err:          ErrPartialViewErrors,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrPartialViewErrors),
		},
		{
			name:         "ErrDeltaSourceIsTombstone",
			err:          ErrDeltaSourceIsTombstone,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrDeltaSourceIsTombstone),
		},
		{
			name:         "ErrConfigVersionMismatch",
			err:          ErrConfigVersionMismatch,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrConfigVersionMismatch),
		},
		{
			name:         "ErrConfigRegistryRollback",
			err:          ErrConfigRegistryRollback,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrConfigRegistryRollback),
		},
		{
			name:         "ErrConfigRegistryReloadRequired",
			err:          ErrConfigRegistryReloadRequired,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrConfigRegistryReloadRequired),
		},
		{
			name:         "ErrSkippedSequencesMissing",
			err:          ErrSkippedSequencesMissing,
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", ErrSkippedSequencesMissing),
		},
		{
			name:         "SyncFnDryRunError HTTPError",
			err:          &SyncFnDryRunError{Err: &HTTPError{Status: http.StatusForbidden, Message: "forbidden"}},
			expectedCode: http.StatusForbidden,
			expectedText: "forbidden",
		},
		{
			name:         "ImportFilterDryRunError HTTPError",
			err:          &ImportFilterDryRunError{Err: &HTTPError{Status: http.StatusForbidden, Message: "forbidden"}},
			expectedCode: http.StatusForbidden,
			expectedText: "forbidden",
		},
		{
			name:         "FleeceDeltaError",
			err:          FleeceDeltaError{e: fmt.Errorf("fleece failure")},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", FleeceDeltaError{e: fmt.Errorf("fleece failure")}),
		},
		{
			name:         "MultiError ErrNotFound",
			err:          &MultiError{Errors: []error{ErrNotFound}},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &MultiError{Errors: []error{ErrNotFound}}),
		},
		{
			name:         "unrecognized error",
			err:          &json.UnsupportedTypeError{Type: reflect.TypeOf(3.14)},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", (&json.UnsupportedTypeError{Type: reflect.TypeOf(3.14)}).Error()),
		},
	}
}

// errorAsHTTPStatusWrappers are the ways Sync Gateway code wraps errors before they reach ErrorAsHTTPStatus.
var errorAsHTTPStatusWrappers = []struct {
	name string
	wrap func(error) error
}{
	{"fmt.Errorf", func(err error) error { return fmt.Errorf("wrapped: %w", err) }},
	{"pkgerrors.Wrap", func(err error) error { return pkgerrors.Wrap(err, "wrapped") }},
	{"pkgerrors.Wrap of fmt.Errorf", func(err error) error { return pkgerrors.Wrap(fmt.Errorf("wrapped: %w", err), "wrapped") }},
	{"RedactErrorf", func(err error) error { return RedactErrorf("wrapped: %w", err) }},
}

func TestErrorAsHTTPStatus(t *testing.T) {
	for _, tc := range errorAsHTTPStatusTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			code, text := ErrorAsHTTPStatus(tc.err)
			assert.Equal(t, tc.expectedCode, code)
			assert.Equal(t, tc.expectedText, text)
			if tc.err == nil {
				return
			}
			for _, wrapper := range errorAsHTTPStatusWrappers {
				t.Run(wrapper.name, func(t *testing.T) {
					code, text := ErrorAsHTTPStatus(wrapper.wrap(tc.err))
					assert.Equal(t, tc.expectedCode, code)
					// the 500 message includes the wrapper's text
					if tc.expectedCode != http.StatusInternalServerError {
						assert.Equal(t, tc.expectedText, text)
					}
				})
			}
		})
	}
}

// TestErrorAsHTTPStatusMultipleKnownErrors shows that when a chain holds more than one known error, the order of the
// checks in ErrorAsHTTPStatus picks the status, not which error is outermost.
func TestErrorAsHTTPStatusMultipleKnownErrors(t *testing.T) {
	errBucketGone := errors.New("bucket does not exist")
	testCases := []errorAsHTTPStatusTestCase{
		{
			name:         "unknown error then gocb.ErrTimeout",
			err:          fmt.Errorf("%w: %w", errBucketGone, gocb.ErrTimeout),
			expectedCode: http.StatusServiceUnavailable,
			expectedText: "Database timeout error (gocb.ErrTimeout)",
		},
		{
			name:         "gocb.ErrTemporaryFailure then gocb.ErrTimeout",
			err:          errors.Join(fmt.Errorf("vb1: %w", gocb.ErrTemporaryFailure), fmt.Errorf("vb2: %w", gocb.ErrTimeout)),
			expectedCode: http.StatusServiceUnavailable,
			expectedText: "Database timeout error (gocb.ErrTimeout)",
		},
		{
			name:         "HTTPError then ErrNotFound",
			err:          fmt.Errorf("%w: %w", &HTTPError{Status: http.StatusForbidden, Message: "forbidden"}, ErrNotFound),
			expectedCode: http.StatusNotFound,
			expectedText: "missing",
		},
		{
			name:         "ErrNotFound then HTTPError",
			err:          fmt.Errorf("%w: %w", ErrNotFound, &HTTPError{Status: http.StatusForbidden, Message: "forbidden"}),
			expectedCode: http.StatusNotFound,
			expectedText: "missing",
		},
		{
			name:         "ErrAlreadyExists then ErrNotFound",
			err:          errors.Join(ErrAlreadyExists, ErrNotFound),
			expectedCode: http.StatusNotFound,
			expectedText: "missing",
		},
		{
			name:         "gocb.ErrDocumentExists wrapping gocb.ErrTimeout",
			err:          fmt.Errorf("%w: %w", gocb.ErrDocumentExists, fmt.Errorf("inner: %w", gocb.ErrTimeout)),
			expectedCode: http.StatusConflict,
			expectedText: "Conflict",
		},
		{
			name:         "gocb.ErrTimeout wrapping gocb.ErrDocumentNotFound",
			err:          fmt.Errorf("%w: %w", gocb.ErrTimeout, fmt.Errorf("inner: %w", gocb.ErrDocumentNotFound)),
			expectedCode: http.StatusNotFound,
			expectedText: "missing",
		},
		{
			name:         "KeyValueError StatusTooBig wrapping gocb.ErrDocumentNotFound",
			err:          &gocb.KeyValueError{InnerError: gocb.ErrDocumentNotFound, StatusCode: memd.StatusTooBig},
			expectedCode: http.StatusNotFound,
			expectedText: "missing",
		},
		{
			name:         "two HTTPErrors",
			err:          errors.Join(&HTTPError{Status: http.StatusForbidden, Message: "forbidden"}, &HTTPError{Status: http.StatusUnauthorized, Message: "unauthorized"}),
			expectedCode: http.StatusForbidden,
			expectedText: "forbidden",
		},
		{
			name: "deep HTTPError in first branch then shallow HTTPError",
			err: fmt.Errorf("%w: %w",
				fmt.Errorf("inner: %w", &HTTPError{Status: http.StatusUnauthorized, Message: "unauthorized"}),
				&HTTPError{Status: http.StatusForbidden, Message: "forbidden"}),
			expectedCode: http.StatusUnauthorized,
			expectedText: "unauthorized",
		},
		{
			name:         "HTTPError then ErrEmptyDocument",
			err:          errors.Join(&HTTPError{Status: http.StatusForbidden, Message: "forbidden"}, ErrEmptyDocument),
			expectedCode: http.StatusBadRequest,
			expectedText: "Document body is empty",
		},
		{
			name:         "json.SyntaxError then MCResponse KEY_EEXISTS",
			err:          errors.Join(&json.SyntaxError{}, &gomemcached.MCResponse{Status: gomemcached.KEY_EEXISTS}),
			expectedCode: http.StatusConflict,
			expectedText: "Conflict",
		},
		{
			name:         "RetryTimeoutError then json.SyntaxError",
			err:          errors.Join(&RetryTimeoutError{}, &json.SyntaxError{}),
			expectedCode: http.StatusBadRequest,
			expectedText: fmt.Sprintf("Invalid JSON: \"%v\"", &json.SyntaxError{}),
		},
		{
			name:         "sgbucket.CasMismatchErr then sgbucket.MissingError",
			err:          errors.Join(sgbucket.CasMismatchErr{}, sgbucket.MissingError{}),
			expectedCode: http.StatusConflict,
			expectedText: "Conflict",
		},
		{
			name:         "RedactErrorf ErrNotFound with %v then ErrAlreadyExists with %w",
			err:          RedactErrorf("%v: %w", ErrNotFound, ErrAlreadyExists),
			expectedCode: http.StatusConflict,
			expectedText: "Conflict",
		},
		{
			name:         "MultiError does not unwrap",
			err:          &MultiError{Errors: []error{ErrNotFound, ErrAlreadyExists}},
			expectedCode: http.StatusInternalServerError,
			expectedText: fmt.Sprintf("Internal error: %v", &MultiError{Errors: []error{ErrNotFound, ErrAlreadyExists}}),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			code, text := ErrorAsHTTPStatus(tc.err)
			assert.Equal(t, tc.expectedCode, code)
			assert.Equal(t, tc.expectedText, text)
		})
	}
}

// TestErrorAsHTTPStatusWrappedInternalError shows which wrapped internal errors map to a different status than they
// did when ErrorAsHTTPStatus found the root error with pkgerrors.Cause. handler.writeError logs only a status of 500 or
// more, so an error that moves from 500 to 4xx is no longer logged.
func TestErrorAsHTTPStatusWrappedInternalError(t *testing.T) {
	testCases := []struct {
		name         string
		err          error
		previousCode int
		expectedCode int
	}{
		{
			name:         "config write CAS mismatch with RedactErrorf",
			err:          RedactErrorf("Error writing %q: %w", MD("_sync:dbconfig:db"), sgbucket.CasMismatchErr{}),
			previousCode: http.StatusInternalServerError,
			expectedCode: http.StatusConflict,
		},
		{
			name:         "ErrNotFound with fmt.Errorf",
			err:          fmt.Errorf("failed to get registry: %w", ErrNotFound),
			previousCode: http.StatusInternalServerError,
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "ErrAlreadyExists with fmt.Errorf",
			err:          fmt.Errorf("failed to create: %w", ErrAlreadyExists),
			previousCode: http.StatusInternalServerError,
			expectedCode: http.StatusConflict,
		},
		{
			name:         "HTTPError with fmt.Errorf",
			err:          fmt.Errorf("sync fn: %w", &HTTPError{Status: http.StatusForbidden, Message: "forbidden"}),
			previousCode: http.StatusInternalServerError,
			expectedCode: http.StatusForbidden,
		},
		{
			name:         "HTTPError with RedactErrorf",
			err:          RedactErrorf("sync fn: %w", &HTTPError{Status: http.StatusForbidden, Message: "forbidden"}),
			previousCode: http.StatusInternalServerError,
			expectedCode: http.StatusForbidden,
		},
		{
			// errors.Is already found gocb sentinels through fmt.Errorf
			name:         "gocb.KeyValueError not found with fmt.Errorf",
			err:          fmt.Errorf("failed to get registry: %w", gocb.KeyValueError{InnerError: gocb.ErrDocumentNotFound}),
			previousCode: http.StatusNotFound,
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "unknown error then gocb.ErrTimeout",
			err:          fmt.Errorf("%w: %w", errors.New("bucket does not exist"), gocb.ErrTimeout),
			previousCode: http.StatusServiceUnavailable,
			expectedCode: http.StatusServiceUnavailable,
		},
		{
			name:         "unknown error then gocb.ErrTemporaryFailure",
			err:          fmt.Errorf("%w: %w", errors.New("bucket does not exist"), gocb.ErrTemporaryFailure),
			previousCode: http.StatusInternalServerError,
			expectedCode: http.StatusServiceUnavailable,
		},
		{
			name:         "ErrNotFound with pkgerrors.Wrap",
			err:          pkgerrors.Wrap(ErrNotFound, "failed to get registry"),
			previousCode: http.StatusNotFound,
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "ErrNotFound with %v",
			err:          RedactErrorf("failed to get registry: %v", ErrNotFound),
			previousCode: http.StatusInternalServerError,
			expectedCode: http.StatusInternalServerError,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			code, _ := ErrorAsHTTPStatus(tc.err)
			assert.Equal(t, tc.expectedCode, code, "previous status was %d", tc.previousCode)
		})
	}
}

func BenchmarkErrorAsHTTPStatus(b *testing.B) {
	for _, tc := range errorAsHTTPStatusTestCases() {
		b.Run(tc.name, func(b *testing.B) {
			for b.Loop() {
				ErrorAsHTTPStatus(tc.err)
			}
		})
		if tc.err == nil {
			continue
		}
		for _, wrapper := range errorAsHTTPStatusWrappers {
			wrappedErr := wrapper.wrap(tc.err)
			b.Run(tc.name+"/"+wrapper.name, func(b *testing.B) {
				for b.Loop() {
					ErrorAsHTTPStatus(wrappedErr)
				}
			})
		}
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

func TestIsCollectionOutdatedError(t *testing.T) {
	// A timeout is only treated as terminal when KV_COLLECTION_OUTDATED is its sole retry reason -
	// a timeout that also lists transient reasons must stay retryable.
	testCases := []struct {
		name     string
		err      error
		expected bool
	}{
		{name: "nil", err: nil, expected: false},
		{name: "collection not found", err: gocb.ErrCollectionNotFound, expected: true},
		{name: "scope not found", err: gocb.ErrScopeNotFound, expected: true},
		{name: "wrapped collection not found", err: fmt.Errorf("get failed: %w", gocb.ErrCollectionNotFound), expected: true},
		{name: "document not found", err: gocb.ErrDocumentNotFound, expected: false},
		{
			name:     "timeout with no retry reasons",
			err:      &gocb.TimeoutError{InnerError: gocb.ErrTimeout},
			expected: false,
		},
		{
			name: "timeout from collection outdated only",
			err: &gocb.TimeoutError{InnerError: gocb.ErrTimeout,
				RetryReasons: []gocb.RetryReason{gocb.KVCollectionOutdatedRetryReason}},
			expected: true,
		},
		{
			name: "timeout with transient reason alongside collection outdated",
			err: &gocb.TimeoutError{InnerError: gocb.ErrTimeout,
				RetryReasons: []gocb.RetryReason{gocb.KVCollectionOutdatedRetryReason, gocb.KVNotMyVBucketRetryReason}},
			expected: false,
		},
		{
			name: "timeout from unrelated reason",
			err: &gocb.TimeoutError{InnerError: gocb.ErrTimeout,
				RetryReasons: []gocb.RetryReason{gocb.KVNotMyVBucketRetryReason}},
			expected: false,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expected, IsCollectionOutdatedError(testCase.err))
		})
	}
}
