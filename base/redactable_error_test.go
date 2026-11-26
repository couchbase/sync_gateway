// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestRedactErrorf(t *testing.T) {
	tests := []struct {
		name,
		expectedString,
		expectedRedact,
		fmt string
		args []any
	}{
		{
			name:           "%s",
			expectedString: "Couldn't get user \"Bob\": Not Found",
			expectedRedact: "Couldn't get user \"<ud>Bob</ud>\": Not Found",
			fmt:            "Couldn't get user %q: %s",
			args:           []any{UD("Bob"), ErrNotFound},
		},
		{
			name:           "%w",
			expectedString: "Couldn't get user \"Bob\": Not Found",
			expectedRedact: "Couldn't get user \"<ud>Bob</ud>\": Not Found",
			fmt:            "Couldn't get user %q: %w",
			args:           []any{UD("Bob"), ErrNotFound},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := RedactErrorf(test.fmt, test.args...)
			assert.Equal(t, test.expectedString, err.String())
			assert.Equal(t, test.expectedRedact, err.Redact())
		})
	}
}

func TestRedactableError_IsAs(t *testing.T) {
	innerErr := &customErr{msg: "inner error"}
	re := RedactErrorf("wrapped: %w", innerErr)

	t.Run("errors.Is", func(t *testing.T) {
		require.ErrorIs(t, re, innerErr)
	})

	t.Run("errors.As", func(t *testing.T) {
		var target *customErr
		assert.True(t, errors.As(re, &target), "errors.As(re, &target) should be true")
		assert.Equal(t, innerErr, target)

		var redactedTarget *RedactableError
		assert.True(t, errors.As(re, &redactedTarget), "errors.As(re, &redactedTarget) should be true")
		assert.Equal(t, re, redactedTarget)
	})

	t.Run("errors.AsType", func(t *testing.T) {
		err := &customErr{msg: "err"}
		re := RedactErrorf("wrapped: %w", err)

		target, ok := errors.AsType[*customErr](re)
		assert.True(t, ok)
		assert.Equal(t, err, target)

		redactedTarget, ok := errors.AsType[*RedactableError](re)
		assert.True(t, ok)
		assert.Equal(t, re, redactedTarget)
	})

	t.Run("errors.As value type", func(t *testing.T) {
		err := customValErr("val error")
		re := RedactErrorf("wrapped: %w", err)

		var target customValErr
		assert.True(t, errors.As(re, &target))
		assert.Equal(t, err, target)
	})

	t.Run("errors.AsType value type", func(t *testing.T) {
		err := customValErr("val error")
		re := RedactErrorf("wrapped: %w", err)

		target, ok := errors.AsType[customValErr](re)
		assert.True(t, ok)
		assert.Equal(t, err, target)
	})

	t.Run("Multiple wraps", func(t *testing.T) {
		err1 := &customErr{msg: "err1"}
		err2 := &customErr{msg: "err2"}
		re2 := RedactErrorf("err1: %w, err2: %w", err1, err2)

		assert.True(t, errors.Is(re2, err1))
		assert.True(t, errors.Is(re2, err2))

		var target *customErr
		assert.True(t, errors.As(re2, &target))
		assert.Equal(t, err1, target) // errors.As returns the first one it finds
	})
	t.Run("Redactor interface survives Unwrap, multiple redactable errors", func(t *testing.T) {
		inner := RedactErrorf("user %s", UD("Bob"))
		outer := RedactErrorf("wrapped: %w", inner)

		// Outer redacts correctly
		assert.Contains(t, outer.Redact(), "user <ud>Bob</ud>")

		// errors.As does not unextract inner
		var target *RedactableError
		require.True(t, errors.As(outer, &target))

		// Unwrapped inner still redacts
		var r Redactor = target
		assert.Contains(t, r.Redact(), "user <ud>Bob</ud>")
		assert.NotContains(t, r.Redact(), "user Bob")
	})
	t.Run("Redactor interface survives Unwrap", func(t *testing.T) {
		inner := RedactErrorf("user %s", UD("Bob"))
		outer := fmt.Errorf("wrapped: %w", inner)

		// errors.As extracts inner, still satisfies Redactor
		var target *RedactableError
		require.True(t, errors.As(outer, &target))
		require.NotSame[any](t, outer, target)

		// Unwrapped inner still redacts
		var r Redactor = target
		assert.Contains(t, r.Redact(), "user <ud>Bob</ud>")
		assert.NotContains(t, r.Redact(), "user Bob")
	})

}

// TestRedactableErrorUnwrapOnlyWrapVerb checks that RedactableError unwraps the same errors as fmt.Errorf, so an error
// formatted with a verb other than %w stays hidden from errors.Is and errors.As.
func TestRedactableErrorUnwrapOnlyWrapVerb(t *testing.T) {
	err1 := &customErr{msg: "err1"}
	err2 := &customErr{msg: "err2"}
	var nilErr error
	var typedNilErr *customErr
	testCases := []struct {
		name            string
		format          string
		args            []any
		expectedWrapped []error
	}{
		{
			name:   "%v",
			format: "formatted: %v",
			args:   []any{err1},
		},
		{
			name:   "%s",
			format: "formatted: %s",
			args:   []any{err1},
		},
		{
			name:   "%+v",
			format: "formatted: %+v",
			args:   []any{err1},
		},
		{
			name:   "%q",
			format: "formatted: %q",
			args:   []any{err1},
		},
		{
			name:   "escaped %%w then %v",
			format: "formatted: %%w %v",
			args:   []any{err1},
		},
		{
			name:   "no error arguments",
			format: "user %s",
			args:   []any{UD("Bob")},
		},
		{
			name:            "%w",
			format:          "wrapped: %w",
			args:            []any{err1},
			expectedWrapped: []error{err1},
		},
		{
			name:            "%v then %w",
			format:          "formatted: %v, wrapped: %w",
			args:            []any{err1, err2},
			expectedWrapped: []error{err2},
		},
		{
			name:            "%w then %v",
			format:          "wrapped: %w, formatted: %v",
			args:            []any{err1, err2},
			expectedWrapped: []error{err1},
		},
		{
			name:            "%w twice",
			format:          "wrapped: %w, wrapped: %w",
			args:            []any{err1, err2},
			expectedWrapped: []error{err1, err2},
		},
		{
			name:            "explicit argument indexes",
			format:          "wrapped: %[2]w, formatted: %[1]v",
			args:            []any{err1, err2},
			expectedWrapped: []error{err2},
		},
		{
			name:   "%w nil",
			format: "wrapped: %w",
			args:   []any{nilErr},
		},
		{
			name:            "%w nil then %w",
			format:          "wrapped: %w, wrapped: %w",
			args:            []any{nilErr, err1},
			expectedWrapped: []error{err1},
		},
		{
			name:   "%w of a non-error",
			format: "wrapped: %w",
			args:   []any{"not an error"},
		},
		{
			name:            "%w of a typed nil error",
			format:          "wrapped: %w",
			args:            []any{typedNilErr},
			expectedWrapped: []error{typedNilErr},
		},
		{
			name:            "* width operand before %w",
			format:          "%*d wrapped: %w",
			args:            []any{5, 3, err1},
			expectedWrapped: []error{err1},
		},
		{
			name:            "same error with %w twice",
			format:          "wrapped: %[1]w, wrapped: %[1]w",
			args:            []any{err1},
			expectedWrapped: []error{err1},
		},
		{
			name:            "%w of a UD error",
			format:          "user %s, wrapped: %w",
			args:            []any{UD("Bob"), err1},
			expectedWrapped: []error{err1},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			re := RedactErrorf(tc.format, tc.args...)
			assert.Equal(t, tc.expectedWrapped, re.Unwrap())
			assert.Equal(t, tc.expectedWrapped, fmtErrorfWrapped(fmt.Errorf(tc.format, tc.args...)))
			for _, arg := range tc.args {
				argErr, ok := arg.(error)
				if !ok {
					continue
				}
				if slices.Contains(tc.expectedWrapped, argErr) {
					assert.ErrorIs(t, re, argErr)
				} else {
					assert.NotErrorIs(t, re, argErr)
				}
			}
		})
	}
}

// fmtErrorfWrapped returns the errors that an error from fmt.Errorf wraps, as a slice.
func fmtErrorfWrapped(err error) []error {
	switch typedErr := err.(type) {
	case interface{ Unwrap() []error }:
		return typedErr.Unwrap()
	case interface{ Unwrap() error }:
		if inner := typedErr.Unwrap(); inner != nil {
			return []error{inner}
		}
	}
	return nil
}

// TestRedactableErrorNonWrapVerbHidesError shows that callers that check the chain with errors.Is or errors.As do not
// see an error that RedactErrorf formats with %v or %s.
func TestRedactableErrorNonWrapVerbHidesError(t *testing.T) {
	testCases := []struct {
		name           string
		err            error
		docNotFound    bool
		httpStatusCode int
	}{
		{
			name:           "ErrNotFound with %w",
			err:            RedactErrorf("get %s: %w", MD("doc"), ErrNotFound),
			docNotFound:    true,
			httpStatusCode: http.StatusNotFound,
		},
		{
			name:           "ErrNotFound with %v",
			err:            RedactErrorf("get %s: %v", MD("doc"), ErrNotFound),
			docNotFound:    false,
			httpStatusCode: http.StatusInternalServerError,
		},
		{
			name:           "ErrNotFound with %s",
			err:            RedactErrorf("get %s: %s", MD("doc"), ErrNotFound),
			docNotFound:    false,
			httpStatusCode: http.StatusInternalServerError,
		},
		{
			name:           "sgbucket.MissingError with %w",
			err:            RedactErrorf("get: %w", sgbucket.MissingError{Key: "doc"}),
			docNotFound:    true,
			httpStatusCode: http.StatusNotFound,
		},
		{
			name:           "sgbucket.MissingError with %v",
			err:            RedactErrorf("get: %v", sgbucket.MissingError{Key: "doc"}),
			docNotFound:    false,
			httpStatusCode: http.StatusInternalServerError,
		},
		{
			name:           "%w inside %v",
			err:            RedactErrorf("outer: %v", RedactErrorf("inner: %w", ErrNotFound)),
			docNotFound:    false,
			httpStatusCode: http.StatusInternalServerError,
		},
		{
			name:           "%v inside %w",
			err:            RedactErrorf("outer: %w", RedactErrorf("inner: %v", ErrNotFound)),
			docNotFound:    false,
			httpStatusCode: http.StatusInternalServerError,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.docNotFound, IsDocNotFoundError(tc.err))
			code, _ := ErrorAsHTTPStatus(tc.err)
			assert.Equal(t, tc.httpStatusCode, code)
		})
	}
}

type customErr struct{ msg string }

func (e *customErr) Error() string { return e.msg }

type customValErr string

func (e customValErr) Error() string { return string(e) }

func BenchmarkRedactErrorf(b *testing.B) {
	fmt := "Couldn't get user %q: "
	fmtVerbs := []string{"%s", "%w"}
	args := []any{UD("Bob"), ErrNotFound}

	for _, verb := range fmtVerbs {
		err := RedactErrorf(fmt+verb, args...)
		b.Run(verb+" String()", func(b *testing.B) {
			for b.Loop() {
				_ = err.String()
			}
		})
		b.Run(verb+" Redact()", func(b *testing.B) {
			for b.Loop() {
				_ = err.Redact()
			}
		})
	}
}

func BenchmarkRedactErrorfCreate(b *testing.B) {
	doc := map[string]any{"channels": []string{"a", "b", "c"}, "body": strings.Repeat("x", 1024)}
	testCases := []struct {
		name   string
		format string
		args   []any
	}{
		{"no error", "user %s, seq %d", []any{UD("Bob"), 42}},
		{"%v error", "user %s: %v", []any{UD("Bob"), ErrNotFound}},
		{"%w error", "user %s: %w", []any{UD("Bob"), ErrNotFound}},
		{"%+v doc with %w", "doc %+v: %w", []any{UD(doc), ErrNotFound}},
	}
	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for b.Loop() {
				_ = RedactErrorf(tc.format, tc.args...)
			}
		})
	}
}
