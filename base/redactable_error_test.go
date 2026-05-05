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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
