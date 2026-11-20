/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"fmt"
	"strings"
)

// RedactableError is an error that can be used as a drop-in replacement for an error,
// which has the ability to redact any sensitive data by calling redact() on all of its args.
type RedactableError struct {
	fmt     string
	args    []any
	wrapped []error // errors formatted with %w
}

var (
	_ error    = &RedactableError{}
	_ Redactor = &RedactableError{}
)

// RedactErrorf creates a new redactable error.  Same signature as fmt.Errorf() for easy drop-in replacement.
func RedactErrorf(format string, args ...any) *RedactableError {
	// no-op call to enable golang.org/x/tools/go/analysis/passes/printf checking in go vet
	if false {
		_ = fmt.Errorf(format, args...)
	}
	return &RedactableError{
		fmt:     format,
		args:    args,
		wrapped: wrappedErrors(format, args),
	}
}

// wrapPlaceholder stands in for the error argument at its index, so fmt.Errorf can find the %w arguments without
// formatting any of the real arguments.
type wrapPlaceholder int

func (wrapPlaceholder) Error() string { return "" }

// wrappedErrors returns the arguments that fmt.Errorf(format, args...) would wrap with %w.
func wrappedErrors(format string, args []any) []error {
	if !strings.ContainsRune(format, 'w') {
		return nil
	}
	var placeholders []any
	for i, arg := range args {
		if _, ok := arg.(error); ok {
			if placeholders == nil {
				placeholders = make([]any, len(args))
			}
			placeholders[i] = wrapPlaceholder(i)
		}
	}
	if placeholders == nil {
		return nil
	}
	// keep ints, which can be * width or precision operands
	for i, arg := range args {
		if n, ok := arg.(int); ok {
			placeholders[i] = n
		}
	}
	var wrapped []error
	switch typedErr := fmt.Errorf(format, placeholders...).(type) {
	case interface{ Unwrap() []error }:
		for _, err := range typedErr.Unwrap() {
			wrapped = append(wrapped, args[err.(wrapPlaceholder)].(error))
		}
	case interface{ Unwrap() error }:
		if err := typedErr.Unwrap(); err != nil {
			wrapped = []error{args[err.(wrapPlaceholder)].(error)}
		}
	}
	return wrapped
}

// Error satisfies the error interface
func (re *RedactableError) Error() string {
	return re.String()
}

// String returns a non-redacted version of the error - satisfies the Redactor interface.
func (re *RedactableError) String() string {
	// can't use Sprintf as it doesn't support `%w`
	return fmt.Errorf(re.fmt, re.args...).Error()
}

// Redact returns a redacted version of the error - satisfies the Redactor interface.
func (re *RedactableError) Redact() string {
	redactedArgs := redact(re.args)
	// can't use Sprintf as it doesn't support `%w`
	return fmt.Errorf(re.fmt, redactedArgs...).Error()
}

// Unwrap returns the errors formatted with %w, matching fmt.Errorf.
func (re *RedactableError) Unwrap() []error {
	return re.wrapped
}
