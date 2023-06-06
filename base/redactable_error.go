/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import "fmt"

// RedactableError is an error that can be used as a drop-in replacement for an error,
// which has the ability to redact any sensitive data by calling redact() on all of its args.
type RedactableError struct {
	fmt  string
	args []interface{}
}

var (
	_ error    = &RedactableError{}
	_ Redactor = &RedactableError{}
)

// Create a new redactable error.  Same signature as fmt.Errorf() for easy drop-in replacement.
func RedactErrorf(fmt string, args ...interface{}) *RedactableError {
	return &RedactableError{
		fmt:  fmt,
		args: args,
	}
}

// Error satisfies the error interface
func (re *RedactableError) Error() string {
	return re.String()
}

// String returns a non-redacted version of the error - satisfies the Redactor interface.
func (re *RedactableError) String() string {
	return fmt.Sprintf(re.fmt, re.args...)
}

// Redact returns a redacted version of the error - satisfies the Redactor interface.
func (re *RedactableError) Redact() string {
	redactedArgs := redact(re.args)
	return fmt.Sprintf(re.fmt, redactedArgs...)
}
