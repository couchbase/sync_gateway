// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedactErrorf(t *testing.T) {
	tests := []struct {
		name,
		expectedString,
		expectedRedact,
		fmt string
		args []interface{}
	}{
		{
			name:           "%s",
			expectedString: "Couldn't get user \"Bob\": Not Found",
			expectedRedact: "Couldn't get user \"<ud>Bob</ud>\": Not Found",
			fmt:            "Couldn't get user %q: %s",
			args:           []interface{}{UD("Bob"), ErrNotFound},
		},
		{
			name:           "%w",
			expectedString: "Couldn't get user \"Bob\": Not Found",
			expectedRedact: "Couldn't get user \"<ud>Bob</ud>\": Not Found",
			fmt:            "Couldn't get user %q: %w",
			args:           []interface{}{UD("Bob"), ErrNotFound},
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

func BenchmarkRedactErrorf(b *testing.B) {
	fmt := "Couldn't get user %q: "
	fmtVerbs := []string{"%s", "%w"}
	args := []interface{}{UD("Bob"), ErrNotFound}

	for _, verb := range fmtVerbs {
		err := RedactErrorf(fmt+verb, args...)
		b.Run(verb+" String()", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = err.String()
			}
		})
		b.Run(verb+" Redact()", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = err.Redact()
			}
		})
	}
}
