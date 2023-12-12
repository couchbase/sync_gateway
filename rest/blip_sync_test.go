// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHostOnlyCORS(t *testing.T) {
	const unparseableURL = "1http:///example.com"
	testsCases := []struct {
		input    []string
		output   []string
		hasError bool
	}{
		{
			input:  []string{"http://example.com"},
			output: []string{"example.com"},
		},
		{
			input:  []string{"https://example.com", "http://example.com"},
			output: []string{"example.com", "example.com"},
		},
		{
			input:  []string{"*", "http://example.com"},
			output: []string{"*", "example.com"},
		},
		{
			input:  []string{"wss://example.com"},
			output: []string{"example.com"},
		},
		{
			input:  []string{"http://example.com:12345"},
			output: []string{"example.com:12345"},
		},
		{
			input:    []string{unparseableURL},
			output:   nil,
			hasError: true,
		},
		{
			input:    []string{"*", unparseableURL},
			output:   []string{"*"},
			hasError: true,
		},
		{
			input:    []string{"*", unparseableURL, "http://example.com"},
			output:   []string{"*", "example.com"},
			hasError: true,
		},
	}
	for _, test := range testsCases {
		t.Run(fmt.Sprintf("%v->%v", test.input, test.output), func(t *testing.T) {
			output, err := hostOnlyCORS(test.input)
			if test.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, test.output, output)
		})
	}
}
