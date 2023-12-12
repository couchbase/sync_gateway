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
	testsCases := []struct {
		input  []string
		output []string
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
	}
	for _, test := range testsCases {
		t.Run(fmt.Sprintf("%v->%v", test.input, test.output), func(t *testing.T) {
			output, err := hostOnlyCORS(test.input)
			assert.NoError(t, err)
			assert.Equal(t, test.output, output)
		})
	}
}
