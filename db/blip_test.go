// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSubprotocolString roundtrips the parse/format Subprotocol methods on the subprotocol constants.
func TestSubprotocolString(t *testing.T) {
	for i := minCBMobileSubprotocolVersion; i <= maxCBMobileSubprotocolVersion; i++ {
		str := i.SubprotocolString()
		parsed, err := ParseSubprotocolString(str)
		require.NoError(t, err)
		require.Equal(t, i, parsed)
	}
}
