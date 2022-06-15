/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test channel-name parameter expansion for user query specifications
func TestExpandChannel(t *testing.T) {
	params := map[string]interface{}{
		"CITY":  "Paris",
		"BREAD": "Baguette",
		"YEAR":  2020,
		"WORDS": []string{"ouais", "fromage", "amour"},
		"user": &userQueryUserInfo{
			name:     "maurice",
			email: "maurice@academie.fr",
			channels: []string{"x", "y"},
			roles:    []string{"a", "b"},
		},
	}

	ch, err := expandChannelPattern("query", "someChannel", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "someChannel")

	ch, err = expandChannelPattern("query", "sales-$CITY-all", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "sales-Paris-all")

	ch, err = expandChannelPattern("query", "sales$(CITY)All", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "salesParisAll")

	ch, err = expandChannelPattern("query", "sales$CITY-$BREAD", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "salesParis-Baguette")

	ch, err = expandChannelPattern("query", "sales-upTo-$YEAR", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "sales-upTo-2020")

	ch, err = expandChannelPattern("query", "employee-$user", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "employee-maurice")

	// Should replace `$$` with `$`
	ch, err = expandChannelPattern("query", "expen$$ive", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "expen$ive")

	// No-ops since the `$` does not match a pattern:
	ch, err = expandChannelPattern("query", "$+wow", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "$+wow")

	ch, err = expandChannelPattern("query", "foobar$", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "foobar$")

	// error: param value is not a string
	ch, err = expandChannelPattern("query", "knows-$WORDS", params)
	assert.NotNil(t, err)

	// error: undefined parameter
	ch, err = expandChannelPattern("query", "sales-upTo-$FOO", params)
	assert.NotNil(t, err)
}
