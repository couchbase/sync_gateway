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
			Name:     "maurice",
			Email:    "maurice@academie.fr",
			Channels: []string{"x", "y"},
			Roles:    []string{"a", "b"},
		},
	}

	allow := UserQueryAllow{}

	ch, err := allow.expandPattern("someChannel", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "someChannel")

	ch, err = allow.expandPattern("sales-$CITY-all", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "sales-Paris-all")

	ch, err = allow.expandPattern("sales$(CITY)All", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "salesParisAll")

	ch, err = allow.expandPattern("sales$CITY-$BREAD", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "salesParis-Baguette")

	ch, err = allow.expandPattern("sales-upTo-$YEAR", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "sales-upTo-2020")

	ch, err = allow.expandPattern("employee-$user", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "employee-maurice")

	// Should replace `$$` with `$`
	ch, err = allow.expandPattern("expen$$ive", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "expen$ive")

	// No-ops since the `$` does not match a pattern:
	ch, err = allow.expandPattern("$+wow", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "$+wow")

	ch, err = allow.expandPattern("foobar$", params)
	require.NoError(t, err)
	assert.Equal(t, ch, "foobar$")

	// error: param value is not a string
	_, err = allow.expandPattern("knows-$WORDS", params)
	assert.NotNil(t, err)

	// error: undefined parameter
	_, err = allow.expandPattern("sales-upTo-$FOO", params)
	assert.NotNil(t, err)
}
