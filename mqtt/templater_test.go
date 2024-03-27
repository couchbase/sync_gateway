//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package mqtt

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTemplater(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339, "2024-04-11T12:41:31.016-07:00")
	tmpl := templater{
		payload:   Body{"temp": 98.6, "foo": "bar", "numstr": "98.6", "a": []any{10, "ten"}},
		timestamp: ts,
		topic:     TopicMatch{Name: "temp/ear", Wildcards: []string{"ear"}},
	}

	var result any
	result = tmpl.expand("$now")
	require.NoError(t, tmpl.err)
	assert.Equal(t, int64(1712864491), result)

	result = tmpl.expand("${now.unix}")
	require.NoError(t, tmpl.err)
	assert.Equal(t, int64(1712864491), result)

	result = tmpl.expand("${now.iso8601}")
	require.NoError(t, tmpl.err)
	assert.Equal(t, "2024-04-11T12:41:31.016-07:00", result)

	result = tmpl.expand("${message.payload.temp}")
	require.NoError(t, tmpl.err)
	assert.Equal(t, 98.6, result)

	result = tmpl.expand("${message.topic}")
	require.NoError(t, tmpl.err)
	assert.Equal(t, "temp/ear", result)

	// Error: can't substitute a non-string into a string:
	_ = tmpl.expand("${message.payload.temp} degrees")
	assert.Error(t, tmpl.err)
	tmpl.err = nil

	result = tmpl.expand("${message.payload.temp|string} degrees")
	require.NoError(t, tmpl.err)
	assert.Equal(t, "98.6 degrees", result)

	result = tmpl.expand("Temperature at $1 is ${message.payload.temp|string} degrees.")
	require.NoError(t, tmpl.err)
	assert.Equal(t, "Temperature at ear is 98.6 degrees.", result)

	result = tmpl.expand("${message.payload.numstr | number}")
	require.NoError(t, tmpl.err)
	assert.Equal(t, 98.6, result)

	result = tmpl.expand("${message.payload.numstr | number | round}")
	require.NoError(t, tmpl.err)
	assert.Equal(t, int64(99), result)

	result = tmpl.expand("${message.payload.a[0]}")
	require.NoError(t, tmpl.err)
	assert.Equal(t, 10, result)

	result = tmpl.expand("${message.payload.a[1]}")
	require.NoError(t, tmpl.err)
	assert.Equal(t, "ten", result)

	result = tmpl.expand("${message.payload.a |json}")
	require.NoError(t, tmpl.err)
	assert.Equal(t, "[10,\"ten\"]", result)

	result = tmpl.expand("${message.payload.missing}")
	require.NoError(t, tmpl.err)
	assert.Nil(t, result)

	// Error: nil value with "required" filter
	_ = tmpl.expand("${message.payload.missing | required}")
	assert.Error(t, tmpl.err)
	tmpl.err = nil
}
