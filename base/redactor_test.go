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
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedactHelper(t *testing.T) {
	RedactUserData = true
	defer func() { RedactUserData = false }()

	ptr := UserData("hello")

	var in = []interface{}{
		UserData("alice"),
		&ptr,
		"bob",
		1234,
		big.NewInt(1234),
		struct{}{},
	}

	out := redact(in)

	// Since redact performs an in-place redaction,
	// we'd expect the input slice to change too.
	assert.Equal(t, in, out)

	// Check that ptr wasn't redacted outside of the slice... this could be dangerous!
	assert.Equal(t, "hello", string(ptr))

	// Verify that only the types implementing Redactor have changed.
	assert.Equal(t, UserData("alice").Redact(), out[0])
	assert.Equal(t, ptr.Redact(), out[1])
	assert.Equal(t, "bob", out[2])
	assert.Equal(t, 1234, out[3])
	assert.Equal(t, big.NewInt(1234).String(), out[4].(*big.Int).String())
	assert.Equal(t, struct{}{}, out[5])
}

func TestSetRedaction(t *testing.T) {
	// Hits the default case
	SetRedaction(-1)
	assert.Equal(t, true, RedactUserData)

	SetRedaction(RedactFull)
	assert.Equal(t, true, RedactUserData)

	SetRedaction(RedactPartial)
	assert.Equal(t, true, RedactUserData)

	SetRedaction(RedactNone)
	assert.Equal(t, false, RedactUserData)
}

func TestRedactionLevelMarshalText(t *testing.T) {
	var level RedactionLevel
	level = RedactNone
	text, err := level.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "none", string(text))

	level = RedactPartial
	text, err = level.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "partial", string(text))

	level = RedactFull
	text, err = level.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "full", string(text))
}

func TestRedactionLevelUnmarshalText(t *testing.T) {
	var level RedactionLevel
	err := level.UnmarshalText([]byte("none"))
	assert.NoError(t, err)
	assert.Equal(t, RedactNone, level)

	err = level.UnmarshalText([]byte("partial"))
	assert.NoError(t, err)
	assert.Equal(t, RedactPartial, level)

	err = level.UnmarshalText([]byte("full"))
	assert.NoError(t, err)
	assert.Equal(t, RedactFull, level)

	err = level.UnmarshalText([]byte("unset"))
	assert.NoError(t, err)
	assert.Equal(t, RedactUnset, level)

	err = level.UnmarshalText([]byte("asdf"))
	assert.Equal(t, "unrecognized redaction level: \"asdf\"", err.Error())
}

func TestMixedTypeSliceRedaction(t *testing.T) {
	RedactMetadata = true
	RedactSystemData = true
	RedactUserData = true
	defer func() {
		RedactMetadata = false
		RedactSystemData = false
		RedactUserData = false
	}()

	slice := RedactorSlice{MD("cluster name"), SD("server ip"), UD("username")}
	assert.Equal(t, `[ `+metaDataPrefix+"cluster name"+metaDataSuffix+` `+systemDataPrefix+"server ip"+systemDataSuffix+" "+userDataPrefix+"username"+userDataSuffix+" ]", slice.Redact())
}

func BenchmarkRedactHelper(b *testing.B) {
	RedactUserData = true
	defer func() { RedactUserData = false }()

	var data = []interface{}{
		UserData("alice"),
		"bob",
		1234,
		big.NewInt(1234),
		struct{}{},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		redact(data)
	}
}
