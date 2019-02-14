package base

import (
	"math/big"
	"testing"

	goassert "github.com/couchbaselabs/go.assert"
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
	goassert.DeepEquals(t, out, in)

	// Check that ptr wasn't redacted outside of the slice... this could be dangerous!
	goassert.Equals(t, string(ptr), "hello")

	// Verify that only the types implementing Redactor have changed.
	goassert.Equals(t, out[0], UserData("alice").Redact())
	goassert.Equals(t, out[1], ptr.Redact())
	goassert.Equals(t, out[2], "bob")
	goassert.Equals(t, out[3], 1234)
	goassert.Equals(t, out[4].(*big.Int).String(), big.NewInt(1234).String())
	goassert.Equals(t, out[5], struct{}{})
}

func TestSetRedaction(t *testing.T) {
	// Hits the default case
	SetRedaction(-1)
	goassert.Equals(t, RedactUserData, false)

	SetRedaction(RedactFull)
	goassert.Equals(t, RedactUserData, true)

	SetRedaction(RedactPartial)
	goassert.Equals(t, RedactUserData, true)

	SetRedaction(RedactNone)
	goassert.Equals(t, RedactUserData, false)
}

func TestRedactionLevelMarshalText(t *testing.T) {
	var level RedactionLevel
	level = RedactNone
	text, err := level.MarshalText()
	goassert.Equals(t, err, nil)
	goassert.Equals(t, string(text), "none")

	level = RedactPartial
	text, err = level.MarshalText()
	goassert.Equals(t, err, nil)
	goassert.Equals(t, string(text), "partial")

	level = RedactFull
	text, err = level.MarshalText()
	goassert.Equals(t, err, nil)
	goassert.Equals(t, string(text), "full")
}

func TestRedactionLevelUnmarshalText(t *testing.T) {
	var level RedactionLevel
	err := level.UnmarshalText([]byte("none"))
	goassert.Equals(t, err, nil)
	goassert.Equals(t, level, RedactNone)

	err = level.UnmarshalText([]byte("partial"))
	goassert.Equals(t, err, nil)
	goassert.Equals(t, level, RedactPartial)

	err = level.UnmarshalText([]byte("full"))
	goassert.Equals(t, err, nil)
	goassert.Equals(t, level, RedactFull)

	err = level.UnmarshalText([]byte("asdf"))
	goassert.Equals(t, err.Error(), "unrecognized redaction level: \"asdf\"")
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
