package base

import (
	"math/big"
	"testing"

	assert "github.com/couchbaselabs/go.assert"
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
	assert.DeepEquals(t, out, in)

	// Check that ptr wasn't redacted outside of the slice... this could be dangerous!
	assert.Equals(t, string(ptr), "hello")

	// Verify that only the types implementing Redactor have changed.
	assert.Equals(t, out[0], UserData("alice").Redact())
	assert.Equals(t, out[1], ptr.Redact())
	assert.Equals(t, out[2], "bob")
	assert.Equals(t, out[3], 1234)
	assert.Equals(t, out[4].(*big.Int).String(), big.NewInt(1234).String())
	assert.Equals(t, out[5], struct{}{})
}

func TestSetRedaction(t *testing.T) {
	// Hits the default case
	SetRedaction(-1)
	assert.Equals(t, RedactUserData, false)

	SetRedaction(REDACT_FULL)
	assert.Equals(t, RedactUserData, true)

	SetRedaction(REDACT_PARTIAL)
	assert.Equals(t, RedactUserData, true)

	SetRedaction(REDACT_NONE)
	assert.Equals(t, RedactUserData, false)
}

func TestRedactionLevelMarshalText(t *testing.T) {
	var level RedactionLevel
	level = REDACT_NONE
	text, err := level.MarshalText()
	assert.Equals(t, err, nil)
	assert.Equals(t, string(text), "none")

	level = REDACT_PARTIAL
	text, err = level.MarshalText()
	assert.Equals(t, err, nil)
	assert.Equals(t, string(text), "partial")

	level = REDACT_FULL
	text, err = level.MarshalText()
	assert.Equals(t, err, nil)
	assert.Equals(t, string(text), "full")
}

func TestRedactionLevelUnmarshalText(t *testing.T) {
	var level RedactionLevel
	err := level.UnmarshalText([]byte("none"))
	assert.Equals(t, err, nil)
	assert.Equals(t, level, REDACT_NONE)

	err = level.UnmarshalText([]byte("partial"))
	assert.Equals(t, err, nil)
	assert.Equals(t, level, REDACT_PARTIAL)

	err = level.UnmarshalText([]byte("full"))
	assert.Equals(t, err, nil)
	assert.Equals(t, level, REDACT_FULL)

	err = level.UnmarshalText([]byte("asdf"))
	assert.Equals(t, err.Error(), "unrecognized level: asdf")
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
