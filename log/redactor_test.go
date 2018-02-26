package log

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

	out := Redact(in)

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
		Redact(data)
	}
}
