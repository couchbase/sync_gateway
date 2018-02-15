package base

import (
	"testing"

	assert "github.com/couchbaselabs/go.assert"
)

func TestUserDataRedact(t *testing.T) {
	username := UserData("alice")

	// This is intentionally brittle (hardcoded redaction tags)
	// We'd want to know if this changed by accident...
	assert.Equals(t, username.Redact(), "<ud>alice</ud>")
}

func BenchmarkUserDataRedact(b *testing.B) {
	username := UserData("alice")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		username.Redact()
	}
}
