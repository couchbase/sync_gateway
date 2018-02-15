package base

import (
	"testing"

	assert "github.com/couchbaselabs/go.assert"
)

func TestUserDataRedact(t *testing.T) {
	username := "alice"
	userdata := UserData(username)

	RedactUserData = true

	// This is intentionally brittle (hardcoded redaction tags)
	// We'd probably want to know if this changed by accident...
	assert.Equals(t, userdata.Redact(), "<ud>"+username+"</ud>")

	RedactUserData = false
	assert.Equals(t, userdata.Redact(), username)
}

func BenchmarkUserDataRedact(b *testing.B) {
	username := UserData("alice")
	RedactUserData = true

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		username.Redact()
	}
}

func BenchmarkUserDataRedactDisabled(b *testing.B) {
	username := UserData("alice")
	RedactUserData = false

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		username.Redact()
	}
}
