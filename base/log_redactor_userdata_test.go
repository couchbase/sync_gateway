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

	b.Run("Enabled", func(bn *testing.B) {
		RedactUserData = true
		for i := 0; i < bn.N; i++ {
			username.Redact()
		}
	})

	b.Run("Disabled", func(bn *testing.B) {
		RedactUserData = false
		for i := 0; i < bn.N; i++ {
			username.Redact()
		}
	})
}
