package base

import (
	"math/big"
	"testing"

	assert "github.com/couchbaselabs/go.assert"
)

func TestUserDataRedact(t *testing.T) {
	username := "alice"
	userdata := UserData(username)

	RedactUserData = true
	defer func() { RedactUserData = false }()

	// This is intentionally brittle (hardcoded redaction tags)
	// We'd probably want to know if this changed by accident...
	assert.Equals(t, userdata.Redact(), "<ud>"+username+"</ud>")

	RedactUserData = false
	assert.Equals(t, userdata.Redact(), username)
}

func TestToUD(t *testing.T) {

	RedactUserData = true
	defer func() { RedactUserData = false }()

	ud := ToUD(big.NewInt(1234))
	assert.Equals(t, ud.Redact(), userDataPrefix+"1234"+userDataSuffix)

	ud = ToUD("hello world")
	assert.Equals(t, ud.Redact(), userDataPrefix+"hello world"+userDataSuffix)

	ud = ToUD(struct{}{})
	assert.Equals(t, ud.Redact(), userDataPrefix+"{}"+userDataSuffix)
}

func BenchmarkUserDataRedact(b *testing.B) {
	username := UserData("alice")

	RedactUserData = true
	defer func() { RedactUserData = false }()

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
