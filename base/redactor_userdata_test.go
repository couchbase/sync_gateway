package base

import (
	"math/big"
	"testing"

	goassert "github.com/couchbaselabs/go.assert"
)

const (
	// This is intentionally brittle (hardcoded redaction tags)
	// We'd probably want to know if this got changed by accident...
	userDataPrefix = "<ud>"
	userDataSuffix = "</ud>"
)

func TestUserDataRedact(t *testing.T) {
	username := "alice"
	userdata := UserData(username)

	RedactUserData = true
	goassert.Equals(t, userdata.Redact(), userDataPrefix+username+userDataSuffix)

	RedactUserData = false
	goassert.Equals(t, userdata.Redact(), username)
}

func TestUD(t *testing.T) {
	RedactUserData = true
	defer func() { RedactUserData = false }()

	// Straight-forward string test.
	ud := UD("hello world")
	goassert.Equals(t, ud.Redact(), userDataPrefix+"hello world"+userDataSuffix)

	// big.Int fulfils the Stringer interface, so we should get sensible values.
	ud = UD(big.NewInt(1234))
	goassert.Equals(t, ud.Redact(), userDataPrefix+"1234"+userDataSuffix)

	// Even plain structs could be redactable.
	ud = UD(struct{}{})
	goassert.Equals(t, ud.Redact(), userDataPrefix+"{}"+userDataSuffix)
}

func BenchmarkUserDataRedact(b *testing.B) {
	username := UserData("alice")

	// We'd expect a minor performance hit when redaction is enabled.
	b.Run("Enabled", func(bn *testing.B) {
		RedactUserData = true
		for i := 0; i < bn.N; i++ {
			username.Redact()
		}
	})

	// When redaction is disabled, we should see no performance hit.
	b.Run("Disabled", func(bn *testing.B) {
		RedactUserData = false
		for i := 0; i < bn.N; i++ {
			username.Redact()
		}
	})
}
