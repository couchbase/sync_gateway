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
	"time"

	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
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

	// String slice test.
	ud = UD([]string{"hello", "world", "o/"})
	goassert.Equals(t, ud.Redact(), "[ "+userDataPrefix+"hello"+userDataSuffix+" "+userDataPrefix+"world"+userDataSuffix+" "+userDataPrefix+"o/"+userDataSuffix+" ]")

	// Set
	ud = UD(SetOf("hello", "world"))
	// As a set comes from a map we can't be sure which order it'll end up with so should check both permutations
	redactedPerm1 := "{" + userDataPrefix + "hello" + userDataSuffix + ", " + userDataPrefix + "world" + userDataSuffix + "}"
	redactedPerm2 := "{" + userDataPrefix + "world" + userDataSuffix + ", " + userDataPrefix + "hello" + userDataSuffix + "}"
	redactedSet := ud.Redact()
	redactedCorrectly := redactedPerm1 == redactedSet || redactedPerm2 == redactedSet
	assert.True(t, redactedCorrectly, "Unexpected redact got %v", redactedSet)
}

func BenchmarkUserDataRedact(b *testing.B) {
	username := UserData("alice")
	usernameSlice := UD([]string{"adam", "ben", "jacques"})

	// We'd expect a minor performance hit when redaction is enabled.
	b.Run("Enabled", func(bn *testing.B) {
		RedactUserData = true
		for i := 0; i < bn.N; i++ {
			username.Redact()
		}
	})

	b.Run("EnabledSlice", func(bn *testing.B) {
		RedactUserData = true
		for i := 0; i < bn.N; i++ {
			usernameSlice.Redact()
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

type FakeLogger struct {
}

func (fakeLogger FakeLogger) String() string {
	time.Sleep(10 * time.Microsecond)
	return "Fixed String"
}

func BenchmarkRedactOnLog(b *testing.B) {

	defer SetUpBenchmarkLogging(LevelWarn, KeyAll)()

	b.Run("WarnPlain", func(b *testing.B) {
		ctx := TestCtx(b)
		for i := 0; i < b.N; i++ {
			WarnfCtx(ctx, "Log: %s", "Fixed String")
		}
	})

	b.Run("WarnRedactTrueNotUD", func(b *testing.B) {
		ctx := TestCtx(b)
		RedactUserData = true
		for i := 0; i < b.N; i++ {
			WarnfCtx(ctx, "Log: %s", FakeLogger{})
		}
	})

	b.Run("WarnRedactTrueUD", func(b *testing.B) {
		ctx := TestCtx(b)
		RedactUserData = true
		for i := 0; i < b.N; i++ {
			WarnfCtx(ctx, "Log: %s", UD(FakeLogger{}))
		}
	})

	b.Run("WarnRedactFalseNotUD", func(b *testing.B) {
		ctx := TestCtx(b)
		RedactUserData = false
		for i := 0; i < b.N; i++ {
			WarnfCtx(ctx, "Log: %s", FakeLogger{})
		}
	})

	b.Run("WarnRedactFalseUD", func(b *testing.B) {
		ctx := TestCtx(b)
		RedactUserData = false
		for i := 0; i < b.N; i++ {
			WarnfCtx(ctx, "Log: %s", UD(FakeLogger{}))
		}
	})

	b.Run("DebugPlain", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Debugf(KeyAll, "Log: %s", "Fixed String")
		}
	})

	b.Run("DebugRedactTrueNotUD", func(b *testing.B) {
		RedactUserData = true
		for i := 0; i < b.N; i++ {
			Debugf(KeyAll, "Log: %s", FakeLogger{})
		}
	})

	b.Run("DebugRedactTrueUD", func(b *testing.B) {
		RedactUserData = true
		for i := 0; i < b.N; i++ {
			Debugf(KeyAll, "Log: %s", UD(FakeLogger{}))
		}
	})

	b.Run("DebugRedactFalseNotUD", func(b *testing.B) {
		RedactUserData = false
		for i := 0; i < b.N; i++ {
			Debugf(KeyAll, "Log: %s", FakeLogger{})
		}
	})

	b.Run("DebugRedactFalseUD", func(b *testing.B) {
		RedactUserData = false
		for i := 0; i < b.N; i++ {
			Debugf(KeyAll, "Log: %s", UD(FakeLogger{}))
		}
	})
}
