//go:build !race
// +build !race

/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

// The tests in this file are time-sensitive and disabled when running with the -race flag.  See SG #3055

package auth

import (
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

// Test that multiple authentications of the same user/password are fast.
// This is an important check because the underlying bcrypt algorithm used to verify passwords
// is _extremely_ slow (~100ms!) so we use a cache to speed it up (see password_hash.go).
func TestAuthenticationSpeed(t *testing.T) {

	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket.Bucket, nil, DefaultAuthenticatorOptions())
	user, _ := auth.NewUser("me", "goIsKewl", nil)
	assert.True(t, user.Authenticate("goIsKewl"))

	start := time.Now()
	for i := 0; i < 1000; i++ {
		assert.True(t, user.Authenticate("goIsKewl"))
	}
	durationPerAuth := time.Since(start) / 1000
	if durationPerAuth > time.Millisecond {
		t.Errorf("user.Authenticate is too slow: %v", durationPerAuth)
	}
}
