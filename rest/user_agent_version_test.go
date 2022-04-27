/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUserAgentVersion(t *testing.T) {

	userAgentVersion := NewUserAgentVersion("")
	assert.Equal(t, 0, userAgentVersion.MajorVersion())
	assert.Equal(t, 0, userAgentVersion.MinorVersion())

	userAgentVersion = NewUserAgentVersion("CouchbaseLite/1.0.0")
	assert.Equal(t, 1, userAgentVersion.MajorVersion())
	assert.Equal(t, 0, userAgentVersion.MinorVersion())

	userAgentVersion = NewUserAgentVersion("CouchbaseLite/1.1.0")
	assert.Equal(t, 1, userAgentVersion.MajorVersion())
	assert.Equal(t, 1, userAgentVersion.MinorVersion())

	userAgentVersion = NewUserAgentVersion("CouchbaseLite/1.2 (suff/goes/here)")
	assert.Equal(t, 1, userAgentVersion.MajorVersion())
	assert.Equal(t, 2, userAgentVersion.MinorVersion())

	userAgentVersion = NewUserAgentVersion("CouchbaseLite/1.2(suff.goes.here)")
	assert.Equal(t, 1, userAgentVersion.MajorVersion())
	assert.Equal(t, 2, userAgentVersion.MinorVersion())

	userAgentVersion = NewUserAgentVersion("CouchbaseLite/1.2 suff.goes/here)")
	assert.Equal(t, 1, userAgentVersion.MajorVersion())
	assert.Equal(t, 2, userAgentVersion.MinorVersion())

	userAgentVersion = NewUserAgentVersion("whatever/thing$$h3ysuz1!!///")
	assert.Equal(t, 0, userAgentVersion.MajorVersion())
	assert.Equal(t, 0, userAgentVersion.MinorVersion())

	userAgentVersion = NewUserAgentVersion("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9")
	assert.Equal(t, 0, userAgentVersion.MajorVersion())
	assert.Equal(t, 0, userAgentVersion.MinorVersion())

}

func TestUserAgentVersionIsVersionAfter(t *testing.T) {

	userAgentVersion := NewUserAgentVersion("CouchbaseLite/0.3 suff.goes/here)")
	assert.True(t, userAgentVersion.IsBefore(1, 2))
	assert.True(t, userAgentVersion.IsEqualToOrAfter(0, 2))

	userAgentVersion = NewUserAgentVersion("CouchbaseLite/1.1.1 suff.goes/here)")
	assert.True(t, userAgentVersion.IsBefore(1, 3))
	assert.True(t, userAgentVersion.IsEqualToOrAfter(1, 0))

	userAgentVersion = NewUserAgentVersion("CouchbaseLite/1.3 suff.goes/here)")
	assert.True(t, userAgentVersion.IsEqualToOrAfter(1, 3))
	assert.True(t, userAgentVersion.IsEqualToOrAfter(1, 2))
	assert.True(t, userAgentVersion.IsBefore(1, 4))

}
