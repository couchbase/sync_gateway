// +build !windows

/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"syscall"
	"testing"

	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetSoftFDLimitWithCurrent(t *testing.T) {

	requestedSoftFDLimit := uint64(1024)
	currentSoftFdLimit := uint64(2048)
	currentHardFdLimit := uint64(4096)

	limit := syscall.Rlimit{
		Cur: currentSoftFdLimit,
		Max: currentHardFdLimit,
	}

	requiresUpdate, softFDLimit := getSoftFDLimit(
		requestedSoftFDLimit,
		limit,
	)
	goassert.False(t, requiresUpdate)

	limit.Cur = uint64(512)

	requiresUpdate, softFDLimit = getSoftFDLimit(
		requestedSoftFDLimit,
		limit,
	)
	goassert.True(t, requiresUpdate)
	goassert.Equals(t, softFDLimit, requestedSoftFDLimit)

}

func TestSetMaxFileDescriptors(t *testing.T) {
	defer SetUpTestLogging(LevelDebug, KeyAll)()

	// grab current limits
	var limits syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limits)
	require.NoError(t, err)

	// noop
	n, err := SetMaxFileDescriptors(0)
	assert.NoError(t, err)
	assert.Equal(t, 0, int(n))

	// noop (current limit < new limit)
	n, err = SetMaxFileDescriptors(limits.Cur - 1)
	assert.NoError(t, err)
	assert.Equal(t, 0, int(n))

	// noop (current limit == new limit)
	n, err = SetMaxFileDescriptors(limits.Cur)
	assert.NoError(t, err)
	assert.Equal(t, 0, int(n))

	// increase
	n, err = SetMaxFileDescriptors(limits.Cur + 2)
	assert.NoError(t, err)
	assert.Equal(t, int(limits.Cur+2), int(n))

	// noop (we don't decrease limits)
	n, err = SetMaxFileDescriptors(limits.Cur + 1)
	assert.NoError(t, err)
	assert.Equal(t, 0, int(n))
}
