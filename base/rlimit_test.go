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
