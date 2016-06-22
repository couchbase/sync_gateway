// +build !windows

package base

import (
	"syscall"
	"testing"

	"github.com/couchbaselabs/go.assert"
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
	assert.False(t, requiresUpdate)

	limit.Cur = uint64(512)

	requiresUpdate, softFDLimit = getSoftFDLimit(
		requestedSoftFDLimit,
		limit,
	)
	assert.True(t, requiresUpdate)
	assert.Equals(t, softFDLimit, requestedSoftFDLimit)

}
