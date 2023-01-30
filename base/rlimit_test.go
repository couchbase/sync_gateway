// +build !windows

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
