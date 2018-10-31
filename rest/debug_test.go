package rest

import (
	"testing"

	goassert "github.com/couchbaselabs/go.assert"
)

func TestMaxSnapshotsRingBuffer(t *testing.T) {

	for i := 0; i < kMaxGoroutineSnapshots*2; i++ {
		grTracker.recordSnapshot()
	}
	goassert.True(t, len(grTracker.Snapshots) <= kMaxGoroutineSnapshots)

}
