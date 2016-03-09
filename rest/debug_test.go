package rest

import (
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func TestMaxSnapshotsRingBuffer(t *testing.T) {

	for i := 0; i < kMaxGoroutineSnapshots*2; i++ {
		grTracker.recordSnapshot()
	}
	assert.True(t, len(grTracker.Snapshots) <= kMaxGoroutineSnapshots)

}
