package db

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEventTypeNames(t *testing.T) {
	// Ensure number of level constants, and names match.
	assert.Equal(t, int(eventTypeCount), len(eventTypeNames))

	assert.Equal(t, "DocumentChange", DocumentChange.String())
	assert.Equal(t, "DBStateChange", DBStateChange.String())
	assert.Equal(t, "WinningRevChange", WinningRevChange.String())

	// Test out of bounds event type
	assert.Equal(t, "EventType(255)", EventType(math.MaxUint8).String())
}
