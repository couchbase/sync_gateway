/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

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

	// Test out of bounds event type
	assert.Equal(t, "EventType(255)", EventType(math.MaxUint8).String())
}
