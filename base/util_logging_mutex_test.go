// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoggingMutex(t *testing.T) {
	SetUpTestLogging(t, LevelTrace, KeyAll)

	tests := []struct {
		name     string
		getMutex func() *LoggingMutex
	}{
		{
			name: "initialized",
			getMutex: func() *LoggingMutex {
				m := NewLoggingMutex(TestCtx(t), "test")
				return &m
			},
		},
		{
			name: "zero value",
			getMutex: func() *LoggingMutex {
				return &LoggingMutex{}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := test.getMutex()

			m.Lock()
			assert.False(t, m.TryLock())
			m.Unlock()
			assert.True(t, m.TryLock())
		})
	}

}

func TestLoggingRWMutex(t *testing.T) {
	SetUpTestLogging(t, LevelTrace, KeyAll)

	tests := []struct {
		name     string
		getMutex func() *LoggingRWMutex
	}{
		{
			name: "initialized",
			getMutex: func() *LoggingRWMutex {
				m := NewLoggingRWMutex(TestCtx(t), "test")
				return &m
			},
		},
		{
			name: "zero value",
			getMutex: func() *LoggingRWMutex {
				return &LoggingRWMutex{}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := test.getMutex()

			m.Lock()
			assert.False(t, m.TryLock())
			assert.False(t, m.TryRLock())
			m.Unlock()

			m.RLock()
			assert.False(t, m.TryLock())
			assert.True(t, m.TryRLock())
			m.RUnlock() // TryRLock()
			m.RUnlock() // RLock()

			assert.True(t, m.TryLock())
			m.Unlock() // TryLock()

			assert.True(t, m.TryRLock())
		})
	}
}
