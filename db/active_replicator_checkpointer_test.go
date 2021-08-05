/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckpointerSafeSeq(t *testing.T) {
	tests := []struct {
		name                    string
		c                       *Checkpointer
		expectedSafeSeq         string
		expectedExpectedSeqsIdx int
		expectedExpectedSeqs    []string
		expectedProcessedSeqs   map[string]struct{}
	}{
		{
			name: "empty",
			c: &Checkpointer{
				expectedSeqs:  []string{},
				processedSeqs: map[string]struct{}{},
			},
			expectedSafeSeq:         "",
			expectedExpectedSeqsIdx: -1,
			expectedExpectedSeqs:    []string{},
			expectedProcessedSeqs:   map[string]struct{}{},
		},
		{
			name: "none processed",
			c: &Checkpointer{
				expectedSeqs:  []string{"1", "2", "3"},
				processedSeqs: map[string]struct{}{},
			},
			expectedSafeSeq:         "",
			expectedExpectedSeqsIdx: -1,
			expectedExpectedSeqs:    []string{"1", "2", "3"},
			expectedProcessedSeqs:   map[string]struct{}{},
		},
		{
			name: "partial processed",
			c: &Checkpointer{
				expectedSeqs:  []string{"1", "2", "3"},
				processedSeqs: map[string]struct{}{"1": {}},
			},
			expectedSafeSeq:         "1",
			expectedExpectedSeqsIdx: 0,
			expectedExpectedSeqs:    []string{"2", "3"},
			expectedProcessedSeqs:   map[string]struct{}{},
		},
		{
			name: "partial processed with gap",
			c: &Checkpointer{
				expectedSeqs:  []string{"1", "2", "3"},
				processedSeqs: map[string]struct{}{"1": {}, "3": {}},
			},
			expectedSafeSeq:         "1",
			expectedExpectedSeqsIdx: 0,
			expectedExpectedSeqs:    []string{"2", "3"},
			expectedProcessedSeqs:   map[string]struct{}{"3": {}},
		},
		{
			name: "fully processed",
			c: &Checkpointer{
				expectedSeqs:  []string{"1", "2", "3"},
				processedSeqs: map[string]struct{}{"1": {}, "2": {}, "3": {}},
			},
			expectedSafeSeq:         "3",
			expectedExpectedSeqsIdx: 2,
			expectedExpectedSeqs:    []string{},
			expectedProcessedSeqs:   map[string]struct{}{},
		},
		{
			name: "extra processed",
			c: &Checkpointer{
				expectedSeqs:  []string{"1", "2", "3"},
				processedSeqs: map[string]struct{}{"1": {}, "2": {}, "3": {}, "4": {}, "5": {}},
			},
			expectedSafeSeq:         "3",
			expectedExpectedSeqsIdx: 2,
			expectedExpectedSeqs:    []string{},
			expectedProcessedSeqs:   map[string]struct{}{"4": {}, "5": {}},
		},
		{
			name: "out of order expected seqs",
			c: &Checkpointer{
				expectedSeqs:  []string{"3", "2", "1"},
				processedSeqs: map[string]struct{}{"1": {}, "2": {}, "3": {}},
			},
			expectedSafeSeq:         "3",
			expectedExpectedSeqsIdx: 2,
			expectedExpectedSeqs:    []string{},
			expectedProcessedSeqs:   map[string]struct{}{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("_calculateSafeExpectedSeqsIdx", func(t *testing.T) {
				actualIdx := tt.c._calculateSafeExpectedSeqsIdx()
				assert.Equal(t, tt.expectedExpectedSeqsIdx, actualIdx)
			})

			t.Run("_updateCheckpointLists", func(t *testing.T) {
				actualSafeSeq := tt.c._updateCheckpointLists()
				assert.Equal(t, tt.expectedSafeSeq, actualSafeSeq)
				assert.Equal(t, tt.expectedExpectedSeqs, tt.c.expectedSeqs)
				assert.Equal(t, tt.expectedProcessedSeqs, tt.c.processedSeqs)

				// _updateCheckpointLists should be idempotent
				// we'd expect no safe seq, and no further changes to either list, as long as c.processedSeqs hasn't been added to
				actualSafeSeq2 := tt.c._updateCheckpointLists()
				assert.Equal(t, "", actualSafeSeq2)
				assert.Equal(t, tt.expectedExpectedSeqs, tt.c.expectedSeqs)
				assert.Equal(t, tt.expectedProcessedSeqs, tt.c.processedSeqs)
			})
		})
	}
}
