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
	"fmt"
	"strconv"
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

func BenchmarkCheckpointerUpdateCheckpointLists(b *testing.B) {
	tests := []struct {
		expectedSeqsLen  int
		processedSeqsLen int
	}{
		{expectedSeqsLen: 1, processedSeqsLen: 1},
		{expectedSeqsLen: 10, processedSeqsLen: 10},
		{expectedSeqsLen: 100, processedSeqsLen: 100},
		{expectedSeqsLen: 500, processedSeqsLen: 500},
		{expectedSeqsLen: 1000, processedSeqsLen: 1000},
		{expectedSeqsLen: 10000, processedSeqsLen: 10000},
		{expectedSeqsLen: 50000, processedSeqsLen: 50000},
		{expectedSeqsLen: 100000, processedSeqsLen: 100000},
	}

	for _, test := range tests {
		b.Run(fmt.Sprintf("expectedSeqsLen=%d,processedSeqsLen=%d", test.expectedSeqsLen, test.processedSeqsLen), func(b *testing.B) {
			expectedSeqs := make([]string, 0, test.expectedSeqsLen)
			for i := 0; i < test.expectedSeqsLen; i++ {
				expectedSeqs = append(expectedSeqs, strconv.Itoa(i))
			}
			processedSeqs := make(map[string]struct{}, test.processedSeqsLen)
			for i := 0; i < test.processedSeqsLen; i++ {
				processedSeqs[strconv.Itoa(i)] = struct{}{}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c := &Checkpointer{expectedSeqs: expectedSeqs, processedSeqs: processedSeqs}
				_ = c._updateCheckpointLists()
			}
		})
	}
}
