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
	"testing"

	"github.com/stretchr/testify/assert"
)

func genExpectedForTest(t testing.TB, seqs ...string) []SequenceID {
	result := make([]SequenceID, 0, len(seqs))
	for _, seq := range seqs {
		s, err := ParsePlainSequenceID(seq)
		if err != nil {
			t.Fatalf("Error parsing sequence %q for test setup: %v", seq, err)
		}
		result = append(result, s)
	}
	return result
}

func genProcessedForTest(t testing.TB, seqs ...string) map[SequenceID]struct{} {
	result := make(map[SequenceID]struct{}, len(seqs))
	for _, seq := range seqs {
		s, err := ParsePlainSequenceID(seq)
		if err != nil {
			t.Fatalf("Error parsing sequence %q for test setup: %v", seq, err)
		}
		result[s] = struct{}{}
	}
	return result
}

func TestCheckpointerSafeSeq(t *testing.T) {

	tests := []struct {
		name                    string
		c                       *Checkpointer
		expectedSafeSeq         *SequenceID
		expectedExpectedSeqsIdx int
		expectedExpectedSeqs    []SequenceID
		expectedProcessedSeqs   map[SequenceID]struct{}
	}{
		{
			name: "empty",
			c: &Checkpointer{
				expectedSeqs:  genExpectedForTest(t),
				processedSeqs: genProcessedForTest(t),
			},
			expectedSafeSeq:         nil,
			expectedExpectedSeqsIdx: -1,
			expectedExpectedSeqs:    genExpectedForTest(t),
			expectedProcessedSeqs:   genProcessedForTest(t),
		},
		{
			name: "none processed",
			c: &Checkpointer{
				expectedSeqs:  genExpectedForTest(t, "1", "2", "3"),
				processedSeqs: genProcessedForTest(t),
			},
			expectedSafeSeq:         nil,
			expectedExpectedSeqsIdx: -1,
			expectedExpectedSeqs:    genExpectedForTest(t, "1", "2", "3"),
			expectedProcessedSeqs:   genProcessedForTest(t),
		},
		{
			name: "partial processed",
			c: &Checkpointer{
				expectedSeqs:  genExpectedForTest(t, "1", "2", "3"),
				processedSeqs: genProcessedForTest(t, "1"),
			},
			expectedSafeSeq:         &SequenceID{Seq: 1},
			expectedExpectedSeqsIdx: 0,
			expectedExpectedSeqs:    genExpectedForTest(t, "2", "3"),
			expectedProcessedSeqs:   genProcessedForTest(t),
		},
		{
			name: "partial processed with gap",
			c: &Checkpointer{
				expectedSeqs:  genExpectedForTest(t, "1", "2", "3"),
				processedSeqs: genProcessedForTest(t, "1", "3"),
			},
			expectedSafeSeq:         &SequenceID{Seq: 1},
			expectedExpectedSeqsIdx: 0,
			expectedExpectedSeqs:    genExpectedForTest(t, "2", "3"),
			expectedProcessedSeqs:   genProcessedForTest(t, "3"),
		},
		{
			name: "fully processed",
			c: &Checkpointer{
				expectedSeqs:  genExpectedForTest(t, "1", "2", "3"),
				processedSeqs: genProcessedForTest(t, "1", "2", "3"),
			},
			expectedSafeSeq:         &SequenceID{Seq: 3},
			expectedExpectedSeqsIdx: 2,
			expectedExpectedSeqs:    genExpectedForTest(t),
			expectedProcessedSeqs:   genProcessedForTest(t),
		},
		{
			name: "extra processed",
			c: &Checkpointer{
				expectedSeqs:  genExpectedForTest(t, "1", "2", "3"),
				processedSeqs: genProcessedForTest(t, "1", "2", "3", "4", "5"),
			},
			expectedSafeSeq:         &SequenceID{Seq: 3},
			expectedExpectedSeqsIdx: 2,
			expectedExpectedSeqs:    genExpectedForTest(t),
			expectedProcessedSeqs:   genProcessedForTest(t, "4", "5"),
		},
		{
			name: "out of order expected seqs",
			c: &Checkpointer{
				expectedSeqs:  genExpectedForTest(t, "3", "2", "1"),
				processedSeqs: genProcessedForTest(t, "1", "2", "3"),
			},
			expectedSafeSeq:         &SequenceID{Seq: 3},
			expectedExpectedSeqsIdx: 2,
			expectedExpectedSeqs:    genExpectedForTest(t),
			expectedProcessedSeqs:   genProcessedForTest(t),
		},
		{
			name: "compound sequence",
			c: &Checkpointer{
				expectedSeqs:  genExpectedForTest(t, "1", "1::3"),
				processedSeqs: genProcessedForTest(t, "1", "1::3"),
			},
			expectedSafeSeq:         &SequenceID{Seq: 3, LowSeq: 1},
			expectedExpectedSeqsIdx: 1,
			expectedExpectedSeqs:    genExpectedForTest(t),
			expectedProcessedSeqs:   genProcessedForTest(t),
		},
		{
			name: "compound sequence triggered by",
			c: &Checkpointer{
				expectedSeqs:  genExpectedForTest(t, "1", "1::3", "4:2"),
				processedSeqs: genProcessedForTest(t, "1", "1::3", "4:2"),
			},
			expectedSafeSeq:         &SequenceID{Seq: 2, TriggeredBy: 4},
			expectedExpectedSeqsIdx: 2,
			expectedExpectedSeqs:    genExpectedForTest(t),
			expectedProcessedSeqs:   genProcessedForTest(t),
		},
		{
			// ensure we maintain enough sequences that we can checkpoint expected but not yet processed without retaining the full list of processed sequences
			// in most cases this will be keeping the last processed sequence and removing all prior ones, until the missing sequence in the list.
			// e.g.
			//    expected:  [2 3 4 5 6]
			//    processed: [  3 4 5  ]
			// can be safely compacted to:
			//    expected:  [2 5 6]
			//    processed: [  5  ]
			name: "processed compaction",
			c: &Checkpointer{
				expectedSeqs:                   genExpectedForTest(t, "1", "2", "3", "4", "5", "6"),
				processedSeqs:                  genProcessedForTest(t, "1", "3", "4", "5"),
				expectedSeqCompactionThreshold: 3, // this many expected seqs to trigger compaction
			},
			expectedSafeSeq:         &SequenceID{Seq: 1},
			expectedExpectedSeqsIdx: 0,
			expectedExpectedSeqs:    genExpectedForTest(t, "2", "5", "6"),
			expectedProcessedSeqs:   genProcessedForTest(t, "5"),
		},
		{
			name: "multiple skipped processed compaction",
			c: &Checkpointer{
				expectedSeqs:                   genExpectedForTest(t, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"),
				processedSeqs:                  genProcessedForTest(t, "1", "2", "4", "5", "7", "8", "9", "11", "12", "13", "15", "16", "17", "19"),
				expectedSeqCompactionThreshold: 5, // this many expected seqs to trigger compaction
			},
			expectedSafeSeq:         &SequenceID{Seq: 2},
			expectedExpectedSeqsIdx: 1,
			expectedExpectedSeqs:    genExpectedForTest(t, "3", "5", "6", "9", "10", "13", "14", "17", "18", "19", "20"),
			expectedProcessedSeqs:   genProcessedForTest(t, "5", "9", "13", "17", "19"),
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
				if tt.expectedSafeSeq == nil {
					assert.Nil(t, actualSafeSeq)
				} else {
					assert.Equal(t, *tt.expectedSafeSeq, *actualSafeSeq)
				}
				assert.Equal(t, tt.expectedExpectedSeqs, tt.c.expectedSeqs)
				assert.Equal(t, tt.expectedProcessedSeqs, tt.c.processedSeqs)

				// _updateCheckpointLists should be idempotent
				// we'd expect no safe seq, and no further changes to either list, as long as c.processedSeqs hasn't been added to
				actualSafeSeq2 := tt.c._updateCheckpointLists()
				assert.Nil(t, actualSafeSeq2)
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
		{expectedSeqsLen: 50, processedSeqsLen: 50},
		{expectedSeqsLen: 400, processedSeqsLen: 400}, // ~expected size (2x changes batch)
		{expectedSeqsLen: 1000, processedSeqsLen: 1000},
		{expectedSeqsLen: 1000, processedSeqsLen: 10000},
		{expectedSeqsLen: 100000, processedSeqsLen: 100000},
		{expectedSeqsLen: 1000000, processedSeqsLen: 1000000},
	}
	for _, test := range tests {
		// -1    no skip
		//  0    skip first
		//  1    skip last
		for _, numCheckpoints := range []int{1, 10} {
			for _, skipSeq := range []int{-1, 0, 1} {
				bFunc := func(skipSeq, numCheckpoints int) func(b *testing.B) {
					return func(b *testing.B) {
						expectedSeqs := make([]SequenceID, 0, test.expectedSeqsLen)
						for i := 0; i < test.expectedSeqsLen; i++ {
							expectedSeqs = append(expectedSeqs, SequenceID{Seq: uint64(i)})
						}
						processedSeqs := make(map[SequenceID]struct{}, test.processedSeqsLen)
						for i := 0; i < test.processedSeqsLen; i++ {
							if (skipSeq == 0 && i == 0) || (skipSeq == 1 && i == test.processedSeqsLen-1) {
								continue
							}
							processedSeqs[SequenceID{Seq: uint64(i)}] = struct{}{}
						}
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							c := &Checkpointer{expectedSeqs: expectedSeqs, processedSeqs: processedSeqs, expectedSeqCompactionThreshold: 100}
							// run checkpointing multiple times to test pruning speedup
							for j := 0; j < numCheckpoints; j++ {
								_ = c._updateCheckpointLists()
							}
						}
					}
				}
				b.Run(
					fmt.Sprintf("expectedSeqsLen=%d,processedSeqsLen=%d,skipSeq=%d,numCheckpoints=%d",
						test.expectedSeqsLen, test.processedSeqsLen, skipSeq, numCheckpoints),
					bFunc(skipSeq, numCheckpoints),
				)
			}
		}
	}
}
