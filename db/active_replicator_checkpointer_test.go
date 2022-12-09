package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
				expectedSeqs:  []SequenceID{},
				processedSeqs: map[SequenceID]struct{}{},
			},
			expectedSafeSeq:         nil,
			expectedExpectedSeqsIdx: -1,
			expectedExpectedSeqs:    []SequenceID{},
			expectedProcessedSeqs:   map[SequenceID]struct{}{},
		},
		{
			name: "none processed",
			c: &Checkpointer{
				expectedSeqs:  []SequenceID{{Seq: 1}, {Seq: 2}, {Seq: 3}},
				processedSeqs: map[SequenceID]struct{}{},
			},
			expectedSafeSeq:         nil,
			expectedExpectedSeqsIdx: -1,
			expectedExpectedSeqs:    []SequenceID{{Seq: 1}, {Seq: 2}, {Seq: 3}},
			expectedProcessedSeqs:   map[SequenceID]struct{}{},
		},
		{
			name: "partial processed",
			c: &Checkpointer{
				expectedSeqs:  []SequenceID{{Seq: 1}, {Seq: 2}, {Seq: 3}},
				processedSeqs: map[SequenceID]struct{}{{Seq: 1}: {}},
			},
			expectedSafeSeq:         &SequenceID{Seq: 1},
			expectedExpectedSeqsIdx: 0,
			expectedExpectedSeqs:    []SequenceID{{Seq: 2}, {Seq: 3}},
			expectedProcessedSeqs:   map[SequenceID]struct{}{},
		},
		{
			name: "partial processed with gap",
			c: &Checkpointer{
				expectedSeqs:  []SequenceID{{Seq: 1}, {Seq: 2}, {Seq: 3}},
				processedSeqs: map[SequenceID]struct{}{{Seq: 1}: {}, {Seq: 3}: {}},
			},
			expectedSafeSeq:         &SequenceID{Seq: 1},
			expectedExpectedSeqsIdx: 0,
			expectedExpectedSeqs:    []SequenceID{{Seq: 2}, {Seq: 3}},
			expectedProcessedSeqs:   map[SequenceID]struct{}{{Seq: 3}: {}},
		},
		{
			name: "fully processed",
			c: &Checkpointer{
				expectedSeqs:  []SequenceID{{Seq: 1}, {Seq: 2}, {Seq: 3}},
				processedSeqs: map[SequenceID]struct{}{{Seq: 1}: {}, {Seq: 2}: {}, {Seq: 3}: {}},
			},
			expectedSafeSeq:         &SequenceID{Seq: 3},
			expectedExpectedSeqsIdx: 2,
			expectedExpectedSeqs:    []SequenceID{},
			expectedProcessedSeqs:   map[SequenceID]struct{}{},
		},
		{
			name: "extra processed",
			c: &Checkpointer{
				expectedSeqs:  []SequenceID{{Seq: 1}, {Seq: 2}, {Seq: 3}},
				processedSeqs: map[SequenceID]struct{}{{Seq: 1}: {}, {Seq: 2}: {}, {Seq: 3}: {}, {Seq: 4}: {}, {Seq: 5}: {}},
			},
			expectedSafeSeq:         &SequenceID{Seq: 3},
			expectedExpectedSeqsIdx: 2,
			expectedExpectedSeqs:    []SequenceID{},
			expectedProcessedSeqs:   map[SequenceID]struct{}{{Seq: 4}: {}, {Seq: 5}: {}},
		},
		{
			name: "out of order expected seqs",
			c: &Checkpointer{
				expectedSeqs:  []SequenceID{{Seq: 3}, {Seq: 2}, {Seq: 1}},
				processedSeqs: map[SequenceID]struct{}{{Seq: 1}: {}, {Seq: 2}: {}, {Seq: 3}: {}},
			},
			expectedSafeSeq:         &SequenceID{Seq: 3},
			expectedExpectedSeqsIdx: 2,
			expectedExpectedSeqs:    []SequenceID{},
			expectedProcessedSeqs:   map[SequenceID]struct{}{},
		},
		{
			name: "compound sequence",
			c: &Checkpointer{
				expectedSeqs:  []SequenceID{{Seq: 1}, {Seq: 3, LowSeq: 1}},
				processedSeqs: map[SequenceID]struct{}{{Seq: 1}: {}, {Seq: 3, LowSeq: 1}: {}},
			},
			expectedSafeSeq:         &SequenceID{Seq: 3, LowSeq: 1},
			expectedExpectedSeqsIdx: 1,
			expectedExpectedSeqs:    []SequenceID{},
			expectedProcessedSeqs:   map[SequenceID]struct{}{},
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
		{expectedSeqsLen: 100, processedSeqsLen: 100},
		{expectedSeqsLen: 500, processedSeqsLen: 500},
		{expectedSeqsLen: 1000, processedSeqsLen: 1000},
		{expectedSeqsLen: 10000, processedSeqsLen: 10000},
		{expectedSeqsLen: 50000, processedSeqsLen: 50000},
		{expectedSeqsLen: 100000, processedSeqsLen: 100000},
	}
	for _, test := range tests {
		b.Run(fmt.Sprintf("expectedSeqsLen=%d,processedSeqsLen=%d", test.expectedSeqsLen, test.processedSeqsLen), func(b *testing.B) {
			expectedSeqs := make([]SequenceID, 0, test.expectedSeqsLen)
			for i := 0; i < test.expectedSeqsLen; i++ {
				expectedSeqs = append(expectedSeqs, SequenceID{Seq: uint64(i)})
			}
			processedSeqs := make(map[SequenceID]struct{}, test.processedSeqsLen)
			for i := 0; i < test.processedSeqsLen; i++ {
				processedSeqs[SequenceID{Seq: uint64(i)}] = struct{}{}
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
