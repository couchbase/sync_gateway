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
			// ensure we maintain enough sequences that we can checkpoint expected but not yet processed without retaining the full list of processed sequences
			// in most cases this will be keeping the last processed sequence and removing all prior ones, until the missing sequence in the list.
			// e.g.
			//    expected:  [2 4 6 8 9]
			//    processed: [  4 6 8  ]
			// can be safely compacted to:
			//    expected:  [2 8 9]
			//    processed: [  8  ]
			name: "processed compaction non-sequential (out of order)",
			c: &Checkpointer{
				expectedSeqs:                   genExpectedForTest(t, "2", "1", "6", "8", "4", "9"),
				processedSeqs:                  genProcessedForTest(t, "4", "1", "6", "8"),
				expectedSeqCompactionThreshold: 3, // this many expected seqs to trigger compaction
			},
			expectedSafeSeq:         &SequenceID{Seq: 1},
			expectedExpectedSeqsIdx: 0,
			expectedExpectedSeqs:    genExpectedForTest(t, "2", "8", "9"),
			expectedProcessedSeqs:   genProcessedForTest(t, "8"),
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
		{
			name: "compaction when safeSeq unchanged",
			c: &Checkpointer{
				expectedSeqs: genExpectedForTest(t, "47358299", "47358300", "47358301", "47358302", "47358303", "47358304", "47358305", "47358306", "47358307", "47358308",
					"47358309", "47358310", "47358311", "47358312", "47358313", "47358314", "47358315", "47358316", "47358317", "47358318", "47358319", "47358320", "47358321", "47358322",
					"47358323", "47358324", "47358325", "47358326", "47358327", "47358328", "47358329", "47358330", "47358331", "47358332", "47358333", "47358334", "47358335", "47358336",
					"47358337", "47358338", "47358339", "47358340", "47358341", "47358342", "47358343", "47358344", "47358345", "47358346", "47358347", "47358348", "47358349", "47358350",
					"47358351", "47358352", "47358353", "47358354", "47358355", "47358356", "47358357", "47358358", "47358359", "47358360", "47358361", "47358362", "47358363", "47358364",
					"47358365", "47358366", "47358367", "47358368", "47358369", "47358370", "47358371", "47358372", "47358373", "47358374", "47358375", "47358376", "47358377", "47358378",
					"47358379", "47358380", "47358381", "47358382", "47358383", "47358384", "47358385", "47358386", "47358387", "47358388", "47358389", "47358390", "47358391", "47358392",
					"47358393", "47358394", "47358395", "47358396", "47358397", "47358398", "47358399", "47358400", "47358401", "47358402", "47358403", "47358404", "47358405", "47358406",
					"47358407", "47358408", "47358409", "47358410", "47358411", "47358412", "47358413", "47358414", "47358415", "47358416", "47358417", "47358418", "47358419", "47358420",
					"47358421", "47358422", "47358423", "47358424", "47358425", "47358426", "47358427", "47358428", "47358429", "47358430", "47358431", "47358432", "47358433", "47358434",
					"47358435", "47358436", "47358437", "47358438", "47358439", "47358440", "47358441", "47358442", "47358443", "47358444", "47358445", "47358446", "47358447", "47358448",
					"47358449", "47358450", "47358451"),
				processedSeqs: genProcessedForTest(t, "47358364", "47358365", "47358366", "47358367", "47358368", "47358369", "47358370", "47358371", "47358372", "47358373",
					"47358374", "47358375", "47358376", "47358377", "47358378", "47358379", "47358380", "47358381", "47358382", "47358383", "47358384", "47358385", "47358386", "47358387",
					"47358388", "47358389", "47358390", "47358391", "47358392", "47358393", "47358394", "47358395", "47358396", "47358397", "47358398", "47358399", "47358400", "47358401",
					"47358402", "47358403", "47358404", "47358405", "47358406", "47358407", "47358409", "47358410", "47358411", "47358412", "47358413", "47358414", "47358415", "47358416",
					"47358417", "47358418", "47358419", "47358420", "47358422", "47358423", "47358424", "47358425", "47358426", "47358427", "47358428", "47358429", "47358430", "47358431",
					"47358432", "47358433", "47358434", "47358435", "47358436", "47358437", "47358438", "47358439", "47358440", "47358442", "47358443", "47358444", "47358448", "47358449",
					"47358450", "47358451", "47358452", "47358453", "47358454", "47358455", "47358456", "47358457", "47358458", "47358459", "47358460", "47358461", "47358462", "47358463",
					"47358464", "47358465", "47358466", "47358467", "47358468", "47358469", "47358470", "47358471", "47358472", "47358473", "47358474", "47358475", "47358477", "47358478",
					"47358479", "47358481", "47358482", "47358484", "47358486", "47358487", "47358489", "47358490", "47358491", "47358492", "47358493", "47358495", "47358496", "47358497",
					"47358498", "47358499", "47358500", "47358501", "47358502", "47358503", "47358504", "47358505", "47358507", "47358508", "47358509", "47358510", "47358511", "47358512",
					"47358513", "47358514", "47358515", "47358516", "47358517", "47358518", "47358519", "47358520", "47358521", "47358522", "47358523", "47358524", "47358525", "47358526",
					"47358527", "47358528", "47358529", "47358530", "47358531", "47358532"),
				expectedSeqCompactionThreshold: 5, // this many expected seqs to trigger compaction
			},
			expectedSafeSeq:         nil,
			expectedExpectedSeqsIdx: -1,
			expectedExpectedSeqs: genExpectedForTest(t, "47358299", "47358300", "47358301", "47358302", "47358303", "47358304", "47358305", "47358306", "47358307", "47358308",
				"47358309", "47358310", "47358311", "47358312", "47358313", "47358314", "47358315", "47358316", "47358317", "47358318", "47358319", "47358320", "47358321", "47358322",
				"47358323", "47358324", "47358325", "47358326", "47358327", "47358328", "47358329", "47358330", "47358331", "47358332", "47358333", "47358334", "47358335", "47358336",
				"47358337", "47358338", "47358339", "47358340", "47358341", "47358342", "47358343", "47358344", "47358345", "47358346", "47358347", "47358348", "47358349", "47358350",
				"47358351", "47358352", "47358353", "47358354", "47358355", "47358356", "47358357", "47358358", "47358359", "47358360", "47358361", "47358362", "47358363", "47358407",
				"47358408", "47358420", "47358421", "47358440", "47358441", "47358444", "47358445", "47358446", "47358447", "47358451"),
			expectedProcessedSeqs: genProcessedForTest(t, "47358407", "47358420", "47358440", "47358444", "47358451", "47358452", "47358453", "47358454", "47358455", "47358456",
				"47358457", "47358458", "47358459", "47358460", "47358461", "47358462", "47358463", "47358464", "47358465", "47358466", "47358467", "47358468", "47358469", "47358470",
				"47358471", "47358472", "47358473", "47358474", "47358475", "47358477", "47358478", "47358479", "47358481", "47358482", "47358484", "47358486", "47358487", "47358489",
				"47358490", "47358491", "47358492", "47358493", "47358495", "47358496", "47358497", "47358498", "47358499", "47358500", "47358501", "47358502", "47358503", "47358504",
				"47358505", "47358507", "47358508", "47358509", "47358510", "47358511", "47358512", "47358513", "47358514", "47358515", "47358516", "47358517", "47358518", "47358519",
				"47358520", "47358521", "47358522", "47358523", "47358524", "47358525", "47358526", "47358527", "47358528", "47358529", "47358530", "47358531", "47358532"),
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
