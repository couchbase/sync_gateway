/*
Copyright 2016-Present Couchbase, Inc.

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

	"github.com/stretchr/testify/require"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

func TestParseSequenceID(t *testing.T) {
	s, err := parseIntegerSequenceID("1234")
	assert.NoError(t, err, "parseIntegerSequenceID")
	assert.Equal(t, SequenceID{Seq: 1234}, s)

	s, err = parseIntegerSequenceID("5678:1234")
	assert.NoError(t, err, "parseIntegerSequenceID")
	assert.Equal(t, SequenceID{Seq: 1234, TriggeredBy: 5678}, s)

	s, err = parseIntegerSequenceID("")
	assert.NoError(t, err, "parseIntegerSequenceID")
	assert.Equal(t, SequenceID{Seq: 0, TriggeredBy: 0}, s)

	s, err = parseIntegerSequenceID("123:456:789")
	assert.NoError(t, err, "parseIntegerSequenceID")
	assert.Equal(t, SequenceID{Seq: 789, TriggeredBy: 456, LowSeq: 123}, s)

	s, err = parseIntegerSequenceID("123::789")
	assert.NoError(t, err, "parseIntegerSequenceID")
	assert.Equal(t, SequenceID{Seq: 789, TriggeredBy: 0, LowSeq: 123}, s)

	s, err = parseIntegerSequenceID("foo")
	assert.True(t, err != nil)
	s, err = parseIntegerSequenceID(":")
	assert.True(t, err != nil)
	s, err = parseIntegerSequenceID(":1")
	assert.True(t, err != nil)
	s, err = parseIntegerSequenceID("::1")
	assert.True(t, err != nil)
	s, err = parseIntegerSequenceID("10:11:12:13")
	assert.True(t, err != nil)
	s, err = parseIntegerSequenceID("123:ggg")
	assert.True(t, err != nil)

	// Parse sequences with metadata components
	s, err = parseIntegerSequenceID("10-1234")
	assert.NoError(t, err, "parseIntegerSequenceID")
	assert.Equal(t, SequenceID{MetaSeq: 10, Seq: 1234}, s)

	s, err = parseIntegerSequenceID("10:456-1234")
	assert.NoError(t, err, "parseIntegerSequenceID")
	assert.Equal(t, SequenceID{MetaSeq: 10, BackfillSeq: 456, Seq: 1234}, s)

	s, err = parseIntegerSequenceID("10:456-1234::5678")
	assert.NoError(t, err, "parseIntegerSequenceID")
	assert.Equal(t, SequenceID{MetaSeq: 10, BackfillSeq: 456, LowSeq: 1234, Seq: 5678}, s)

	// Malformed sequences with metadata components
	s, err = parseIntegerSequenceID("10-")
	assert.True(t, err != nil)
	s, err = parseIntegerSequenceID("10:13:234-15")
	assert.True(t, err != nil)
	s, err = parseIntegerSequenceID("-234")
	assert.True(t, err != nil)

}

func BenchmarkParseSequenceID(b *testing.B) {
	tests := []string{
		"1234",
		"5678:1234",
		"",
		"123:456:789",
		"123::789",
		"foo",
		":",
		":1",
		"::1",
		"10:11:12:13",
		"123:ggg",
		"10:456-1234::5678",
	}

	for _, test := range tests {
		b.Run(test, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = parseIntegerSequenceID(test)
			}
		})
	}
}

func TestMarshalSequenceIDAndString(t *testing.T) {
	type testCase struct {
		seq                SequenceID
		expectedString     string
		expectedJSONString string
	}
	tests := []testCase{
		{SequenceID{Seq: 1234}, "1234", "1234"},
		{SequenceID{Seq: 1234, TriggeredBy: 5678}, "5678:1234", `"5678:1234"`},
		{SequenceID{Seq: 789, TriggeredBy: 456, LowSeq: 123}, "123:456:789", `"123:456:789"`},
		{SequenceID{Seq: 789, LowSeq: 123}, "123::789", `"123::789"`},
		{SequenceID{MetaSeq: 10, Seq: 789}, "10-789", `"10-789"`},
		{SequenceID{MetaSeq: 10, Seq: 789, LowSeq: 123}, "10-123::789", `"10-123::789"`},
		{SequenceID{MetaSeq: 10, BackfillSeq: 234, Seq: 789, LowSeq: 123}, "10:234-123::789", `"10:234-123::789"`},
		{SequenceID{MetaSeq: 10, BackfillSeq: 234, Seq: 789}, "10:234-789", `"10:234-789"`},
	}

	for _, testCase := range tests {
		s := testCase.seq
		assert.Equal(t, testCase.expectedString, s.String())
		asJson, err := base.JSONMarshal(s)
		assert.NoError(t, err, "Marshal failed")
		assert.Equal(t, testCase.expectedJSONString, string(asJson))

		var s2 SequenceID
		err = base.JSONUnmarshal(asJson, &s2)
		assert.NoError(t, err, "Unmarshal failed")
		assert.Equal(t, s, s2)
	}

}

func TestSequenceIDUnmarshalJSON(t *testing.T) {

	str := "123"
	s := SequenceID{}
	err := s.UnmarshalJSON([]byte(str))
	assert.NoError(t, err, "UnmarshalJSON failed")
	assert.Equal(t, SequenceID{Seq: 123}, s)

	str = "456:123"
	s = SequenceID{}
	err = s.UnmarshalJSON([]byte(str))
	assert.NoError(t, err, "UnmarshalJSON failed")
	assert.Equal(t, SequenceID{TriggeredBy: 456, Seq: 123}, s)

	str = "220::222"
	s = SequenceID{}
	err = s.UnmarshalJSON([]byte(str))
	assert.NoError(t, err, "UnmarshalJSON failed")
	assert.Equal(t, SequenceID{LowSeq: 220, TriggeredBy: 0, Seq: 222}, s)

	str = "10:100-220::222"
	s = SequenceID{}
	err = s.UnmarshalJSON([]byte(str))
	assert.NoError(t, err, "UnmarshalJSON failed")
	assert.Equal(t, SequenceID{MetaSeq: 10, BackfillSeq: 100, LowSeq: 220, TriggeredBy: 0, Seq: 222}, s)

	str = "\"234\""
	s = SequenceID{}
	err = s.UnmarshalJSON([]byte(str))
	assert.NoError(t, err, "UnmarshalJSON failed")
	assert.Equal(t, SequenceID{Seq: 234}, s)

	str = "\"567:234\""
	s = SequenceID{}
	err = s.UnmarshalJSON([]byte(str))
	assert.NoError(t, err, "UnmarshalJSON failed")
	assert.Equal(t, SequenceID{TriggeredBy: 567, Seq: 234}, s)

	str = "\"220::222\""
	s = SequenceID{}
	err = s.UnmarshalJSON([]byte(str))
	assert.NoError(t, err, "UnmarshalJSON failed")
	assert.Equal(t, SequenceID{LowSeq: 220, TriggeredBy: 0, Seq: 222}, s)

	str = "\"10:100-220::222\""
	s = SequenceID{}
	err = s.UnmarshalJSON([]byte(str))
	assert.NoError(t, err, "UnmarshalJSON failed")
	assert.Equal(t, SequenceID{MetaSeq: 10, BackfillSeq: 100, LowSeq: 220, TriggeredBy: 0, Seq: 222}, s)
}

func TestCompareSequenceIDs(t *testing.T) {
	orderedSeqs := []SequenceID{
		{Seq: 1234},
		{Seq: 5677},
		{TriggeredBy: 5678, Seq: 1234},
		{TriggeredBy: 5678, Seq: 2222},
		{Seq: 5678}, // 5678 comes after the sequences it triggered
		{TriggeredBy: 6666, Seq: 5678},
		{Seq: 6666},
	}

	for i := 0; i < len(orderedSeqs); i++ {
		for j := 0; j < len(orderedSeqs); j++ {
			assert.Equal(t, i < j, orderedSeqs[i].Before(orderedSeqs[j]))
		}
	}
}

func TestCompareSequenceIDsLowSeq(t *testing.T) {
	orderedSeqs := []SequenceID{
		{LowSeq: 1200, Seq: 1233},
		{LowSeq: 1205, Seq: 1234},
		{Seq: 1234},
		{LowSeq: 1234, Seq: 5677},
		{LowSeq: 1234, Seq: 5678},
	}

	for i := 0; i < len(orderedSeqs); i++ {
		for j := 0; j < len(orderedSeqs); j++ {
			t.Run(fmt.Sprintf("%v<%v==%v", orderedSeqs[i], orderedSeqs[j], i < j), func(t *testing.T) {
				assert.Equalf(t, i < j, orderedSeqs[i].Before(orderedSeqs[j]), "expected %v < %v", orderedSeqs[i], orderedSeqs[j])
			})
		}
	}
}

func TestCompareSequenceIDsMetaSeq(t *testing.T) {

	orderedSeqs := []string{
		"10-123",
		"10-124:10",
		"10-124",
		"10-124::130",
		"10-124::135",
		"10-125::130",
		"10-126",
		"11-126",
		"11-127",
		"12:100-127",
		"12:101-127",
		"12-127",
		"13-100",
	}

	for i := 0; i < len(orderedSeqs); i++ {
		for j := 0; j < len(orderedSeqs); j++ {
			seq_i := SequenceID{}
			err := seq_i.UnmarshalJSON([]byte(orderedSeqs[i]))
			require.NoError(t, err)
			seq_j := SequenceID{}
			err = seq_j.UnmarshalJSON([]byte(orderedSeqs[j]))
			require.NoError(t, err)
			t.Run(fmt.Sprintf("%v<%v==%v", seq_i, seq_j, i < j), func(t *testing.T) {
				assert.Equalf(t, i < j, seq_i.Before(seq_j), "expected %v < %v", orderedSeqs[i], orderedSeqs[j])
			})
		}
	}
}
