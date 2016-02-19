package db

import (
	"encoding/json"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func TestParseSequenceID(t *testing.T) {
	s, err := parseIntegerSequenceID("1234")
	assertNoError(t, err, "parseIntegerSequenceID")
	assert.Equals(t, s, SequenceID{Seq: 1234, SeqType: 1})

	s, err = parseIntegerSequenceID("5678:1234")
	assertNoError(t, err, "parseIntegerSequenceID")
	assert.Equals(t, s, SequenceID{Seq: 1234, TriggeredBy: 5678, SeqType: 1})

	s, err = parseIntegerSequenceID("")
	assertNoError(t, err, "parseIntegerSequenceID")
	assert.Equals(t, s, SequenceID{Seq: 0, TriggeredBy: 0})

	s, err = parseIntegerSequenceID("123:456:789")
	assertNoError(t, err, "parseIntegerSequenceID")
	assert.Equals(t, s, SequenceID{Seq: 789, TriggeredBy: 456, LowSeq: 123, SeqType: 1})

	s, err = parseIntegerSequenceID("123::789")
	assertNoError(t, err, "parseIntegerSequenceID")
	assert.Equals(t, s, SequenceID{Seq: 789, TriggeredBy: 0, LowSeq: 123, SeqType: 1})

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
}

func TestMarshalSequenceID(t *testing.T) {
	s := SequenceID{Seq: 1234, SeqType: IntSequenceType}
	assert.Equals(t, s.String(), "1234")
	asJson, err := json.Marshal(s)
	assertNoError(t, err, "Marshal failed")
	assert.Equals(t, string(asJson), "1234")

	var s2 SequenceID
	err = json.Unmarshal(asJson, &s2)
	assertNoError(t, err, "Unmarshal failed")
	assert.Equals(t, s2, s)
}

func TestSequenceIDUnmarshalJSON(t *testing.T) {

	str := "123"
	s := SequenceID{}
	err := s.UnmarshalJSON([]byte(str))
	assertNoError(t, err, "UnmarshalJSON failed")
	assert.Equals(t, s, SequenceID{Seq: 123, SeqType: IntSequenceType})

	str = "456:123"
	s = SequenceID{}
	err = s.UnmarshalJSON([]byte(str))
	assertNoError(t, err, "UnmarshalJSON failed")
	assert.Equals(t, s, SequenceID{TriggeredBy: 456, Seq: 123, SeqType: IntSequenceType})

	str = "220::222"
	s = SequenceID{}
	err = s.UnmarshalJSON([]byte(str))
	assertNoError(t, err, "UnmarshalJSON failed")
	assert.Equals(t, s, SequenceID{LowSeq: 220, TriggeredBy: 0, Seq: 222, SeqType: IntSequenceType})

	str = "\"234\""
	s = SequenceID{}
	err = s.UnmarshalJSON([]byte(str))
	assertNoError(t, err, "UnmarshalJSON failed")
	assert.Equals(t, s, SequenceID{Seq: 234, SeqType: IntSequenceType})

	str = "\"567:234\""
	s = SequenceID{}
	err = s.UnmarshalJSON([]byte(str))
	assertNoError(t, err, "UnmarshalJSON failed")
	assert.Equals(t, s, SequenceID{TriggeredBy: 567, Seq: 234, SeqType: IntSequenceType})

	str = "\"220::222\""
	s = SequenceID{}
	err = s.UnmarshalJSON([]byte(str))
	assertNoError(t, err, "UnmarshalJSON failed")
	assert.Equals(t, s, SequenceID{LowSeq: 220, TriggeredBy: 0, Seq: 222, SeqType: IntSequenceType})
}

func TestMarshalTriggeredSequenceID(t *testing.T) {
	s := SequenceID{TriggeredBy: 5678, Seq: 1234, SeqType: 1}
	assert.Equals(t, s.String(), "5678:1234")
	asJson, err := json.Marshal(s)
	assertNoError(t, err, "Marshal failed")
	assert.Equals(t, string(asJson), "\"5678:1234\"")

	var s2 SequenceID
	err = json.Unmarshal(asJson, &s2)
	assertNoError(t, err, "Unmarshal failed")
	assert.Equals(t, s2, s)
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
			assert.Equals(t, orderedSeqs[i].Before(orderedSeqs[j]), i < j)
		}
	}
}
