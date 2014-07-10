package db

import (
	"encoding/json"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func TestParseSequenceID(t *testing.T) {
	s, err := ParseSequenceID("1234")
	assertNoError(t, err, "ParseSequenceID")
	assert.Equals(t, s, SequenceID{Seq: 1234})

	s, err = ParseSequenceID("5678:1234")
	assertNoError(t, err, "ParseSequenceID")
	assert.Equals(t, s, SequenceID{Seq: 1234, TriggeredBy: 5678})

	s, err = ParseSequenceID("")
	assertNoError(t, err, "ParseSequenceID")
	assert.Equals(t, s, SequenceID{Seq: 0, TriggeredBy: 0})

	s, err = ParseSequenceID("foo")
	assert.True(t, err != nil)
	s, err = ParseSequenceID(":")
	assert.True(t, err != nil)
	s, err = ParseSequenceID("123:456:789")
	assert.True(t, err != nil)
	s, err = ParseSequenceID(":1")
	assert.True(t, err != nil)
	s, err = ParseSequenceID("123:ggg")
	assert.True(t, err != nil)
}

func TestMarshalSequenceID(t *testing.T) {
	s := SequenceID{Seq: 1234}
	assert.Equals(t, s.String(), "1234")
	asJson, err := json.Marshal(s)
	assertNoError(t, err, "Marshal failed")
	assert.Equals(t, string(asJson), "1234")

	var s2 SequenceID
	err = json.Unmarshal(asJson, &s2)
	assertNoError(t, err, "Unmarshal failed")
	assert.Equals(t, s2, s)
}

func TestMarshalTriggeredSequenceID(t *testing.T) {
	s := SequenceID{TriggeredBy: 5678, Seq: 1234}
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
		SequenceID{Seq: 1234},
		SequenceID{Seq: 5677},
		SequenceID{TriggeredBy: 5678, Seq: 1234},
		SequenceID{TriggeredBy: 5678, Seq: 2222},
		SequenceID{Seq: 5678}, // 5678 comes after the sequences it triggered
		SequenceID{TriggeredBy: 6666, Seq: 5678},
		SequenceID{Seq: 6666},
	}

	for i := 0; i < len(orderedSeqs); i++ {
		for j := 0; j < len(orderedSeqs); j++ {
			assert.Equals(t, orderedSeqs[i].Before(orderedSeqs[j]), i < j)
		}
	}
}
