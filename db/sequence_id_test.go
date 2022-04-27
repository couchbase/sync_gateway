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
	"testing"

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
}

func TestMarshalSequenceID(t *testing.T) {
	s := SequenceID{Seq: 1234}
	assert.Equal(t, "1234", s.String())
	asJson, err := base.JSONMarshal(s)
	assert.NoError(t, err, "Marshal failed")
	assert.Equal(t, "1234", string(asJson))

	var s2 SequenceID
	err = base.JSONUnmarshal(asJson, &s2)
	assert.NoError(t, err, "Unmarshal failed")
	assert.Equal(t, s, s2)
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
}

func TestMarshalTriggeredSequenceID(t *testing.T) {
	s := SequenceID{TriggeredBy: 5678, Seq: 1234}
	assert.Equal(t, "5678:1234", s.String())
	asJson, err := base.JSONMarshal(s)
	assert.NoError(t, err, "Marshal failed")
	assert.Equal(t, "\"5678:1234\"", string(asJson))

	var s2 SequenceID
	err = base.JSONUnmarshal(asJson, &s2)
	assert.NoError(t, err, "Unmarshal failed")
	assert.Equal(t, s, s2)
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
