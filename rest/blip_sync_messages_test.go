/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"testing"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddRevision(t *testing.T) {

	testCases := []struct {
		deletedPropertyValue string
		expectedDeletedVal   bool
	}{
		{
			deletedPropertyValue: "true",
			expectedDeletedVal:   true,
		},
		{
			deletedPropertyValue: "truedat",
			expectedDeletedVal:   true,
		},
		{
			deletedPropertyValue: "0",
			expectedDeletedVal:   false,
		},
		{
			deletedPropertyValue: "false",
			expectedDeletedVal:   false,
		},
		{
			deletedPropertyValue: "False",
			expectedDeletedVal:   true, // Counter-intuitive!  deleted() should probably lowercase the string before comparing
		},
		{
			deletedPropertyValue: "",
			expectedDeletedVal:   true, // It's not false, so it's true
		},
		{
			deletedPropertyValue: "nil",
			expectedDeletedVal:   false, // was not set, so it's false
		},
	}

	for _, testCase := range testCases {
		blipMessage := blip.Message{}
		blipMessage.Properties = blip.Properties{}
		if testCase.deletedPropertyValue != "nil" {
			blipMessage.Properties["deleted"] = testCase.deletedPropertyValue
		}
		revMessage := &db.RevMessage{
			Message: &blipMessage,
		}
		assert.Equal(t, testCase.expectedDeletedVal, revMessage.Deleted())
	}

}

// Make sure that the subChangesParams helper deals correctly with JSON strings.
// Reproduces SG #3283
func TestSubChangesSince(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	testDb := rt.GetDatabase()

	rq := blip.NewRequest()
	rq.Properties["since"] = `"1"`

	subChangesParams, err := db.NewSubChangesParams(base.TestCtx(t), rq, db.SequenceID{}, nil, testDb.ParseSequenceID)
	require.NoError(t, err)

	seqID := subChangesParams.Since()
	assert.Equal(t, uint64(1), seqID.Seq)

}

// Tests parsing the "future" mode of subChanges.
func TestSubChangesFuture(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	testDb := rt.GetDatabase()

	latestSeq := func() (db.SequenceID, error) {
		return db.SequenceID{Seq: 999}, nil
	}

	rq := blip.NewRequest()
	rq.Properties["future"] = "true"
	rq.Properties["since"] = `"1"`

	subChangesParams, err := db.NewSubChangesParams(base.TestCtx(t), rq, db.SequenceID{}, latestSeq, testDb.ParseSequenceID)
	require.NoError(t, err)

	seqID := subChangesParams.Since()
	assert.Equal(t, uint64(999), seqID.Seq)

}
