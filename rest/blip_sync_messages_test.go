package rest

import (
	"testing"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/go.assert"
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
		addRevision := newAddRevisionParams(&blipMessage)
		assert.Equals(t, addRevision.deleted(), testCase.expectedDeletedVal)
	}

}

// Make sure that the subChangesParams helper deals correctly with JSON strings.
// Reproduces SG #3283
func TestSubChangesSince(t *testing.T) {

	var rt RestTester
	defer rt.Close()

	testDb := rt.GetDatabase()

	rq := blip.NewRequest()
	rq.Properties["since"] = `"1"`

	zeroSinceVal := db.SequenceID{}

	subChangesParams := newSubChangesParams(
		rq,
		StdIoLogger{},
		zeroSinceVal,
		testDb.ParseSequenceID,
	)
	seqId, err := subChangesParams.since()
	assert.True(t, err == nil)
	assert.True(t, seqId.SeqType == db.IntSequenceType)
	assert.True(t, seqId.Seq == 1)

}
