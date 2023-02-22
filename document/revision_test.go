/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package document

import (
	"fmt"
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

func TestParseRevID(t *testing.T) {

	var generation int
	var digest string

	generation, _ = ParseRevID("ljlkjl")
	log.Printf("generation: %v", generation)
	assert.True(t, generation == -1, "Expected -1 generation for invalid rev id")

	generation, digest = ParseRevID("1-ljlkjl")
	log.Printf("generation: %v, digest: %v", generation, digest)
	assert.True(t, generation == 1, "Expected 1 generation")
	assert.True(t, digest == "ljlkjl", "Unexpected digest")

	generation, digest = ParseRevID("2222-")
	log.Printf("generation: %v, digest: %v", generation, digest)
	assert.True(t, generation == 2222, "Expected invalid generation")
	assert.True(t, digest == "", "Unexpected digest")

	generation, digest = ParseRevID("333-a")
	log.Printf("generation: %v, digest: %v", generation, digest)
	assert.True(t, generation == 333, "Expected generation")
	assert.True(t, digest == "a", "Unexpected digest")

}

func TestBodyUnmarshal(t *testing.T) {

	tests := []struct {
		name         string
		inputBytes   []byte
		expectedBody Body
	}{
		{"empty bytes", []byte(""), nil},
		{"null", []byte("null"), Body(nil)},
		{"{}", []byte("{}"), Body{}},
		{"example body", []byte(`{"test":true}`), Body{"test": true}},
	}

	for _, test := range tests {
		t.Run(test.name, func(ts *testing.T) {
			var b Body
			err := b.Unmarshal(test.inputBytes)

			// Unmarshal using json.Unmarshal for comparison below
			var jsonUnmarshalBody Body
			unmarshalErr := base.JSONUnmarshal(test.inputBytes, &jsonUnmarshalBody)

			if unmarshalErr != nil {
				// If json.Unmarshal returns error for input, body.Unmarshal should do the same
				assert.True(t, err != nil, fmt.Sprintf("Expected error when unmarshalling %s", test.name))
			} else {
				assert.NoError(t, err, fmt.Sprintf("Expected no error when unmarshalling %s", test.name))
				assert.Equal(t, test.expectedBody, b) // Check against expected body
				assert.Equal(t, jsonUnmarshalBody, b) // Check against json.Unmarshal results
			}

		})
	}
}

func TestParseRevisionsToAncestor(t *testing.T) {
	revisions := Revisions{RevisionsStart: 5, RevisionsIds: []string{"five", "four", "three", "two", "one"}}

	assert.Equal(t, []string{"4-four", "3-three"}, revisions.ParseAncestorRevisions("3-three"))
	assert.Equal(t, []string{"4-four"}, revisions.ParseAncestorRevisions("4-four"))
	assert.Equal(t, []string{"4-four", "3-three", "2-two", "1-one"}, revisions.ParseAncestorRevisions("1-one"))
	assert.Equal(t, []string{"4-four", "3-three", "2-two", "1-one"}, revisions.ParseAncestorRevisions("5-five"))
	assert.Equal(t, []string{"4-four", "3-three", "2-two", "1-one"}, revisions.ParseAncestorRevisions("0-zero"))
	assert.Equal(t, []string{"4-four", "3-three", "2-two", "1-one"}, revisions.ParseAncestorRevisions("3-threeve"))

	shortRevisions := Revisions{RevisionsStart: 3, RevisionsIds: []string{"three"}}
	assert.Equal(t, []string(nil), shortRevisions.ParseAncestorRevisions("2-two"))
}

func TestStripSpecialProperties(t *testing.T) {
	testCases := []struct {
		name              string
		input             Body
		stripInternalOnly bool
		stripped          bool // Should at least 1 property have been stripped
		newBodyIfStripped *Body
	}{
		{
			name:              "No special",
			input:             Body{"test": 0, "bob": 0, "alice": 0},
			stripInternalOnly: false,
			stripped:          false,
			newBodyIfStripped: nil,
		},
		{
			name:              "No special internal only",
			input:             Body{"test": 0, "bob": 0, "alice": 0},
			stripInternalOnly: true,
			stripped:          false,
			newBodyIfStripped: nil,
		},
		{
			name:              "Strip special props",
			input:             Body{"_test": 0, "test": 0, "_attachments": 0, "_id": 0},
			stripInternalOnly: false,
			stripped:          true,
			newBodyIfStripped: &Body{"test": 0},
		},
		{
			name:              "Strip internal special props",
			input:             Body{"_test": 0, "test": 0, "_rev": 0, "_exp": 0},
			stripInternalOnly: true,
			stripped:          true,
			newBodyIfStripped: &Body{"_test": 0, "test": 0},
		},
		{
			name:              "Confirm internal attachments and deleted skipped",
			input:             Body{"_test": 0, "test": 0, "_attachments": 0, "_rev": 0, "_deleted": 0},
			stripInternalOnly: true,
			stripped:          true,
			newBodyIfStripped: &Body{"_test": 0, "test": 0, "_attachments": 0, "_deleted": 0},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			stripped, specialProps := stripSpecialProperties(test.input, test.stripInternalOnly)
			assert.Equal(t, test.stripped, specialProps)
			if test.stripped {
				assert.Equal(t, *test.newBodyIfStripped, stripped)
				return
			}
			assert.Equal(t, test.input, stripped)
		})
	}
}

func BenchmarkSpecialProperties(b *testing.B) {
	noSpecialBody := Body{
		"asdf": "qwerty", "a": true, "b": true, "c": true,
		"one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
		"six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
	}

	specialBody := noSpecialBody.Copy(BodyShallowCopy)
	specialBody[BodyId] = "abc123"
	specialBody[BodyRev] = "1-abc"

	tests := []struct {
		name string
		body Body
	}{
		{
			"no special",
			noSpecialBody,
		},
		{
			"special",
			specialBody,
		},
	}

	for _, t := range tests {
		b.Run(t.name+"-stripInternalProperties", func(bb *testing.B) {
			for i := 0; i < bb.N; i++ {
				StripInternalProperties(t.body)
			}
		})
		b.Run(t.name+"-stripAllInternalProperties", func(bb *testing.B) {
			for i := 0; i < bb.N; i++ {
				StripAllSpecialProperties(t.body)
			}
		})
	}
}
