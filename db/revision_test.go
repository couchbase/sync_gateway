package db

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"

	goassert "github.com/couchbaselabs/go.assert"
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
			unmarshalErr := json.Unmarshal(test.inputBytes, &jsonUnmarshalBody)

			if unmarshalErr != nil {
				// If json.Unmarshal returns error for input, body.Unmarshal should do the same
				assert.True(t, err != nil, fmt.Sprintf("Expected error when unmarshalling %s", test.name))
			} else {
				assert.NoError(t, err, fmt.Sprintf("Expected no error when unmarshalling %s", test.name))
				goassert.DeepEquals(t, b, test.expectedBody) // Check against expected body
				goassert.DeepEquals(t, b, jsonUnmarshalBody) // Check against json.Unmarshal results
			}

		})
	}
}

func TestParseRevisionsToAncestor(t *testing.T) {
	revisions := Revisions{RevisionsStart: 5, RevisionsIds: []string{"five", "four", "three", "two", "one"}}

	assert.Equal(t, []string{"4-four", "3-three"}, revisions.parseAncestorRevisions("3-three"))
	assert.Equal(t, []string{"4-four"}, revisions.parseAncestorRevisions("4-four"))
	assert.Equal(t, []string{"4-four", "3-three", "2-two", "1-one"}, revisions.parseAncestorRevisions("1-one"))
	assert.Equal(t, []string{"4-four", "3-three", "2-two", "1-one"}, revisions.parseAncestorRevisions("5-five"))
	assert.Equal(t, []string{"4-four", "3-three", "2-two", "1-one"}, revisions.parseAncestorRevisions("0-zero"))
	assert.Equal(t, []string{"4-four", "3-three", "2-two", "1-one"}, revisions.parseAncestorRevisions("3-threeve"))

	shortRevisions := Revisions{RevisionsStart: 3, RevisionsIds: []string{"three"}}
	assert.Equal(t, []string(nil), shortRevisions.parseAncestorRevisions("2-two"))
}

func BenchmarkStripSpecialProperties(b *testing.B) {
	tests := []struct {
		name string
		body Body
	}{
		{
			"no special",
			Body{
				"asdf":  "qwerty",
				"one":   1,
				"two":   2,
				"three": 3,
				"four":  4,
				"five":  5,
				"six":   6,
				"seven": 7,
				"eight": 8,
				"nine":  9,
				"ten":   10,
				"a":     true,
				"b":     true,
				"c":     true,
			},
		},
		{
			"special",
			Body{
				"asdf":  "qwerty",
				"one":   1,
				"two":   2,
				"three": 3,
				"four":  4,
				"five":  5,
				"six":   6,
				"seven": 7,
				"eight": 8,
				"nine":  9,
				"ten":   10,
				"a":     true,
				"b":     true,
				"c":     true,
				BodyId:  "1234",
				BodyRev: "1-abc",
			},
		},
	}

	for _, t := range tests {
		b.Run(t.name, func(bb *testing.B) {
			bb.ReportAllocs()
			for i := 0; i < bb.N; i++ {
				stripSpecialProperties(t.body)
			}
		})
	}

}
