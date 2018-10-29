package db

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"

	goassert "github.com/couchbaselabs/go.assert"
)

func TestParseRevID(t *testing.T) {

	var generation int
	var digest string

	generation, _ = ParseRevID("ljlkjl")
	log.Printf("generation: %v", generation)
	assertTrue(t, generation == -1, "Expected -1 generation for invalid rev id")

	generation, digest = ParseRevID("1-ljlkjl")
	log.Printf("generation: %v, digest: %v", generation, digest)
	assertTrue(t, generation == 1, "Expected 1 generation")
	assertTrue(t, digest == "ljlkjl", "Unexpected digest")

	generation, digest = ParseRevID("2222-")
	log.Printf("generation: %v, digest: %v", generation, digest)
	assertTrue(t, generation == 2222, "Expected invalid generation")
	assertTrue(t, digest == "", "Unexpected digest")

	generation, digest = ParseRevID("333-a")
	log.Printf("generation: %v, digest: %v", generation, digest)
	assertTrue(t, generation == 333, "Expected generation")
	assertTrue(t, digest == "a", "Unexpected digest")

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
				assertTrue(t, err != nil, fmt.Sprintf("Expected error when unmarshalling %s", test.name))
			} else {
				assertNoError(t, err, fmt.Sprintf("Expected no error when unmarshalling %s", test.name))
				goassert.DeepEquals(t, b, test.expectedBody) // Check against expected body
				goassert.DeepEquals(t, b, jsonUnmarshalBody) // Check against json.Unmarshal results
			}

		})
	}
}
