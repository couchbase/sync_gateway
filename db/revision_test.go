package db

import (
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
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

func TestMutableAttachmentsCopy(t *testing.T) {
	obj := "obj"

	b := Body{
		"ABC": "abc",
		"def": 1234,
		"xYz": map[string]string{"objData": "abc"},
		"obj": &obj,
	}

	// Take a copy and make sure they're the same
	bCopy := b.MutableAttachmentsCopy()
	assert.DeepEquals(t, b, bCopy)

	// Mutate the original and check the copy is intact
	b["ABC"] = "xyz"
	assert.Equals(t, b["ABC"], "xyz")
	assert.Equals(t, bCopy["ABC"], "abc")

	bCopy["obj"] = base.StringPointer("modifiedObj")
	assert.Equals(t, *b["obj"].(*string), "obj")
	assert.Equals(t, *bCopy["obj"].(*string), "modifiedObj")
}
