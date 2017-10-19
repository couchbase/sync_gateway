package db

import (
	"log"
	"testing"
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
