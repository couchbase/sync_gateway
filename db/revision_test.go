/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"fmt"
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// TestBackupOldRevision ensures that old revisions are kept around temporarily for in-flight requests and delta sync purposes.
func TestBackupOldRevision(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()

	deltasEnabled := base.IsEnterpriseEdition()
	xattrsEnabled := base.TestUseXattrs()

	db := setupTestDBWithOptions(t, DatabaseContextOptions{DeltaSyncOptions: DeltaSyncOptions{
		Enabled:          deltasEnabled,
		RevMaxAgeSeconds: DefaultDeltaSyncRevMaxAge,
	}})
	defer db.Close()

	docID := t.Name()

	rev1ID, _, err := db.Put(docID, Body{"test": true})
	require.NoError(t, err)

	// make sure we didn't accidentally store an empty old revision
	_, err = db.getOldRevisionJSON(docID, "")
	assert.Error(t, err)
	assert.Equal(t, "404 missing", err.Error())

	// check for current rev backup in xattr+delta case (to support deltas by sdk imports)
	_, err = db.getOldRevisionJSON(docID, rev1ID)
	if deltasEnabled && xattrsEnabled {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
		assert.Equal(t, "404 missing", err.Error())
	}

	// create rev 2 and check backups for both revs
	rev2ID := "2-abc"
	_, _, err = db.PutExistingRevWithBody(docID, Body{"test": true, "updated": true}, []string{rev2ID, rev1ID}, true)
	require.NoError(t, err)

	// now in all cases we'll have rev 1 backed up (for at least 5 minutes)
	_, err = db.getOldRevisionJSON(docID, rev1ID)
	require.NoError(t, err)

	// check for current rev backup in xattr+delta case (to support deltas by sdk imports)
	_, err = db.getOldRevisionJSON(docID, rev2ID)
	if deltasEnabled && xattrsEnabled {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
		assert.Equal(t, "404 missing", err.Error())
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
		b.Run(t.name+"-stripSpecialProperties", func(bb *testing.B) {
			for i := 0; i < bb.N; i++ {
				stripSpecialProperties(t.body)
			}
		})
		b.Run(t.name+"-stripAllSpecialProperties", func(bb *testing.B) {
			for i := 0; i < bb.N; i++ {
				stripAllSpecialProperties(t.body)
			}
		})
		b.Run(t.name+"-containsUserSpecialProperties", func(bb *testing.B) {
			for i := 0; i < bb.N; i++ {
				containsUserSpecialProperties(t.body)
			}
		})
	}
}
