// revtree_test.go

package basecouch

import (
	"encoding/json"
	"fmt"
	"github.com/sdegutis/go.assert"
	"sort"
	"testing"
)

var testmap = RevTree{"3-three": "2-two", "2-two": "1-one", "1-one": ""}
var branchymap = RevTree{"3-three": "2-two", "2-two": "1-one", "1-one": "", "3-drei": "2-two"}

const testJSON = `{"revs": ["3-three", "2-two", "1-one"], "parents": [1, 2, -1]}`

func testUnmarshal(t *testing.T, jsonString string) RevTree {
	gotmap := RevTree{}
	assertNoError(t, json.Unmarshal([]byte(jsonString), &gotmap), "Couldn't parse RevTree from JSON")
	assert.DeepEquals(t, gotmap, testmap)
	return gotmap
}

func TestUnmarshal(t *testing.T) {
	gotmap := testUnmarshal(t, testJSON)
	fmt.Printf("Unmarshaled to %v\n", gotmap)
}

func TestMarshal(t *testing.T) {
	bytes, err := json.Marshal(testmap)
	assertNoError(t, err, "Couldn't write RevTree to JSON")
	fmt.Printf("Marshaled RevTree as %s\n", string(bytes))
	testUnmarshal(t, string(bytes))
}

func TestRevTreeAccess(t *testing.T) {
	assertTrue(t, testmap.contains("3-three"), "contains 3 failed")
	assertTrue(t, testmap.contains("1-one"), "contains 1 failed")
	assertFalse(t, testmap.contains("foo"), "contains false positive")
}

func TestParentAccess(t *testing.T) {
	parent := testmap.getParent("3-three")
	assert.Equals(t, parent, "2-two")
	parent = testmap.getParent("1-one")
	assert.Equals(t, parent, "")
}

func TestGetHistory(t *testing.T) {
	history := testmap.getHistory("3-three")
	assert.DeepEquals(t, history, []string{"3-three", "2-two", "1-one"})
}

func TestGetLeaves(t *testing.T) {
	leaves := testmap.getLeaves()
	assert.DeepEquals(t, leaves, []string{"3-three"})
	leaves = branchymap.getLeaves()
	sort.Strings(leaves)
	assert.DeepEquals(t, leaves, []string{"3-drei", "3-three"})
}

func TestAddRevision(t *testing.T) {
	tempmap := testmap.copy()
	assert.DeepEquals(t, tempmap, testmap)

	tempmap.addRevision("4-four", "3-three")
	assert.Equals(t, tempmap.getParent("4-four"), "3-three")
}

func assertNoError(t *testing.T, err error, message string) {
	if err != nil {
		t.Errorf("%s: %v", message, err)
	}
}

func assertTrue(t *testing.T, success bool, message string) {
	if !success {
		t.Errorf("%s", message)
	}
}

func assertFalse(t *testing.T, failure bool, message string) {
	if failure {
		t.Errorf("%s", message)
	}
}
