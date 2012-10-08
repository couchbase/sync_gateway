// revtree_test.go

package basecouch

import (
	"encoding/json"
	"fmt"
	"github.com/sdegutis/go.assert"
	"sort"
	"testing"
)

var testmap = RevTree{"3-three": {ID: "3-three", Parent: "2-two"},
					"2-two": {ID: "2-two", Parent: "1-one"},
					"1-one": {ID: "1-one"}}
var branchymap = RevTree{"3-three": {ID: "3-three", Parent: "2-two"},
	"2-two":  {ID: "2-two", Parent: "1-one"},
	"1-one":  {ID: "1-one"},
	"3-drei": {ID: "3-drei", Parent: "2-two"}}

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

	tempmap.addRevision(RevInfo{ID: "4-four", Parent: "3-three"})
	assert.Equals(t, tempmap.getParent("4-four"), "3-three")
}

func TestCompareRevIDs(t *testing.T) {
	assert.Equals(t, compareRevIDs("1-aaa", "1-aaa"), 0)
	assert.Equals(t, compareRevIDs("1-aaa", "5-aaa"), -1)
	assert.Equals(t, compareRevIDs("10-aaa", "5-aaa"), 1)
	assert.Equals(t, compareRevIDs("1-bbb", "1-aaa"), 1)
	assert.Equals(t, compareRevIDs("5-bbb", "1-zzz"), 1)
}

func TestIsLeaf(t *testing.T) {
	assertTrue(t, branchymap.isLeaf("3-three"), "isLeaf failed on 3-three")
	assertTrue(t, branchymap.isLeaf("3-drei"), "isLeaf failed on 3-drei")
	assertFalse(t, branchymap.isLeaf("2-two"), "isLeaf failed on 2-two")
	assertFalse(t, branchymap.isLeaf("bogus"), "isLeaf failed on 'bogus")
	assertFalse(t, branchymap.isLeaf(""), "isLeaf failed on ''")
}

func TestWinningRev(t *testing.T) {
	tempmap := branchymap.copy()
	assert.Equals(t, tempmap.winningRevision(), "3-three")
	tempmap.addRevision(RevInfo{ID: "4-four", Parent: "3-three"})
	assert.Equals(t, tempmap.winningRevision(), "4-four")
	tempmap.addRevision(RevInfo{ID: "5-five", Parent: "4-four", Deleted: true})
	assert.Equals(t, tempmap.winningRevision(), "3-drei")
}

//////// HELPERS:

func assertNoError(t *testing.T, err error, message string) {
	if err != nil {
		t.Fatalf("%s: %v", message, err)
	}
}

func assertTrue(t *testing.T, success bool, message string) {
	if !success {
		t.Fatalf("%s", message)
	}
}

func assertFalse(t *testing.T, failure bool, message string) {
	if failure {
		t.Fatalf("%s", message)
	}
}
