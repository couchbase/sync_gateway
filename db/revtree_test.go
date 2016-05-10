//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
)

var testmap = RevTree{"3-three": {ID: "3-three", Parent: "2-two", Body: []byte("{}")},
	"2-two": {ID: "2-two", Parent: "1-one", Channels: base.SetOf("ABC", "CBS")},
	"1-one": {ID: "1-one", Channels: base.SetOf("ABC")}}

// 1-one -- 2-two -- 3-three

var branchymap = RevTree{"3-three": {ID: "3-three", Parent: "2-two"},
	"2-two":  {ID: "2-two", Parent: "1-one"},
	"1-one":  {ID: "1-one"},
	"3-drei": {ID: "3-drei", Parent: "2-two"}}

//               / 3-three
// 1-one -- 2-two
//               \ 3-drei

func testUnmarshal(t *testing.T, jsonString string) RevTree {
	gotmap := RevTree{}
	assertNoError(t, json.Unmarshal([]byte(jsonString), &gotmap), "Couldn't parse RevTree from JSON")
	assert.DeepEquals(t, gotmap, testmap)
	return gotmap
}

func TestRevTreeUnmarshalOldFormat(t *testing.T) {
	const testJSON = `{"revs": ["3-three", "2-two", "1-one"], "parents": [1, 2, -1], "bodies": ["{}", "", ""], "channels": [null, ["ABC", "CBS"], ["ABC"]]}`
	gotmap := testUnmarshal(t, testJSON)
	fmt.Printf("Unmarshaled to %v\n", gotmap)
}

func TestRevTreeUnmarshal(t *testing.T) {
	const testJSON = `{"revs": ["3-three", "2-two", "1-one"], "parents": [1, 2, -1], "bodymap": {"0":"{}"}, "channels": [null, ["ABC", "CBS"], ["ABC"]]}`
	gotmap := testUnmarshal(t, testJSON)
	fmt.Printf("Unmarshaled to %v\n", gotmap)
}

func TestRevTreeMarshal(t *testing.T) {
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

func TestRevTreeParentAccess(t *testing.T) {
	parent := testmap.getParent("3-three")
	assert.Equals(t, parent, "2-two")
	parent = testmap.getParent("1-one")
	assert.Equals(t, parent, "")
}

func TestRevTreeGetHistory(t *testing.T) {
	history := testmap.getHistory("3-three")
	assert.DeepEquals(t, history, []string{"3-three", "2-two", "1-one"})
}

func TestRevTreeGetLeaves(t *testing.T) {
	leaves := testmap.GetLeaves()
	assert.DeepEquals(t, leaves, []string{"3-three"})
	leaves = branchymap.GetLeaves()
	sort.Strings(leaves)
	assert.DeepEquals(t, leaves, []string{"3-drei", "3-three"})
}

func TestRevTreeForEachLeaf(t *testing.T) {
	var leaves []string
	branchymap.forEachLeaf(func(rev *RevInfo) {
		leaves = append(leaves, rev.ID)
	})
	sort.Strings(leaves)
	assert.DeepEquals(t, leaves, []string{"3-drei", "3-three"})
}

func TestRevTreeAddRevision(t *testing.T) {
	tempmap := testmap.copy()
	assert.DeepEquals(t, tempmap, testmap)

	tempmap.addRevision(RevInfo{ID: "4-four", Parent: "3-three"})
	assert.Equals(t, tempmap.getParent("4-four"), "3-three")
}

func TestRevTreeCompareRevIDs(t *testing.T) {
	assert.Equals(t, compareRevIDs("1-aaa", "1-aaa"), 0)
	assert.Equals(t, compareRevIDs("1-aaa", "5-aaa"), -1)
	assert.Equals(t, compareRevIDs("10-aaa", "5-aaa"), 1)
	assert.Equals(t, compareRevIDs("1-bbb", "1-aaa"), 1)
	assert.Equals(t, compareRevIDs("5-bbb", "1-zzz"), 1)
}

func TestRevTreeIsLeaf(t *testing.T) {
	assertTrue(t, branchymap.isLeaf("3-three"), "isLeaf failed on 3-three")
	assertTrue(t, branchymap.isLeaf("3-drei"), "isLeaf failed on 3-drei")
	assertFalse(t, branchymap.isLeaf("2-two"), "isLeaf failed on 2-two")
	assertFalse(t, branchymap.isLeaf("bogus"), "isLeaf failed on 'bogus")
	assertFalse(t, branchymap.isLeaf(""), "isLeaf failed on ''")
}

func TestRevTreeWinningRev(t *testing.T) {
	tempmap := branchymap.copy()
	winner, branched, conflict := tempmap.winningRevision()
	assert.Equals(t, winner, "3-three")
	assert.True(t, branched)
	assert.True(t, conflict)
	tempmap.addRevision(RevInfo{ID: "4-four", Parent: "3-three"})
	winner, branched, conflict = tempmap.winningRevision()
	assert.Equals(t, winner, "4-four")
	assert.True(t, branched)
	assert.True(t, conflict)
	tempmap.addRevision(RevInfo{ID: "5-five", Parent: "4-four", Deleted: true})
	winner, branched, conflict = tempmap.winningRevision()
	assert.Equals(t, winner, "3-drei")
	assert.True(t, branched)
	assert.False(t, conflict)
}

func TestPruneRevisions(t *testing.T) {
	tempmap := branchymap.copy()
	tempmap["4-vier"] = &RevInfo{ID: "4-vier", Parent: "3-drei"}
	//               / 3-three
	// 1-one -- 2-two
	//               \ 3-drei -- 4-vier
	assert.Equals(t, tempmap.pruneRevisions(1000, ""), 0)
	assert.Equals(t, tempmap.pruneRevisions(3, ""), 0)
	assert.Equals(t, tempmap.pruneRevisions(2, ""), 1)
	assert.Equals(t, len(tempmap), 4)
	assert.Equals(t, tempmap["1-one"], (*RevInfo)(nil))
	assert.Equals(t, tempmap["2-two"].Parent, "")

	// Make sure leaves are never pruned: (note: by now 1-one is already gone)
	assert.Equals(t, tempmap.pruneRevisions(1, ""), 1)
	assert.Equals(t, len(tempmap), 3)
	assert.True(t, tempmap["3-three"] != nil)
	assert.Equals(t, tempmap["3-three"].Parent, "")
	assert.True(t, tempmap["4-vier"] != nil)
	assert.Equals(t, tempmap["4-vier"].Parent, "3-drei")
	assert.Equals(t, tempmap["3-drei"].Parent, "")

	// Make sure old merged conflicts don't prevent pruning:
	tempmap = branchymap.copy()
	tempmap["4-vier"] = &RevInfo{ID: "4-vier", Parent: "3-drei", Deleted: true}
	tempmap["4-four"] = &RevInfo{ID: "4-four", Parent: "3-three"}
	tempmap["5-five"] = &RevInfo{ID: "5-five", Parent: "4-four"}
	tempmap["6-six"] = &RevInfo{ID: "6-six", Parent: "5-five"}
	//               / 3-three -- 4-four -- 5-five -- 6-six
	// 1-one -- 2-two
	//               \ 3-drei -- [4-vier]
	assert.Equals(t, tempmap.pruneRevisions(3, "1-one"), 0)
	assert.Equals(t, tempmap.pruneRevisions(3, "2-two"), 1)
	assert.Equals(t, tempmap.pruneRevisions(3, ""), 3)
	assert.Equals(t, len(tempmap), 4)
	assert.Equals(t, tempmap.pruneRevisions(2, ""), 2)
	assert.Equals(t, len(tempmap), 2)
	assert.Equals(t, tempmap["5-five"].Parent, "")
	assert.Equals(t, tempmap["6-six"].Parent, "5-five")

	// Check what happens when all revs are deleted:
	tempmap = branchymap.copy()
	tempmap["3-three"].Deleted = true
	tempmap["3-drei"].Deleted = true
	//               / [3-three]
	// 1-one -- 2-two
	//               \ [3-drei]
	assert.Equals(t, tempmap.pruneRevisions(3, ""), 0)
	assert.Equals(t, tempmap.pruneRevisions(2, ""), 1)
}

func TestParseRevisions(t *testing.T) {
	type testCase struct {
		json string
		ids  []string
	}
	cases := []testCase{
		{`{"_revisions": {"start": 5, "ids": ["huey", "dewey", "louie"]}}`,
			[]string{"5-huey", "4-dewey", "3-louie"}},
		{`{"_revisions": {"start": 3, "ids": ["huey"]}}`,
			[]string{"3-huey"}},
		{`{"_rev": "3-huey"}`,
			[]string{"3-huey"}},
		{`{"_revisions": {"start": 2, "ids": ["huey", "dewey", "louie"]}}`, nil},
		{`{"_revisions": {"ids": ["huey", "dewey", "louie"]}}`, nil},
		{`{"_revisions": {"ids": "bogus"}}`, nil},
		{`{"_revisions": {"start": 2}}`, nil},
		{`{"_revisions": {"start": "", "ids": ["huey", "dewey", "louie"]}}`, nil},
		{`{"_revisions": 3.14159}`, nil},
		{`{"_rev": 3.14159}`, nil},
		{`{"_rev": "x-14159"}`, nil},
		{`{"_Xrevisions": {"start": "", "ids": ["huey", "dewey", "louie"]}}`, nil},
	}
	for _, c := range cases {
		var body Body
		assertNoError(t, json.Unmarshal([]byte(c.json), &body), "base JSON in test case")
		ids := ParseRevisions(body)
		assert.DeepEquals(t, ids, c.ids)
	}
}

func TestEncodeRevisions(t *testing.T) {
	encoded := encodeRevisions([]string{"5-huey", "4-dewey", "3-louie"})
	assert.DeepEquals(t, encoded, Body{"start": 5, "ids": []string{"huey", "dewey", "louie"}})
}

func TestTrimEncodedRevisionsToAncestor(t *testing.T) {
	encoded := encodeRevisions([]string{"5-huey", "4-dewey", "3-louie", "2-screwy"})

	assert.True(t, trimEncodedRevisionsToAncestor(encoded, []string{"3-walter", "17-gretchen", "1-fooey"}, 1000))
	assert.DeepEquals(t, encoded, Body{"start": 5, "ids": []string{"huey", "dewey", "louie", "screwy"}})

	assert.True(t, trimEncodedRevisionsToAncestor(encoded, []string{"3-walter", "3-louie", "1-fooey"}, 2))
	assert.DeepEquals(t, encoded, Body{"start": 5, "ids": []string{"huey", "dewey", "louie"}})

	assert.True(t, trimEncodedRevisionsToAncestor(encoded, []string{"3-walter", "3-louie", "1-fooey"}, 3))
	assert.DeepEquals(t, encoded, Body{"start": 5, "ids": []string{"huey", "dewey", "louie"}})

	assert.True(t, trimEncodedRevisionsToAncestor(encoded, []string{"3-walter", "3-louie", "5-huey"}, 3))
	assert.DeepEquals(t, encoded, Body{"start": 5, "ids": []string{"huey"}})

	// Check maxLength with no ancestors:
	encoded = encodeRevisions([]string{"5-huey", "4-dewey", "3-louie", "2-screwy"})
	assert.True(t, trimEncodedRevisionsToAncestor(encoded, nil, 6))
	assert.DeepEquals(t, encoded, Body{"start": 5, "ids": []string{"huey", "dewey", "louie", "screwy"}})
	assert.True(t, trimEncodedRevisionsToAncestor(encoded, nil, 2))
	assert.DeepEquals(t, encoded, Body{"start": 5, "ids": []string{"huey", "dewey"}})
}

//////// HELPERS:

func assertFailed(t *testing.T, message string) {
	_, file, line, ok := runtime.Caller(2) // assertFailed + assertNoError + public function.
	if ok {
		// Truncate file name at last file name separator.
		if index := strings.LastIndex(file, "/"); index >= 0 {
			file = file[index+1:]
		} else if index = strings.LastIndex(file, "\\"); index >= 0 {
			file = file[index+1:]
		}
	} else {
		file = "???"
		line = 1
	}
	t.Fatalf("%s:%d: %s", file, line, message)
}

func assertNoError(t *testing.T, err error, message string) {
	if err != nil {
		assertFailed(t, fmt.Sprintf("%s: %v", message, err))
	}
}

func assertTrue(t *testing.T, success bool, message string) {
	if !success {
		assertFailed(t, message)
	}
}

func assertFalse(t *testing.T, failure bool, message string) {
	if failure {
		assertFailed(t, message)
	}
}
