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
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"

)

// 1-one -- 2-two -- 3-three
var testmap = RevTree{"3-three": {ID: "3-three", Parent: "2-two", Body: []byte("{}")},
	"2-two": {ID: "2-two", Parent: "1-one", Channels: base.SetOf("ABC", "CBS")},
	"1-one": {ID: "1-one", Channels: base.SetOf("ABC")}}

//               / 3-three
// 1-one -- 2-two
//               \ 3-drei
var branchymap = RevTree{"3-three": {ID: "3-three", Parent: "2-two"},
	"2-two":  {ID: "2-two", Parent: "1-one"},
	"1-one":  {ID: "1-one"},
	"3-drei": {ID: "3-drei", Parent: "2-two"}}

var multiroot = RevTree{"3-a": {ID: "3-a", Parent: "2-a"},
	"2-a": {ID: "2-a", Parent: "1-a"},
	"1-a": {ID: "1-a"},
	"7-b": {ID: "7-b", Parent: "6-b"},
	"6-b": {ID: "6-b"},
}

type BranchSpec struct {
	NumRevs                 int
	LastRevisionIsTombstone bool
	Digest                  string
}

//            / 3-a -- 4-a -- 5-a ...... etc (winning branch)
// 1-a -- 2-a
//            \ 3-b -- 4-b ... etc (losing branch)
//
// NOTE: the 1-a -- 2-a unconflicted branch can be longer, depending on value of unconflictedBranchNumRevs
func getTwoBranchTestRevtree1(unconflictedBranchNumRevs, winningBranchNumRevs, losingBranchNumRevs int, tombstoneLosingBranch bool) RevTree {

	branchSpecs := []BranchSpec{
		{
			NumRevs:                 losingBranchNumRevs,
			Digest:                  "b",
			LastRevisionIsTombstone: tombstoneLosingBranch,
		},
	}

	return getMultiBranchTestRevtree1(unconflictedBranchNumRevs, winningBranchNumRevs, branchSpecs)

}

//            / 3-a -- 4-a -- 5-a ...... etc (winning branch)
// 1-a -- 2-a
//            \ 3-b -- 4-b ... etc (losing branch #1)
//            \ 3-c -- 4-c ... etc (losing branch #2)
//            \ 3-d -- 4-d ... etc (losing branch #n)
//
// NOTE: the 1-a -- 2-a unconflicted branch can be longer, depending on value of unconflictedBranchNumRevs
func getMultiBranchTestRevtree1(unconflictedBranchNumRevs, winningBranchNumRevs int, losingBranches []BranchSpec) RevTree {

	if unconflictedBranchNumRevs < 1 {
		panic(fmt.Sprintf("Must have at least 1 unconflictedBranchNumRevs"))
	}

	winningBranchDigest := "winning"

	const testJSON = `{
		   "revs":[
			  "1-winning"
		   ],
		   "parents":[
			  -1
		   ],
		   "channels":[
			  null
		   ]
		}`

	revTree := RevTree{}
	if err := json.Unmarshal([]byte(testJSON), &revTree); err != nil {
		panic(fmt.Sprintf("Error: %v", err))
	}

	if unconflictedBranchNumRevs > 1 {
		// Add revs to unconflicted branch
		addRevs(
			revTree,
			"1-winning",
			unconflictedBranchNumRevs-1,
			winningBranchDigest,
		)
	}

	if winningBranchNumRevs > 0 {

		// Figure out which generation the conflicting branches will start at
		generation := unconflictedBranchNumRevs

		// Figure out the starting revision id on winning and losing branches
		winningBranchStartRev := fmt.Sprintf("%d-%s", generation, winningBranchDigest)

		// Add revs to winning branch
		addRevs(
			revTree,
			winningBranchStartRev,
			winningBranchNumRevs,
			winningBranchDigest,
		)

	}

	for _, losingBranchSpec := range losingBranches {

		if losingBranchSpec.NumRevs > 0 {

			// Figure out which generation the conflicting branches will start at
			generation := unconflictedBranchNumRevs

			losingBranchStartRev := fmt.Sprintf("%d-%s", generation, winningBranchDigest) // Start on last revision of the non-conflicting branch

			// Add revs to losing branch
			addRevs(
				revTree,
				losingBranchStartRev,
				losingBranchSpec.NumRevs, // Subtract 1 since we already added initial
				losingBranchSpec.Digest,
			)

			generation += losingBranchSpec.NumRevs

			if losingBranchSpec.LastRevisionIsTombstone {

				newRevId := fmt.Sprintf("%v-%v", generation+1, losingBranchSpec.Digest)
				parentRevId := fmt.Sprintf("%v-%v", generation, losingBranchSpec.Digest)

				revInfo := RevInfo{
					ID:      newRevId,
					Parent:  parentRevId,
					Deleted: true,
				}
				revTree.addRevision("testdoc", revInfo)

			}

		}

	}

	return revTree

}

func testUnmarshal(t *testing.T, jsonString string) RevTree {
	gotmap := RevTree{}
	assertNoError(t, json.Unmarshal([]byte(jsonString), &gotmap), "Couldn't parse RevTree from JSON")
	assert.DeepEquals(t, gotmap, testmap)
	return gotmap
}

// Make sure that the getMultiBranchTestRevtree1() helper works as expected
// (added in reaction to bug where it created broken trees/forests)
func TestGetMultiBranchTestRevtree(t *testing.T) {

	branchSpecs := []BranchSpec{
		{
			NumRevs:                 60,
			Digest:                  "left",
			LastRevisionIsTombstone: false,
		},
		{
			NumRevs:                 25,
			Digest:                  "right",
			LastRevisionIsTombstone: true,
		},
	}
	revTree := getMultiBranchTestRevtree1(50, 100, branchSpecs)
	leaves := revTree.GetLeaves()
	sort.Strings(leaves)
	assert.DeepEquals(t, leaves, []string{"110-left", "150-winning", "76-right"})

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

func TestRevTreeUnmarshalRevChannelCountMismatch(t *testing.T) {
	const testJSON = `{"revs": ["3-three", "2-two", "1-one"], "parents": [1, 2, -1], "bodymap": {"0":"{}"}, "channels": [null, ["ABC", "CBS"]]}`
	gotmap := RevTree{}
	err := json.Unmarshal([]byte(testJSON), &gotmap)
	assert.DeepEquals(t, err, errors.New("revtreelist data is invalid, revs/parents/channels counts are inconsistent"))
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
	history, err := testmap.getHistory("3-three")
	assert.True(t, err == nil)
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

	tempmap.addRevision("testdoc", RevInfo{ID: "4-four", Parent: "3-three"})
	assert.Equals(t, tempmap.getParent("4-four"), "3-three")
}

func TestRevTreeAddRevisionWithEmptyID(t *testing.T) {
	tempmap := testmap.copy()
	assert.DeepEquals(t, tempmap, testmap)

	err := tempmap.addRevision("testdoc", RevInfo{Parent: "3-three"})
	assert.DeepEquals(t, err, errors.New(fmt.Sprintf("doc: %v, RevTree addRevision, empty revid is illegal", "testdoc")))
}

func TestRevTreeAddDuplicateRevID(t *testing.T) {
	tempmap := testmap.copy()
	assert.DeepEquals(t, tempmap, testmap)

	err := tempmap.addRevision("testdoc", RevInfo{ID: "2-two", Parent: "1-one"})
	assert.DeepEquals(t, err, errors.New(fmt.Sprintf("doc: %v, RevTree addRevision, already contains rev %q", "testdoc", "2-two")))
}

func TestRevTreeAddRevisionWithMissingParent(t *testing.T) {
	tempmap := testmap.copy()
	assert.DeepEquals(t, tempmap, testmap)

	err := tempmap.addRevision("testdoc", RevInfo{ID: "5-five", Parent: "4-four"})
	assert.DeepEquals(t, err, errors.New(fmt.Sprintf("doc: %v, RevTree addRevision, parent id %q is missing", "testdoc", "4-four")))
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
	tempmap.addRevision("testdoc", RevInfo{ID: "4-four", Parent: "3-three"})
	winner, branched, conflict = tempmap.winningRevision()
	assert.Equals(t, winner, "4-four")
	assert.True(t, branched)
	assert.True(t, conflict)
	tempmap.addRevision("testdoc", RevInfo{ID: "5-five", Parent: "4-four", Deleted: true})
	winner, branched, conflict = tempmap.winningRevision()
	assert.Equals(t, winner, "3-drei")
	assert.True(t, branched)
	assert.False(t, conflict)
}

func TestPruneRevisions(t *testing.T) {

	tempmap := testmap.copy()
	tempmap.computeDepthsAndFindLeaves()
	assert.Equals(t, tempmap["3-three"].depth, uint32(1))
	assert.Equals(t, tempmap["2-two"].depth, uint32(2))
	assert.Equals(t, tempmap["1-one"].depth, uint32(3))

	tempmap = branchymap.copy()
	tempmap.computeDepthsAndFindLeaves()
	assert.Equals(t, tempmap["3-three"].depth, uint32(1))
	assert.Equals(t, tempmap["3-drei"].depth, uint32(1))
	assert.Equals(t, tempmap["2-two"].depth, uint32(2))
	assert.Equals(t, tempmap["1-one"].depth, uint32(3))

	tempmap["4-vier"] = &RevInfo{ID: "4-vier", Parent: "3-drei"}
	tempmap.computeDepthsAndFindLeaves()
	assert.Equals(t, tempmap["4-vier"].depth, uint32(1))
	assert.Equals(t, tempmap["3-drei"].depth, uint32(2))
	assert.Equals(t, tempmap["3-three"].depth, uint32(1))
	assert.Equals(t, tempmap["2-two"].depth, uint32(2))
	assert.Equals(t, tempmap["1-one"].depth, uint32(3))

	// Prune:
	pruned, _ := tempmap.pruneRevisions(1000, "")
	assert.Equals(t, pruned, 0)
	pruned, _ = tempmap.pruneRevisions(3, "")
	assert.Equals(t, pruned, 0)
	pruned, _ = tempmap.pruneRevisions(2, "")
	assert.Equals(t, pruned, 1)
	assert.Equals(t, len(tempmap), 4)
	assert.Equals(t, tempmap["1-one"], (*RevInfo)(nil))
	assert.Equals(t, tempmap["2-two"].Parent, "")

	// Make sure leaves are never pruned:
	pruned, _ = tempmap.pruneRevisions(1, "")
	assert.Equals(t, pruned, 2)
	assert.Equals(t, len(tempmap), 2)
	assert.True(t, tempmap["3-three"] != nil)
	assert.Equals(t, tempmap["3-three"].Parent, "")
	assert.True(t, tempmap["4-vier"] != nil)
	assert.Equals(t, tempmap["4-vier"].Parent, "")

}


func TestPruneRevsSingleBranch(t *testing.T) {

	numRevs := 100

	revTree := getMultiBranchTestRevtree1(numRevs, 0, []BranchSpec{})

	maxDepth := uint32(20)
	expectedNumPruned := numRevs - int(maxDepth)

	numPruned, _ := revTree.pruneRevisions(maxDepth, "")
	assert.Equals(t, numPruned, expectedNumPruned)

}

func TestPruneRevsOneWinningOneNonwinningBranch(t *testing.T) {

	branchSpecs := []BranchSpec{
		{
			NumRevs:                 1,
			Digest:                  "non-winning unresolved",
			LastRevisionIsTombstone: false,
		},
	}

	unconflictedBranchNumRevs := 2
	winningBranchNumRevs := 4

	revTree := getMultiBranchTestRevtree1(unconflictedBranchNumRevs, winningBranchNumRevs, branchSpecs)

	maxDepth := uint32(2)

	revTree.pruneRevisions(maxDepth, "")

	assert.Equals(t, revTree.LongestBranch(), int(maxDepth))

}

func TestPruneRevsOneWinningOneOldTombstonedBranch(t *testing.T) {

	branchSpecs := []BranchSpec{
		{
			NumRevs:                 1,
			Digest:                  "non-winning tombstoned",
			LastRevisionIsTombstone: true,
		},
	}

	unconflictedBranchNumRevs := 1
	winningBranchNumRevs := 5

	revTree := getMultiBranchTestRevtree1(unconflictedBranchNumRevs, winningBranchNumRevs, branchSpecs)

	maxDepth := uint32(2)

	revTree.pruneRevisions(maxDepth, "")

	assert.True(t, revTree.LongestBranch() == int(maxDepth))

	// we shouldn't have any tombstoned branches, since the tombstoned branch was so old
	// it should have been pruned away
	assert.Equals(t, revTree.FindLongestTombstonedBranch(), 0)

}

func TestPruneRevsOneWinningOneOldAndOneRecentTombstonedBranch(t *testing.T) {

	branchSpecs := []BranchSpec{
		{
			NumRevs:                 1,
			Digest:                  "non-winning low-gen tombstoned",
			LastRevisionIsTombstone: true,
		},
		{
			NumRevs:                 4,
			Digest:                  "non-winning high-gen tombstoned",
			LastRevisionIsTombstone: true,
		},
	}

	unconflictedBranchNumRevs := 1
	winningBranchNumRevs := 5

	revTree := getMultiBranchTestRevtree1(unconflictedBranchNumRevs, winningBranchNumRevs, branchSpecs)

	maxDepth := uint32(2)

	revTree.pruneRevisions(maxDepth, "")

	assert.True(t, revTree.LongestBranch() == int(maxDepth))

	// the "non-winning high-gen tombstoned" branch should still be around, but pruned to maxDepth
	tombstonedLeaves := revTree.GetTombstonedLeaves()
	assert.Equals(t, len(tombstonedLeaves), 1)
	tombstonedLeaf := tombstonedLeaves[0]

	tombstonedBranch, err := revTree.getHistory(tombstonedLeaf)
	assert.True(t, err == nil)
	assert.Equals(t, len(tombstonedBranch), int(maxDepth))

	// The generation of the longest deleted branch is 97:
	// 1 unconflictedBranchNumRevs
	// +
	// 4 revs in branchspec
	// +
	// 1 extra rev in branchspec since LastRevisionIsTombstone (that variable name is misleading)
	expectedGenLongestTSd := 6
	assert.Equals(t, revTree.FindLongestTombstonedBranch(), expectedGenLongestTSd)

}

func TestGenerationShortestNonTombstonedBranch(t *testing.T) {

	branchSpecs := []BranchSpec{
		{
			NumRevs:                 4,
			Digest:                  "non-winning unresolved",
			LastRevisionIsTombstone: false,
		},
		{
			NumRevs:                 2,
			Digest:                  "non-winning tombstoned",
			LastRevisionIsTombstone: true,
		},
	}

	revTree := getMultiBranchTestRevtree1(3, 7, branchSpecs)

	generationShortestNonTombstonedBranch, _ := revTree.FindShortestNonTombstonedBranch()

	// The "non-winning unresolved" branch has 7 revisions due to:
	// 3 unconflictedBranchNumRevs
	// +
	// 4 from it's BranchSpec
	// Since the "non-winning tombstoned" is a deleted branch, it will be ignored by GenerationShortestNonTombstonedBranch()
	// Also, the winning branch has more revisions (10 total), and so will be ignored too
	expectedGenerationShortestNonTombstonedBranch := 7

	assert.Equals(t, generationShortestNonTombstonedBranch, expectedGenerationShortestNonTombstonedBranch)

}

func TestGenerationLongestTombstonedBranch(t *testing.T) {

	branchSpecs := []BranchSpec{
		{
			NumRevs:                 4,
			Digest:                  "non-winning unresolved",
			LastRevisionIsTombstone: false,
		},
		{
			NumRevs:                 2,
			Digest:                  "non-winning tombstoned #1",
			LastRevisionIsTombstone: true,
		},
		{
			NumRevs:                 100,
			Digest:                  "non-winning tombstoned #2",
			LastRevisionIsTombstone: true,
		},
	}

	revTree := getMultiBranchTestRevtree1(3, 7, branchSpecs)
	generationLongestTombstonedBranch := revTree.FindLongestTombstonedBranch()

	// The generation of the longest deleted branch is:
	// 3 unconflictedBranchNumRevs
	// +
	// 100 revs in branchspec
	// +
	// 1 extra rev in branchspec since LastRevisionIsTombstone (that variable name is misleading)
	expectedGenerationLongestTombstonedBranch := 3 + 100 + 1

	assert.Equals(t, generationLongestTombstonedBranch, expectedGenerationLongestTombstonedBranch)

}

// Tests for updated pruning algorithm, post https://github.com/couchbase/sync_gateway/issues/2651
func TestPruneRevisionsPostIssue2651ThreeBranches(t *testing.T) {

	// Try large rev tree with multiple branches
	branchSpecs := []BranchSpec{
		{
			NumRevs:                 60,
			Digest:                  "non-winning unresolved",
			LastRevisionIsTombstone: false,
		},
		{
			NumRevs:                 25,
			Digest:                  "non-winning tombstoned",
			LastRevisionIsTombstone: true,
		},
	}
	revTree := getMultiBranchTestRevtree1(50, 100, branchSpecs)

	maxDepth := uint32(50)
	numPruned, _ := revTree.pruneRevisions(maxDepth, "")
	fmt.Printf("numPruned: %v", numPruned)
	fmt.Printf("LongestBranch: %v", revTree.LongestBranch())

	assert.True(t, uint32(revTree.LongestBranch()) == maxDepth)

}

func TestPruneRevsSingleTombstonedBranch(t *testing.T) {

	numRevsTotal := 100
	numRevsDeletedBranch := 99
	branchSpecs := []BranchSpec{
		{
			NumRevs:                 numRevsDeletedBranch,
			Digest:                  "single-tombstoned-branch",
			LastRevisionIsTombstone: true,
		},
	}

	revTree := getMultiBranchTestRevtree1(1, 0, branchSpecs)

	log.Printf("RevTreeAfter before: %v", revTree.RenderGraphvizDot())

	maxDepth := uint32(20)
	expectedNumPruned := numRevsTotal - int(maxDepth)

	expectedNumPruned += 1 // To account for the tombstone revision in the branchspec, which is spearate from NumRevs

	numPruned, _ := revTree.pruneRevisions(maxDepth, "")

	log.Printf("RevTreeAfter pruning: %v", revTree.RenderGraphvizDot())

	assert.Equals(t, numPruned, expectedNumPruned)

}

func TestLongestBranch1(t *testing.T) {

	branchSpecs := []BranchSpec{
		{
			NumRevs:                 60,
			Digest:                  "non-winning unresolved",
			LastRevisionIsTombstone: false,
		},
		{
			NumRevs:                 25,
			Digest:                  "non-winning tombstoned",
			LastRevisionIsTombstone: true,
		},
	}
	revTree := getMultiBranchTestRevtree1(50, 100, branchSpecs)

	assert.True(t, revTree.LongestBranch() == 150)

}

func TestLongestBranch2(t *testing.T) {

	assert.True(t, multiroot.LongestBranch() == 3)

}


// Create a disconnected rev tree
// Add lots of revisions to winning branch
// Prune rev tree
// Make sure the winning branch is pruned as expected
func TestPruneDisconnectedRevTreeWithLongWinningBranch(t *testing.T) {

	dumpRevTreeDotFiles := false

	branchSpecs := []BranchSpec{
		{
			NumRevs:                 10,
			Digest:                  "non-winning",
			LastRevisionIsTombstone: false,
		},
	}
	revTree := getMultiBranchTestRevtree1(1, 15, branchSpecs)

	if (dumpRevTreeDotFiles) {
		ioutil.WriteFile("/tmp/TestPruneDisconnectedRevTreeWithLongWinningBranch_initial.dot", []byte(revTree.RenderGraphvizDot()), 0666)
	}

	maxDepth := uint32(7)

	revTree.pruneRevisions(maxDepth, "")

	if (dumpRevTreeDotFiles) {
		ioutil.WriteFile("/tmp/TestPruneDisconnectedRevTreeWithLongWinningBranch_pruned1.dot", []byte(revTree.RenderGraphvizDot()), 0666)
	}

	winningBranchStartRev := fmt.Sprintf("%d-%s", 16, "winning")

	// Add revs to winning branch
	addRevs(
		revTree,
		winningBranchStartRev,
		10,
		"winning",
	)

	if (dumpRevTreeDotFiles) {
		ioutil.WriteFile("/tmp/TestPruneDisconnectedRevTreeWithLongWinningBranch_add_winning_revs.dot", []byte(revTree.RenderGraphvizDot()), 0666)
	}

	revTree.pruneRevisions(maxDepth, "")

	if (dumpRevTreeDotFiles) {
		ioutil.WriteFile("/tmp/TestPruneDisconnectedRevTreeWithLongWinningBranch_pruned_final.dot", []byte(revTree.RenderGraphvizDot()), 0666)
	}

	// Make sure the winning branch is pruned down to maxDepth, even with the disconnected rev tree
	assert.True(t, revTree.LongestBranch() == 7)

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

	result, trimmedRevs := trimEncodedRevisionsToAncestor(encoded, []string{"3-walter", "17-gretchen", "1-fooey"}, 1000)
	assert.True(t, result)
	assert.DeepEquals(t, trimmedRevs, Body{"start": 5, "ids": []string{"huey", "dewey", "louie", "screwy"}})

	result, trimmedRevs = trimEncodedRevisionsToAncestor(trimmedRevs, []string{"3-walter", "3-louie", "1-fooey"}, 2)
	assert.True(t, result)
	assert.DeepEquals(t, trimmedRevs, Body{"start": 5, "ids": []string{"huey", "dewey", "louie"}})

	result, trimmedRevs = trimEncodedRevisionsToAncestor(trimmedRevs, []string{"3-walter", "3-louie", "1-fooey"}, 3)
	assert.True(t, result)
	assert.DeepEquals(t, trimmedRevs, Body{"start": 5, "ids": []string{"huey", "dewey", "louie"}})

	result, trimmedRevs = trimEncodedRevisionsToAncestor(trimmedRevs, []string{"3-walter", "3-louie", "5-huey"}, 3)
	assert.True(t, result)
	assert.DeepEquals(t, trimmedRevs, Body{"start": 5, "ids": []string{"huey"}})

	// Check maxLength with no ancestors:
	encoded = encodeRevisions([]string{"5-huey", "4-dewey", "3-louie", "2-screwy"})

	result, trimmedRevs = trimEncodedRevisionsToAncestor(encoded, nil, 6)
	assert.True(t, result)
	assert.DeepEquals(t, trimmedRevs, Body{"start": 5, "ids": []string{"huey", "dewey", "louie", "screwy"}})

	result, trimmedRevs = trimEncodedRevisionsToAncestor(trimmedRevs, nil, 2)
	assert.True(t, result)
	assert.DeepEquals(t, trimmedRevs, Body{"start": 5, "ids": []string{"huey", "dewey"}})
}

// Regression test for https://github.com/couchbase/sync_gateway/issues/2847
func TestRevsHistoryInfiniteLoop(t *testing.T) {

	docId := "testdocProblematicRevTree"

	rawDoc, err := unmarshalDocument(docId, []byte(testdocProblematicRevTree1))
	if err != nil {
		t.Fatalf("Error unmarshalling doc: %v", err)
	}

	revId := "275-6458b32429e335f981fc12b73765833d"
	_, err = getHistoryWithTimeout(
		rawDoc,
		revId,
		time.Second*1,
	)

	// This should return an error, since the history has cycles
	assert.True(t, err != nil)

	// The error should *not* be a timeout error
	assert.False(t, strings.Contains(err.Error(), "Timeout"))

}

// Repair tool for https://github.com/couchbase/sync_gateway/issues/2847
func TestRepairRevsHistoryWithCycles(t *testing.T) {

	base.EnableLogKey("CRUD")

	for i, testdocProblematicRevTree := range testdocProblematicRevTrees {

		docId := "testdocProblematicRevTree"

		rawDoc, err := unmarshalDocument(docId, []byte(testdocProblematicRevTree))
		if err != nil {
			t.Fatalf("Error unmarshalling doc %d: %v", i, err)
		}

		if err := rawDoc.History.RepairCycles(); err != nil {
			t.Fatalf("Unable to repair doc.  Err: %v", err)
		}

		// This function will be called back for every leaf node in tree
		leafProcessor := func(leaf *RevInfo) {

			_, err := rawDoc.History.getHistory(leaf.ID)
			if err != nil {
				t.Fatalf("GetHistory() returned error: %v", err)
			}

		}

		// Iterate over leaves and make sure none of them have a history with cycles
		rawDoc.History.forEachLeaf(leafProcessor)

	}


}

// TODO: add test for two tombstone branches getting pruned at once

// Repro case for https://github.com/couchbase/sync_gateway/issues/2847
func TestRevisionPruningLoop(t *testing.T) {

	revsLimit := uint32(5)
	revBody := []byte(`{"foo":"bar"}`)
	nonTombstone := false
	tombstone := true

	// create rev tree with a root entry
	revTree := RevTree{}
	err := addAndGet(revTree, "1-foo", "", nonTombstone)
	assertNoError(t, err, "Error adding revision 1-foo to tree")

	// Add several entries (2-foo to 5-foo)
	for generation := 2; generation <= 5; generation++ {
		revID := fmt.Sprintf("%d-foo", generation)
		parentRevID := fmt.Sprintf("%d-foo", generation-1)
		err := addAndGet(revTree, revID, parentRevID, nonTombstone)
		assertNoError(t, err, fmt.Sprintf("Error adding revision 1-foo to tree", revID))
	}

	// Add tombstone children of 3-foo and 4-foo

	err = addAndGet(revTree, "4-bar", "3-foo", nonTombstone)
	err = addAndGet(revTree, "5-bar", "4-bar", tombstone)
	assertNoError(t, err, "Error adding tombstone 4-bar to tree")

	/*
		// Add a second branch as a child of 2-foo.
		err = addAndGet(revTree, "3-bar", "2-foo", nonTombstone)
		assertNoError(t, err, "Error adding revision 3-bar to tree")

		// Tombstone the second branch
		err = addAndGet(revTree, "4-bar", "3-bar", tombstone)
		assertNoError(t, err, "Error adding tombstone 4-bar to tree")
	*/

	// Add a another tombstoned branch as a child of 5-foo.  This will ensure that 2-foo doesn't get pruned
	// until the first tombstone branch is deleted.
	/*
		err = addAndGet(revTree, "6-bar2", "5-foo", tombstone)
		assertNoError(t, err, "Error adding tombstone 6-bar2 to tree")
	*/

	log.Printf("Tree before adding to main branch: [[%s]]", revTree.RenderGraphvizDot())

	// Keep adding to the main branch without pruning.  Simulates old pruning algorithm,
	// which maintained rev history due to tombstone branch
	for generation := 6; generation <= 15; generation++ {
		revID := fmt.Sprintf("%d-foo", generation)
		parentRevID := fmt.Sprintf("%d-foo", generation-1)
		_, err := addPruneAndGet(revTree, revID, parentRevID, revBody, revsLimit, nonTombstone)
		assertNoError(t, err, fmt.Sprintf("Error adding revision %s to tree", revID))

		keepAliveRevID := fmt.Sprintf("%d-keep", generation)
		_, err = addPruneAndGet(revTree, keepAliveRevID, parentRevID, revBody, revsLimit, tombstone)
		assertNoError(t, err, fmt.Sprintf("Error adding revision %s to tree", revID))

		// The act of marshalling the rev tree and then unmarshalling back into a revtree data structure
		// causes the issue.
		log.Printf("Tree pre-marshal: [[%s]]", revTree.RenderGraphvizDot())
		treeBytes, marshalErr := revTree.MarshalJSON()
		assertNoError(t, marshalErr, fmt.Sprintf("Error marshalling tree: %v", marshalErr))
		revTree = RevTree{}
		unmarshalErr := revTree.UnmarshalJSON(treeBytes)
		assertNoError(t, unmarshalErr, fmt.Sprintf("Error unmarshalling tree: %v", unmarshalErr))
	}

}

func addAndGet(revTree RevTree, revID string, parentRevID string, isTombstone bool) error {

	revBody := []byte(`{"foo":"bar"}`)
	revTree.addRevision("foobar", RevInfo{
		ID:      revID,
		Parent:  parentRevID,
		Body:    revBody,
		Deleted: isTombstone,
	})
	history, err := revTree.getHistory(revID)
	log.Printf("addAndGet.  Tree length: %d.  History for new rev: %v", len(revTree), history)
	return err

}

func addPruneAndGet(revTree RevTree, revID string, parentRevID string, revBody []byte, revsLimit uint32, tombstone bool) (numPruned int, err error) {
	revTree.addRevision("doc", RevInfo{
		ID:      revID,
		Parent:  parentRevID,
		Body:    revBody,
		Deleted: tombstone,
	})
	numPruned, _ = revTree.pruneRevisions(revsLimit, revID)

	// Get history for new rev (checks for loops)
	history, err := revTree.getHistory(revID)
	log.Printf("addPruneAndGet.  Tree length: %d.  Num pruned: %d.  History for new rev: %v", len(revTree), numPruned, history)
	return numPruned, err

}

func getHistoryWithTimeout(rawDoc *document, revId string, timeout time.Duration) (history []string, err error) {

	historyChannel := make(chan []string)
	errChannel := make(chan error)

	go func() {
		history, err := rawDoc.History.getHistory(revId)
		if err != nil {
			errChannel <- err
		} else {
			historyChannel <- history
		}
	}()

	select {
	case history := <-historyChannel:
		return history, nil
	case err := <-errChannel:
		return nil, err
	case _ = <-time.After(timeout):
		return nil, fmt.Errorf("Timeout waiting for history")
	}

}

//////// BENCHMARK:

func BenchmarkRevTreePruning(b *testing.B) {

	// Try large rev tree with multiple branches
	branchSpecs := []BranchSpec{
		{
			NumRevs:                 60,
			Digest:                  "non-winning unresolved",
			LastRevisionIsTombstone: false,
		},
		{
			NumRevs:                 25,
			Digest:                  "non-winning tombstoned",
			LastRevisionIsTombstone: true,
		},
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {

		b.StopTimer()
		revTree := getMultiBranchTestRevtree1(50, 100, branchSpecs)
		b.StartTimer()

		revTree.pruneRevisions(50, "")
	}

}

//////// HELPERS:

func assertFailed(t testing.TB, message string) {
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

func assertNoError(t testing.TB, err error, message string) {
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

func addRevs(revTree RevTree, startingParentRevId string, numRevs int, revDigest string) {

	docSizeBytes := 1024 * 5
	body := createBodyContentAsMapWithSize(docSizeBytes)
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		panic(fmt.Sprintf("Error: %v", err))
	}

	channels := base.SetOf("ABC", "CBS")

	generation, _ := ParseRevID(startingParentRevId)

	for i := 0; i < numRevs; i++ {

		newRevId := fmt.Sprintf("%v-%v", generation+1, revDigest)
		parentRevId := ""
		if i == 0 {
			parentRevId = startingParentRevId
		} else {
			parentRevId = fmt.Sprintf("%v-%v", generation, revDigest)
		}

		revInfo := RevInfo{
			ID:       newRevId,
			Parent:   parentRevId,
			Body:     bodyBytes,
			Deleted:  false,
			Channels: channels,
		}
		revTree.addRevision("testdoc", revInfo)

		generation += 1

	}

}

func (tree RevTree) GetTombstonedLeaves() []string {
	onlyTombstonedLeavesFilter := func(revId string) bool {
		revInfo := tree[revId]
		return revInfo.Deleted
	}
	return tree.GetLeavesFiltered(onlyTombstonedLeavesFilter)

}

// Find the length of the longest branch
func (tree RevTree) LongestBranch() int {

	longestBranch := 0

	leafProcessor := func(leaf *RevInfo) {

		lengthOfBranch := 0

		// Walk up the tree until we find a root, and append each node
		node := leaf
		for {

			// Increment length of branch
			lengthOfBranch += 1

			// Reached a root, we're done -- if this branch is longer than the
			// current longest branch, record branch length as longestBranch
			if node.IsRoot() {
				if lengthOfBranch > longestBranch {
					longestBranch = lengthOfBranch
				}
				break
			}

			// Walk up the branch to the parent node
			node = tree[node.Parent]

		}
	}

	tree.forEachLeaf(leafProcessor)

	return longestBranch

}

// Create body content as map of 100 byte entries.  Rounds up to the nearest 100 bytes
func createBodyContentAsMapWithSize(docSizeBytes int) map[string]string {

	numEntries := int(docSizeBytes/100) + 1
	body := make(map[string]string, numEntries)
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("field_%d", i)
		body[key] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	}
	return body
}
