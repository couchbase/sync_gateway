//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"fmt"
	"io/ioutil"
	"log"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	if err := base.JSONUnmarshal([]byte(testJSON), &revTree); err != nil {
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
				err := revTree.addRevision("testdoc", revInfo)
				if err != nil {
					panic(fmt.Sprintf("Error: %v", err))
				}

			}

		}

	}

	return revTree

}

func testUnmarshal(t *testing.T, jsonString string) RevTree {
	gotmap := RevTree{}
	assert.NoError(t, base.JSONUnmarshal([]byte(jsonString), &gotmap), "Couldn't parse RevTree from JSON")
	assert.Equal(t, testmap, gotmap)
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
	assert.Equal(t, []string{"110-left", "150-winning", "76-right"}, leaves)

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
	err := base.JSONUnmarshal([]byte(testJSON), &gotmap)
	assert.Errorf(t, err, "revtreelist data is invalid, revs/parents/channels counts are inconsistent")
}

func TestRevTreeMarshal(t *testing.T) {
	bytes, err := base.JSONMarshal(testmap)
	assert.NoError(t, err, "Couldn't write RevTree to JSON")
	fmt.Printf("Marshaled RevTree as %s\n", string(bytes))
	testUnmarshal(t, string(bytes))
}

func TestRevTreeAccess(t *testing.T) {
	assert.True(t, testmap.contains("3-three"), "contains 3 failed")
	assert.True(t, testmap.contains("1-one"), "contains 1 failed")
	assert.False(t, testmap.contains("foo"), "contains false positive")
}

func TestRevTreeParentAccess(t *testing.T) {
	parent := testmap.getParent("3-three")
	assert.Equal(t, "2-two", parent)
	parent = testmap.getParent("1-one")
	assert.Equal(t, "", parent)
}

func TestRevTreeGetHistory(t *testing.T) {
	history, err := testmap.getHistory("3-three")
	assert.True(t, err == nil)
	assert.Equal(t, []string{"3-three", "2-two", "1-one"}, history)
}

func TestRevTreeGetLeaves(t *testing.T) {
	leaves := testmap.GetLeaves()
	assert.Equal(t, []string{"3-three"}, leaves)
	leaves = branchymap.GetLeaves()
	sort.Strings(leaves)
	assert.Equal(t, []string{"3-drei", "3-three"}, leaves)
}

func TestRevTreeForEachLeaf(t *testing.T) {
	var leaves []string
	branchymap.forEachLeaf(func(rev *RevInfo) {
		leaves = append(leaves, rev.ID)
	})
	sort.Strings(leaves)
	assert.Equal(t, []string{"3-drei", "3-three"}, leaves)
}

func TestRevTreeAddRevision(t *testing.T) {
	tempmap := testmap.copy()
	assert.Equal(t, testmap, tempmap)

	err := tempmap.addRevision("testdoc", RevInfo{ID: "4-four", Parent: "3-three"})
	require.NoError(t, err)
	assert.Equal(t, "3-three", tempmap.getParent("4-four"))
}

func TestRevTreeAddRevisionWithEmptyID(t *testing.T) {
	tempmap := testmap.copy()
	assert.Equal(t, testmap, tempmap)

	err := tempmap.addRevision("testdoc", RevInfo{Parent: "3-three"})
	assert.Equal(t, fmt.Sprintf("doc: %v, RevTree addRevision, empty revid is illegal", "testdoc"), err.Error())
}

func TestRevTreeAddDuplicateRevID(t *testing.T) {
	tempmap := testmap.copy()
	assert.Equal(t, testmap, tempmap)

	err := tempmap.addRevision("testdoc", RevInfo{ID: "2-two", Parent: "1-one"})
	assert.Equal(t, fmt.Sprintf("doc: %v, RevTree addRevision, already contains rev %q", "testdoc", "2-two"), err.Error())
}

func TestRevTreeAddRevisionWithMissingParent(t *testing.T) {
	tempmap := testmap.copy()
	assert.Equal(t, testmap, tempmap)

	err := tempmap.addRevision("testdoc", RevInfo{ID: "5-five", Parent: "4-four"})
	assert.Equal(t, fmt.Sprintf("doc: %v, RevTree addRevision, parent id %q is missing", "testdoc", "4-four"), err.Error())
}

func TestRevTreeCompareRevIDs(t *testing.T) {
	assert.Equal(t, 0, compareRevIDs("1-aaa", "1-aaa"))
	assert.Equal(t, -1, compareRevIDs("1-aaa", "5-aaa"))
	assert.Equal(t, 1, compareRevIDs("10-aaa", "5-aaa"))
	assert.Equal(t, 1, compareRevIDs("1-bbb", "1-aaa"))
	assert.Equal(t, 1, compareRevIDs("5-bbb", "1-zzz"))
}

func TestRevTreeIsLeaf(t *testing.T) {
	assert.True(t, branchymap.isLeaf("3-three"), "isLeaf failed on 3-three")
	assert.True(t, branchymap.isLeaf("3-drei"), "isLeaf failed on 3-drei")
	assert.False(t, branchymap.isLeaf("2-two"), "isLeaf failed on 2-two")
	assert.False(t, branchymap.isLeaf("bogus"), "isLeaf failed on 'bogus")
	assert.False(t, branchymap.isLeaf(""), "isLeaf failed on ''")
}

func TestRevTreeWinningRev(t *testing.T) {
	tempmap := branchymap.copy()
	winner, branched, conflict := tempmap.winningRevision()
	assert.Equal(t, "3-three", winner)
	assert.True(t, branched)
	assert.True(t, conflict)
	err := tempmap.addRevision("testdoc", RevInfo{ID: "4-four", Parent: "3-three"})
	require.NoError(t, err)
	winner, branched, conflict = tempmap.winningRevision()
	assert.Equal(t, "4-four", winner)
	assert.True(t, branched)
	assert.True(t, conflict)
	err = tempmap.addRevision("testdoc", RevInfo{ID: "5-five", Parent: "4-four", Deleted: true})
	require.NoError(t, err)
	winner, branched, conflict = tempmap.winningRevision()
	assert.Equal(t, "3-drei", winner)
	assert.True(t, branched)
	assert.False(t, conflict)
}

func TestPruneRevisions(t *testing.T) {

	tempmap := testmap.copy()
	tempmap.computeDepthsAndFindLeaves()
	assert.Equal(t, uint32(1), tempmap["3-three"].depth)
	assert.Equal(t, uint32(2), tempmap["2-two"].depth)
	assert.Equal(t, uint32(3), tempmap["1-one"].depth)

	tempmap = branchymap.copy()
	tempmap.computeDepthsAndFindLeaves()
	assert.Equal(t, uint32(1), tempmap["3-three"].depth)
	assert.Equal(t, uint32(1), tempmap["3-drei"].depth)
	assert.Equal(t, uint32(2), tempmap["2-two"].depth)
	assert.Equal(t, uint32(3), tempmap["1-one"].depth)

	tempmap["4-vier"] = &RevInfo{ID: "4-vier", Parent: "3-drei"}
	tempmap.computeDepthsAndFindLeaves()
	assert.Equal(t, uint32(1), tempmap["4-vier"].depth)
	assert.Equal(t, uint32(2), tempmap["3-drei"].depth)
	assert.Equal(t, uint32(1), tempmap["3-three"].depth)
	assert.Equal(t, uint32(2), tempmap["2-two"].depth)
	assert.Equal(t, uint32(3), tempmap["1-one"].depth)

	// Prune:
	pruned, _ := tempmap.pruneRevisions(1000, "")
	assert.Equal(t, 0, pruned)
	pruned, _ = tempmap.pruneRevisions(3, "")
	assert.Equal(t, 0, pruned)
	pruned, _ = tempmap.pruneRevisions(2, "")
	assert.Equal(t, 1, pruned)
	assert.Equal(t, 4, len(tempmap))
	assert.Equal(t, (*RevInfo)(nil), tempmap["1-one"])
	assert.Equal(t, "", tempmap["2-two"].Parent)

	// Make sure leaves are never pruned:
	pruned, _ = tempmap.pruneRevisions(1, "")
	assert.Equal(t, 2, pruned)
	assert.Equal(t, 2, len(tempmap))
	assert.True(t, tempmap["3-three"] != nil)
	assert.Equal(t, "", tempmap["3-three"].Parent)
	assert.True(t, tempmap["4-vier"] != nil)
	assert.Equal(t, "", tempmap["4-vier"].Parent)

}

func TestPruneRevsSingleBranch(t *testing.T) {

	numRevs := 100

	revTree := getMultiBranchTestRevtree1(numRevs, 0, []BranchSpec{})

	maxDepth := uint32(20)
	expectedNumPruned := numRevs - int(maxDepth)

	numPruned, _ := revTree.pruneRevisions(maxDepth, "")
	assert.Equal(t, expectedNumPruned, numPruned)

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

	assert.Equal(t, int(maxDepth), revTree.LongestBranch())

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
	assert.Equal(t, 0, revTree.FindLongestTombstonedBranch())

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
	assert.Equal(t, 1, len(tombstonedLeaves))
	tombstonedLeaf := tombstonedLeaves[0]

	tombstonedBranch, err := revTree.getHistory(tombstonedLeaf)
	assert.True(t, err == nil)
	assert.Equal(t, int(maxDepth), len(tombstonedBranch))

	// The generation of the longest deleted branch is 97:
	// 1 unconflictedBranchNumRevs
	// +
	// 4 revs in branchspec
	// +
	// 1 extra rev in branchspec since LastRevisionIsTombstone (that variable name is misleading)
	expectedGenLongestTSd := 6
	assert.Equal(t, expectedGenLongestTSd, revTree.FindLongestTombstonedBranch())

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

	assert.Equal(t, expectedGenerationShortestNonTombstonedBranch, generationShortestNonTombstonedBranch)

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

	assert.Equal(t, expectedGenerationLongestTombstonedBranch, generationLongestTombstonedBranch)

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
	t.Logf("numPruned: %v", numPruned)
	t.Logf("LongestBranch: %v", revTree.LongestBranch())

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

	assert.Equal(t, expectedNumPruned, numPruned)

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

	if dumpRevTreeDotFiles {
		err := ioutil.WriteFile("/tmp/TestPruneDisconnectedRevTreeWithLongWinningBranch_initial.dot", []byte(revTree.RenderGraphvizDot()), 0666)
		require.NoError(t, err)
	}

	maxDepth := uint32(7)

	revTree.pruneRevisions(maxDepth, "")

	if dumpRevTreeDotFiles {
		err := ioutil.WriteFile("/tmp/TestPruneDisconnectedRevTreeWithLongWinningBranch_pruned1.dot", []byte(revTree.RenderGraphvizDot()), 0666)
		require.NoError(t, err)
	}

	winningBranchStartRev := fmt.Sprintf("%d-%s", 16, "winning")

	// Add revs to winning branch
	addRevs(
		revTree,
		winningBranchStartRev,
		10,
		"winning",
	)

	if dumpRevTreeDotFiles {
		err := ioutil.WriteFile("/tmp/TestPruneDisconnectedRevTreeWithLongWinningBranch_add_winning_revs.dot", []byte(revTree.RenderGraphvizDot()), 0666)
		require.NoError(t, err)
	}

	revTree.pruneRevisions(maxDepth, "")

	if dumpRevTreeDotFiles {
		err := ioutil.WriteFile("/tmp/TestPruneDisconnectedRevTreeWithLongWinningBranch_pruned_final.dot", []byte(revTree.RenderGraphvizDot()), 0666)
		require.NoError(t, err)
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
		unmarshalErr := body.Unmarshal([]byte(c.json))
		assert.NoError(t, unmarshalErr, "base JSON in test case")
		ids := ParseRevisions(body)
		assert.Equal(t, c.ids, ids)
	}
}

func BenchmarkEncodeRevisions(b *testing.B) {
	tests := []struct {
		name  string
		input []string
	}{
		{
			name:  "empty",
			input: []string{},
		},
		{
			name:  "1",
			input: []string{"1-foo"},
		},
		{
			name:  "3",
			input: []string{"2-bar", "1-foo"},
		},
		{
			name:  "26",
			input: []string{"26-z", "25-y", "24-x", "23-w", "22-v", "21-u", "20-t", "19-s", "18-r", "17-q", "16-p", "15-o", "14-n", "13-m", "12-l", "11-k", "10-j", "9-i", "8-h", "7-g", "6-f", "5-e", "4-d", "3-c", "2-b", "1-a"},
		},
		{
			name: "1000",
			input: []string{
				"1001-abcdef", "1000-abcdef", "999-abcdef", "998-abcdef", "997-abcdef", "996-abcdef", "995-abcdef", "994-abcdef", "993-abcdef", "992-abcdef", "991-abcdef", "990-abcdef", "989-abcdef", "988-abcdef", "987-abcdef", "986-abcdef", "985-abcdef", "984-abcdef", "983-abcdef", "982-abcdef", "981-abcdef", "980-abcdef", "979-abcdef", "978-abcdef", "977-abcdef", "976-abcdef", "975-abcdef", "974-abcdef", "973-abcdef", "972-abcdef", "971-abcdef", "970-abcdef", "969-abcdef", "968-abcdef", "967-abcdef", "966-abcdef", "965-abcdef", "964-abcdef", "963-abcdef", "962-abcdef", "961-abcdef", "960-abcdef", "959-abcdef", "958-abcdef", "957-abcdef", "956-abcdef", "955-abcdef", "954-abcdef", "953-abcdef", "952-abcdef", "951-abcdef", "950-abcdef", "949-abcdef", "948-abcdef", "947-abcdef", "946-abcdef", "945-abcdef", "944-abcdef", "943-abcdef", "942-abcdef", "941-abcdef", "940-abcdef", "939-abcdef", "938-abcdef", "937-abcdef", "936-abcdef", "935-abcdef", "934-abcdef", "933-abcdef", "932-abcdef", "931-abcdef", "930-abcdef", "929-abcdef", "928-abcdef", "927-abcdef", "926-abcdef", "925-abcdef", "924-abcdef", "923-abcdef", "922-abcdef", "921-abcdef", "920-abcdef", "919-abcdef", "918-abcdef", "917-abcdef", "916-abcdef", "915-abcdef", "914-abcdef", "913-abcdef", "912-abcdef", "911-abcdef", "910-abcdef", "909-abcdef", "908-abcdef", "907-abcdef", "906-abcdef", "905-abcdef", "904-abcdef", "903-abcdef", "902-abcdef", "901-abcdef", "900-abcdef", "899-abcdef", "898-abcdef", "897-abcdef", "896-abcdef", "895-abcdef", "894-abcdef", "893-abcdef", "892-abcdef", "891-abcdef", "890-abcdef", "889-abcdef", "888-abcdef", "887-abcdef", "886-abcdef", "885-abcdef", "884-abcdef", "883-abcdef", "882-abcdef", "881-abcdef", "880-abcdef", "879-abcdef", "878-abcdef", "877-abcdef", "876-abcdef", "875-abcdef", "874-abcdef", "873-abcdef", "872-abcdef", "871-abcdef", "870-abcdef", "869-abcdef", "868-abcdef", "867-abcdef", "866-abcdef", "865-abcdef", "864-abcdef", "863-abcdef", "862-abcdef", "861-abcdef", "860-abcdef", "859-abcdef", "858-abcdef", "857-abcdef", "856-abcdef", "855-abcdef", "854-abcdef", "853-abcdef", "852-abcdef", "851-abcdef", "850-abcdef", "849-abcdef", "848-abcdef", "847-abcdef", "846-abcdef", "845-abcdef", "844-abcdef", "843-abcdef", "842-abcdef", "841-abcdef", "840-abcdef", "839-abcdef", "838-abcdef", "837-abcdef", "836-abcdef", "835-abcdef", "834-abcdef", "833-abcdef", "832-abcdef", "831-abcdef", "830-abcdef", "829-abcdef", "828-abcdef", "827-abcdef", "826-abcdef", "825-abcdef", "824-abcdef", "823-abcdef", "822-abcdef", "821-abcdef", "820-abcdef", "819-abcdef", "818-abcdef", "817-abcdef", "816-abcdef", "815-abcdef", "814-abcdef", "813-abcdef", "812-abcdef", "811-abcdef", "810-abcdef", "809-abcdef", "808-abcdef", "807-abcdef", "806-abcdef", "805-abcdef", "804-abcdef", "803-abcdef", "802-abcdef", "801-abcdef", "800-abcdef", "799-abcdef", "798-abcdef", "797-abcdef", "796-abcdef", "795-abcdef", "794-abcdef", "793-abcdef", "792-abcdef", "791-abcdef", "790-abcdef", "789-abcdef", "788-abcdef", "787-abcdef", "786-abcdef", "785-abcdef", "784-abcdef", "783-abcdef", "782-abcdef", "781-abcdef", "780-abcdef", "779-abcdef", "778-abcdef", "777-abcdef", "776-abcdef", "775-abcdef", "774-abcdef", "773-abcdef", "772-abcdef", "771-abcdef", "770-abcdef", "769-abcdef", "768-abcdef", "767-abcdef", "766-abcdef", "765-abcdef", "764-abcdef", "763-abcdef", "762-abcdef", "761-abcdef", "760-abcdef", "759-abcdef", "758-abcdef", "757-abcdef", "756-abcdef", "755-abcdef", "754-abcdef", "753-abcdef", "752-abcdef", "751-abcdef", "750-abcdef", "749-abcdef", "748-abcdef", "747-abcdef", "746-abcdef", "745-abcdef", "744-abcdef", "743-abcdef", "742-abcdef", "741-abcdef", "740-abcdef", "739-abcdef", "738-abcdef", "737-abcdef", "736-abcdef", "735-abcdef", "734-abcdef", "733-abcdef", "732-abcdef", "731-abcdef", "730-abcdef", "729-abcdef", "728-abcdef", "727-abcdef", "726-abcdef", "725-abcdef", "724-abcdef", "723-abcdef", "722-abcdef", "721-abcdef", "720-abcdef", "719-abcdef", "718-abcdef", "717-abcdef", "716-abcdef", "715-abcdef", "714-abcdef", "713-abcdef", "712-abcdef", "711-abcdef", "710-abcdef", "709-abcdef", "708-abcdef", "707-abcdef", "706-abcdef", "705-abcdef", "704-abcdef", "703-abcdef", "702-abcdef", "701-abcdef", "700-abcdef", "699-abcdef", "698-abcdef", "697-abcdef", "696-abcdef", "695-abcdef", "694-abcdef", "693-abcdef", "692-abcdef", "691-abcdef", "690-abcdef", "689-abcdef", "688-abcdef", "687-abcdef", "686-abcdef", "685-abcdef", "684-abcdef", "683-abcdef", "682-abcdef", "681-abcdef", "680-abcdef", "679-abcdef", "678-abcdef", "677-abcdef", "676-abcdef", "675-abcdef", "674-abcdef", "673-abcdef", "672-abcdef", "671-abcdef", "670-abcdef", "669-abcdef", "668-abcdef", "667-abcdef", "666-abcdef", "665-abcdef", "664-abcdef", "663-abcdef", "662-abcdef", "661-abcdef", "660-abcdef", "659-abcdef", "658-abcdef", "657-abcdef", "656-abcdef", "655-abcdef", "654-abcdef", "653-abcdef", "652-abcdef", "651-abcdef", "650-abcdef", "649-abcdef", "648-abcdef", "647-abcdef", "646-abcdef", "645-abcdef", "644-abcdef", "643-abcdef", "642-abcdef", "641-abcdef", "640-abcdef", "639-abcdef", "638-abcdef", "637-abcdef", "636-abcdef", "635-abcdef", "634-abcdef", "633-abcdef", "632-abcdef", "631-abcdef", "630-abcdef", "629-abcdef", "628-abcdef", "627-abcdef", "626-abcdef", "625-abcdef", "624-abcdef", "623-abcdef", "622-abcdef", "621-abcdef", "620-abcdef", "619-abcdef", "618-abcdef", "617-abcdef", "616-abcdef", "615-abcdef", "614-abcdef", "613-abcdef", "612-abcdef", "611-abcdef", "610-abcdef", "609-abcdef", "608-abcdef", "607-abcdef", "606-abcdef", "605-abcdef", "604-abcdef", "603-abcdef", "602-abcdef", "601-abcdef", "600-abcdef", "599-abcdef", "598-abcdef", "597-abcdef", "596-abcdef", "595-abcdef", "594-abcdef", "593-abcdef", "592-abcdef", "591-abcdef", "590-abcdef", "589-abcdef", "588-abcdef", "587-abcdef", "586-abcdef", "585-abcdef", "584-abcdef", "583-abcdef", "582-abcdef", "581-abcdef", "580-abcdef", "579-abcdef", "578-abcdef", "577-abcdef", "576-abcdef", "575-abcdef", "574-abcdef", "573-abcdef", "572-abcdef", "571-abcdef", "570-abcdef", "569-abcdef", "568-abcdef", "567-abcdef", "566-abcdef", "565-abcdef", "564-abcdef", "563-abcdef", "562-abcdef", "561-abcdef", "560-abcdef", "559-abcdef", "558-abcdef", "557-abcdef", "556-abcdef", "555-abcdef", "554-abcdef", "553-abcdef", "552-abcdef", "551-abcdef", "550-abcdef", "549-abcdef", "548-abcdef", "547-abcdef", "546-abcdef", "545-abcdef", "544-abcdef", "543-abcdef", "542-abcdef", "541-abcdef", "540-abcdef", "539-abcdef", "538-abcdef", "537-abcdef", "536-abcdef", "535-abcdef", "534-abcdef", "533-abcdef", "532-abcdef", "531-abcdef", "530-abcdef", "529-abcdef", "528-abcdef", "527-abcdef", "526-abcdef", "525-abcdef", "524-abcdef", "523-abcdef", "522-abcdef", "521-abcdef", "520-abcdef", "519-abcdef", "518-abcdef", "517-abcdef", "516-abcdef", "515-abcdef", "514-abcdef", "513-abcdef", "512-abcdef", "511-abcdef", "510-abcdef", "509-abcdef", "508-abcdef", "507-abcdef", "506-abcdef", "505-abcdef", "504-abcdef", "503-abcdef", "502-abcdef", "501-abcdef", "500-abcdef", "499-abcdef", "498-abcdef", "497-abcdef", "496-abcdef", "495-abcdef", "494-abcdef", "493-abcdef", "492-abcdef", "491-abcdef", "490-abcdef", "489-abcdef", "488-abcdef", "487-abcdef", "486-abcdef", "485-abcdef", "484-abcdef", "483-abcdef", "482-abcdef", "481-abcdef", "480-abcdef", "479-abcdef", "478-abcdef", "477-abcdef", "476-abcdef", "475-abcdef", "474-abcdef", "473-abcdef", "472-abcdef", "471-abcdef", "470-abcdef", "469-abcdef", "468-abcdef", "467-abcdef", "466-abcdef", "465-abcdef", "464-abcdef", "463-abcdef", "462-abcdef", "461-abcdef", "460-abcdef", "459-abcdef", "458-abcdef", "457-abcdef", "456-abcdef", "455-abcdef", "454-abcdef", "453-abcdef", "452-abcdef", "451-abcdef", "450-abcdef", "449-abcdef", "448-abcdef", "447-abcdef", "446-abcdef", "445-abcdef", "444-abcdef", "443-abcdef", "442-abcdef", "441-abcdef", "440-abcdef", "439-abcdef", "438-abcdef", "437-abcdef", "436-abcdef", "435-abcdef", "434-abcdef", "433-abcdef", "432-abcdef", "431-abcdef", "430-abcdef", "429-abcdef", "428-abcdef", "427-abcdef", "426-abcdef", "425-abcdef", "424-abcdef", "423-abcdef", "422-abcdef", "421-abcdef", "420-abcdef", "419-abcdef", "418-abcdef", "417-abcdef", "416-abcdef", "415-abcdef", "414-abcdef", "413-abcdef", "412-abcdef", "411-abcdef", "410-abcdef", "409-abcdef", "408-abcdef", "407-abcdef", "406-abcdef", "405-abcdef", "404-abcdef", "403-abcdef", "402-abcdef", "401-abcdef", "400-abcdef", "399-abcdef", "398-abcdef", "397-abcdef", "396-abcdef", "395-abcdef", "394-abcdef", "393-abcdef", "392-abcdef", "391-abcdef", "390-abcdef", "389-abcdef", "388-abcdef", "387-abcdef", "386-abcdef", "385-abcdef", "384-abcdef", "383-abcdef", "382-abcdef", "381-abcdef", "380-abcdef", "379-abcdef", "378-abcdef", "377-abcdef", "376-abcdef", "375-abcdef", "374-abcdef", "373-abcdef", "372-abcdef", "371-abcdef", "370-abcdef", "369-abcdef", "368-abcdef", "367-abcdef", "366-abcdef", "365-abcdef", "364-abcdef", "363-abcdef", "362-abcdef", "361-abcdef", "360-abcdef", "359-abcdef", "358-abcdef", "357-abcdef", "356-abcdef", "355-abcdef", "354-abcdef", "353-abcdef", "352-abcdef", "351-abcdef", "350-abcdef", "349-abcdef", "348-abcdef", "347-abcdef", "346-abcdef", "345-abcdef", "344-abcdef", "343-abcdef", "342-abcdef", "341-abcdef", "340-abcdef", "339-abcdef", "338-abcdef", "337-abcdef", "336-abcdef", "335-abcdef", "334-abcdef", "333-abcdef", "332-abcdef", "331-abcdef", "330-abcdef", "329-abcdef", "328-abcdef", "327-abcdef", "326-abcdef", "325-abcdef", "324-abcdef", "323-abcdef", "322-abcdef", "321-abcdef", "320-abcdef", "319-abcdef", "318-abcdef", "317-abcdef", "316-abcdef", "315-abcdef", "314-abcdef", "313-abcdef", "312-abcdef", "311-abcdef", "310-abcdef", "309-abcdef", "308-abcdef", "307-abcdef", "306-abcdef", "305-abcdef", "304-abcdef", "303-abcdef", "302-abcdef", "301-abcdef", "300-abcdef", "299-abcdef", "298-abcdef", "297-abcdef", "296-abcdef", "295-abcdef", "294-abcdef", "293-abcdef", "292-abcdef", "291-abcdef", "290-abcdef", "289-abcdef", "288-abcdef", "287-abcdef", "286-abcdef", "285-abcdef", "284-abcdef", "283-abcdef", "282-abcdef", "281-abcdef", "280-abcdef", "279-abcdef", "278-abcdef", "277-abcdef", "276-abcdef", "275-abcdef", "274-abcdef", "273-abcdef", "272-abcdef", "271-abcdef", "270-abcdef", "269-abcdef", "268-abcdef", "267-abcdef", "266-abcdef", "265-abcdef", "264-abcdef", "263-abcdef", "262-abcdef", "261-abcdef", "260-abcdef", "259-abcdef", "258-abcdef", "257-abcdef", "256-abcdef", "255-abcdef", "254-abcdef", "253-abcdef", "252-abcdef", "251-abcdef", "250-abcdef", "249-abcdef", "248-abcdef", "247-abcdef", "246-abcdef", "245-abcdef", "244-abcdef", "243-abcdef", "242-abcdef", "241-abcdef", "240-abcdef", "239-abcdef", "238-abcdef", "237-abcdef", "236-abcdef", "235-abcdef", "234-abcdef", "233-abcdef", "232-abcdef", "231-abcdef", "230-abcdef", "229-abcdef", "228-abcdef", "227-abcdef", "226-abcdef", "225-abcdef", "224-abcdef", "223-abcdef", "222-abcdef", "221-abcdef", "220-abcdef", "219-abcdef", "218-abcdef", "217-abcdef", "216-abcdef", "215-abcdef", "214-abcdef", "213-abcdef", "212-abcdef", "211-abcdef", "210-abcdef", "209-abcdef", "208-abcdef", "207-abcdef", "206-abcdef", "205-abcdef", "204-abcdef", "203-abcdef", "202-abcdef", "201-abcdef", "200-abcdef", "199-abcdef", "198-abcdef", "197-abcdef", "196-abcdef", "195-abcdef", "194-abcdef", "193-abcdef", "192-abcdef", "191-abcdef", "190-abcdef", "189-abcdef", "188-abcdef", "187-abcdef", "186-abcdef", "185-abcdef", "184-abcdef", "183-abcdef", "182-abcdef", "181-abcdef", "180-abcdef", "179-abcdef", "178-abcdef", "177-abcdef", "176-abcdef", "175-abcdef", "174-abcdef", "173-abcdef", "172-abcdef", "171-abcdef", "170-abcdef", "169-abcdef", "168-abcdef", "167-abcdef", "166-abcdef", "165-abcdef", "164-abcdef", "163-abcdef", "162-abcdef", "161-abcdef", "160-abcdef", "159-abcdef", "158-abcdef", "157-abcdef", "156-abcdef", "155-abcdef", "154-abcdef", "153-abcdef", "152-abcdef", "151-abcdef", "150-abcdef", "149-abcdef", "148-abcdef", "147-abcdef", "146-abcdef", "145-abcdef", "144-abcdef", "143-abcdef", "142-abcdef", "141-abcdef", "140-abcdef", "139-abcdef", "138-abcdef", "137-abcdef", "136-abcdef", "135-abcdef", "134-abcdef", "133-abcdef", "132-abcdef", "131-abcdef", "130-abcdef", "129-abcdef", "128-abcdef", "127-abcdef", "126-abcdef", "125-abcdef", "124-abcdef", "123-abcdef", "122-abcdef", "121-abcdef", "120-abcdef", "119-abcdef", "118-abcdef", "117-abcdef", "116-abcdef", "115-abcdef", "114-abcdef", "113-abcdef", "112-abcdef", "111-abcdef", "110-abcdef", "109-abcdef", "108-abcdef", "107-abcdef", "106-abcdef", "105-abcdef", "104-abcdef", "103-abcdef", "102-abcdef", "101-abcdef", "100-abcdef", "99-abcdef", "98-abcdef", "97-abcdef", "96-abcdef", "95-abcdef", "94-abcdef", "93-abcdef", "92-abcdef", "91-abcdef", "90-abcdef", "89-abcdef", "88-abcdef", "87-abcdef", "86-abcdef", "85-abcdef", "84-abcdef", "83-abcdef", "82-abcdef", "81-abcdef", "80-abcdef", "79-abcdef", "78-abcdef", "77-abcdef", "76-abcdef", "75-abcdef", "74-abcdef", "73-abcdef", "72-abcdef", "71-abcdef", "70-abcdef", "69-abcdef", "68-abcdef", "67-abcdef", "66-abcdef", "65-abcdef", "64-abcdef", "63-abcdef", "62-abcdef", "61-abcdef", "60-abcdef", "59-abcdef", "58-abcdef", "57-abcdef", "56-abcdef", "55-abcdef", "54-abcdef", "53-abcdef", "52-abcdef", "51-abcdef", "50-abcdef", "49-abcdef", "48-abcdef", "47-abcdef", "46-abcdef", "45-abcdef", "44-abcdef", "43-abcdef", "42-abcdef", "41-abcdef", "40-abcdef", "39-abcdef", "38-abcdef", "37-abcdef", "36-abcdef", "35-abcdef", "34-abcdef", "33-abcdef", "32-abcdef", "31-abcdef", "30-abcdef", "29-abcdef", "28-abcdef", "27-abcdef", "26-abcdef", "25-abcdef", "24-abcdef", "23-abcdef", "22-abcdef", "21-abcdef", "20-abcdef", "19-abcdef", "18-abcdef", "17-abcdef", "16-abcdef", "15-abcdef", "14-abcdef", "13-abcdef", "12-abcdef", "11-abcdef", "10-abcdef", "9-abcdef", "8-abcdef", "7-abcdef", "6-abcdef", "5-abcdef", "4-abcdef", "3-abcdef", "2-abcdef", "1-abcdef",
			},
		},
	}

	for _, test := range tests {
		docID := b.Name() + "-" + test.name
		b.Run(test.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = encodeRevisions(docID, test.input)
			}
		})
	}
}

func TestEncodeRevisions(t *testing.T) {
	encoded := encodeRevisions(t.Name(), []string{"5-huey", "4-dewey", "3-louie"})
	assert.Equal(t, Revisions{RevisionsStart: 5, RevisionsIds: []string{"huey", "dewey", "louie"}}, encoded)
}

func TestEncodeRevisionsGap(t *testing.T) {
	encoded := encodeRevisions(t.Name(), []string{"5-huey", "3-louie"})
	assert.Equal(t, Revisions{RevisionsStart: 5, RevisionsIds: []string{"huey", "louie"}}, encoded)
}

func TestEncodeRevisionsZero(t *testing.T) {
	encoded := encodeRevisions(t.Name(), []string{"1-foo", "0-bar"})
	assert.Equal(t, Revisions{RevisionsStart: 1, RevisionsIds: []string{"foo", ""}}, encoded)
}

func TestTrimEncodedRevisionsToAncestor(t *testing.T) {

	encoded := encodeRevisions(t.Name(), []string{"5-huey", "4-dewey", "3-louie", "2-screwy"})

	result, trimmedRevs := trimEncodedRevisionsToAncestor(encoded, []string{"3-walter", "17-gretchen", "1-fooey"}, 1000)
	assert.True(t, result)
	assert.Equal(t, Revisions{RevisionsStart: 5, RevisionsIds: []string{"huey", "dewey", "louie", "screwy"}}, trimmedRevs)

	result, trimmedRevs = trimEncodedRevisionsToAncestor(trimmedRevs, []string{"3-walter", "3-louie", "1-fooey"}, 2)
	assert.True(t, result)
	assert.Equal(t, Revisions{RevisionsStart: 5, RevisionsIds: []string{"huey", "dewey", "louie"}}, trimmedRevs)

	result, trimmedRevs = trimEncodedRevisionsToAncestor(trimmedRevs, []string{"3-walter", "3-louie", "1-fooey"}, 3)
	assert.True(t, result)
	assert.Equal(t, Revisions{RevisionsStart: 5, RevisionsIds: []string{"huey", "dewey", "louie"}}, trimmedRevs)

	result, trimmedRevs = trimEncodedRevisionsToAncestor(trimmedRevs, []string{"3-walter", "3-louie", "5-huey"}, 3)
	assert.True(t, result)
	assert.Equal(t, Revisions{RevisionsStart: 5, RevisionsIds: []string{"huey"}}, trimmedRevs)

	// Check maxLength with no ancestors:
	encoded = encodeRevisions(t.Name(), []string{"5-huey", "4-dewey", "3-louie", "2-screwy"})

	result, trimmedRevs = trimEncodedRevisionsToAncestor(encoded, nil, 6)
	assert.True(t, result)
	assert.Equal(t, Revisions{RevisionsStart: 5, RevisionsIds: []string{"huey", "dewey", "louie", "screwy"}}, trimmedRevs)

	result, trimmedRevs = trimEncodedRevisionsToAncestor(trimmedRevs, nil, 2)
	assert.True(t, result)
	assert.Equal(t, Revisions{RevisionsStart: 5, RevisionsIds: []string{"huey", "dewey"}}, trimmedRevs)
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

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCRUD)

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
	err := addAndGet(t, revTree, "1-foo", "", nonTombstone)
	assert.NoError(t, err, "Error adding revision 1-foo to tree")

	// Add several entries (2-foo to 5-foo)
	for generation := 2; generation <= 5; generation++ {
		revID := fmt.Sprintf("%d-foo", generation)
		parentRevID := fmt.Sprintf("%d-foo", generation-1)
		err := addAndGet(t, revTree, revID, parentRevID, nonTombstone)
		assert.NoError(t, err, fmt.Sprintf("Error adding revision %s to tree", revID))
	}

	// Add tombstone children of 3-foo and 4-foo

	err = addAndGet(t, revTree, "4-bar", "3-foo", nonTombstone)
	err = addAndGet(t, revTree, "5-bar", "4-bar", tombstone)
	assert.NoError(t, err, "Error adding tombstone 4-bar to tree")

	/*
		// Add a second branch as a child of 2-foo.
		err = addAndGet(revTree, "3-bar", "2-foo", nonTombstone)
		assert.NoError(t, err, "Error adding revision 3-bar to tree")

		// Tombstone the second branch
		err = addAndGet(revTree, "4-bar", "3-bar", tombstone)
		assert.NoError(t, err, "Error adding tombstone 4-bar to tree")
	*/

	// Add a another tombstoned branch as a child of 5-foo.  This will ensure that 2-foo doesn't get pruned
	// until the first tombstone branch is deleted.
	/*
		err = addAndGet(revTree, "6-bar2", "5-foo", tombstone)
		assert.NoError(t, err, "Error adding tombstone 6-bar2 to tree")
	*/

	log.Printf("Tree before adding to main branch: [[%s]]", revTree.RenderGraphvizDot())

	// Keep adding to the main branch without pruning.  Simulates old pruning algorithm,
	// which maintained rev history due to tombstone branch
	for generation := 6; generation <= 15; generation++ {
		revID := fmt.Sprintf("%d-foo", generation)
		parentRevID := fmt.Sprintf("%d-foo", generation-1)
		_, err := addPruneAndGet(revTree, revID, parentRevID, revBody, revsLimit, nonTombstone)
		assert.NoError(t, err, fmt.Sprintf("Error adding revision %s to tree", revID))

		keepAliveRevID := fmt.Sprintf("%d-keep", generation)
		_, err = addPruneAndGet(revTree, keepAliveRevID, parentRevID, revBody, revsLimit, tombstone)
		assert.NoError(t, err, fmt.Sprintf("Error adding revision %s to tree", revID))

		// The act of marshalling the rev tree and then unmarshalling back into a revtree data structure
		// causes the issue.
		log.Printf("Tree pre-marshal: [[%s]]", revTree.RenderGraphvizDot())
		treeBytes, marshalErr := revTree.MarshalJSON()
		assert.NoError(t, marshalErr, fmt.Sprintf("Error marshalling tree: %v", marshalErr))
		revTree = RevTree{}
		unmarshalErr := revTree.UnmarshalJSON(treeBytes)
		assert.NoError(t, unmarshalErr, fmt.Sprintf("Error unmarshalling tree: %v", unmarshalErr))
	}

}

func addAndGet(t *testing.T, revTree RevTree, revID string, parentRevID string, isTombstone bool) error {

	revBody := []byte(`{"foo":"bar"}`)
	err := revTree.addRevision("foobar", RevInfo{
		ID:      revID,
		Parent:  parentRevID,
		Body:    revBody,
		Deleted: isTombstone,
	})
	require.NoError(t, err)
	history, err := revTree.getHistory(revID)
	log.Printf("addAndGet.  Tree length: %d.  History for new rev: %v", len(revTree), history)
	return err
}

// TestPruneRevisionsWithDisconnectedTombstones ensures that disconnected tombstoned branches are correctly pruned away. Reproduces CBG-1076.
func TestPruneRevisionsWithDisconnected(t *testing.T) {
	revTree := RevTree{
		"100-abc": {ID: "100-abc"},
		"101-def": {ID: "101-def", Parent: "100-abc", Deleted: true},
		"101-abc": {ID: "101-abc", Parent: "100-abc"},
		"102-abc": {ID: "102-abc", Parent: "101-abc"},
		"103-def": {ID: "103-def", Parent: "102-abc", Deleted: true},
		"103-abc": {ID: "103-abc", Parent: "102-abc"},
		"104-abc": {ID: "104-abc", Parent: "103-abc"},
		"105-abc": {ID: "105-abc", Parent: "104-abc"},
		"106-def": {ID: "106-def", Parent: "105-abc", Deleted: true},
		"106-abc": {ID: "106-abc", Parent: "105-abc"},
		"107-abc": {ID: "107-abc", Parent: "106-abc"},

		"1-abc": {ID: "1-abc"},
		"2-abc": {ID: "2-abc", Parent: "1-abc"},
		"3-abc": {ID: "3-abc", Parent: "2-abc"},
		"4-abc": {ID: "4-abc", Parent: "3-abc", Deleted: true},

		"70-abc": {ID: "70-abc"},
		"71-abc": {ID: "71-abc", Parent: "70-abc"},
		"72-abc": {ID: "72-abc", Parent: "71-abc"},
		"73-abc": {ID: "73-abc", Parent: "72-abc", Deleted: true},
	}

	prunedCount, _ := revTree.pruneRevisions(4, "")
	assert.Equal(t, 10, prunedCount)

	remainingKeys := make([]string, 0, len(revTree))
	for key := range revTree {
		remainingKeys = append(remainingKeys, key)
	}
	sort.Strings(remainingKeys)

	assert.Equal(t, []string{"101-abc", "102-abc", "103-abc", "103-def", "104-abc", "105-abc", "106-abc", "106-def", "107-abc"}, remainingKeys)
}

func addPruneAndGet(revTree RevTree, revID string, parentRevID string, revBody []byte, revsLimit uint32, tombstone bool) (numPruned int, err error) {
	_ = revTree.addRevision("doc", RevInfo{
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

func getHistoryWithTimeout(rawDoc *Document, revId string, timeout time.Duration) (history []string, err error) {

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

// ////// BENCHMARK:

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

func BenchmarkRevtreeUnmarshal(b *testing.B) {
	doc := Document{
		ID: "docid",
	}
	err := base.JSONUnmarshal([]byte(largeRevTree), &doc)
	assert.NoError(b, err)
	treeJson, err := base.JSONMarshal(doc.History)
	assert.NoError(b, err)

	b.ResetTimer()
	b.Run("Marshal into revTree", func(b *testing.B) {
		revTree := RevTree{}
		for i := 0; i < b.N; i++ {
			_ = base.JSONUnmarshal(treeJson, &revTree)
		}
	})

	b.Run("Marshal into revTreeList", func(b *testing.B) {
		revTree := revTreeList{}
		for i := 0; i < b.N; i++ {
			_ = base.JSONUnmarshal(treeJson, &revTree)
		}
	})
}

func addRevs(revTree RevTree, startingParentRevId string, numRevs int, revDigest string) {

	docSizeBytes := 1024 * 5
	body := createBodyContentAsMapWithSize(docSizeBytes)
	bodyBytes, err := base.JSONMarshal(body)
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
		_ = revTree.addRevision("testdoc", revInfo)

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
