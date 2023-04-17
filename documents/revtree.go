//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package documents

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"math"

	"github.com/couchbase/sync_gateway/base"
)

type RevKey string

// Information about a single revision.
type RevInfo struct {
	ID             string   // Revision ID
	Parent         *RevInfo // Parent revision; nil if root
	BodyKey        string   // Used when revision body stored externally (doc key used for external storage)
	Body           []byte   // Used when revision body stored inline (stores bodies)
	Channels       base.Set // Channels this revision is in
	depth          uint32   // Depth in tree; only set by computeDepthsAndFindLeaves()
	Deleted        bool     // True if this is a tombstone
	HasAttachments bool     // True if document has attachments
}

func (rev *RevInfo) ParentID() (id string) {
	if rev.Parent != nil {
		return rev.Parent.ID
	}
	return ""
}

func (rev *RevInfo) IsRoot() bool {
	return rev.Parent == nil
}

// Maps revision IDs to RevInfo; content of a RevTree
type RevMap map[string]*RevInfo

// A revision tree maps each revision ID to its RevInfo.
type RevTree struct {
	revs     RevMap // maps revID -> RevInfo
	jsonForm []byte // Marshaled JSON, or nil
	parseErr error  // Error from unmarshaling JSON
}

// Unmarshals the tree from JSON if necessary. Must be called before any use.
func (tree *RevTree) init() {
	if tree.revs == nil {
		if tree.jsonForm != nil {
			tree.parseErr = tree.lazyLoadJSON()
			if tree.parseErr != nil {
				base.ErrorfCtx(context.TODO(), "Error unmarshaling RevTree: %s", tree.parseErr)
				tree.revs = make(RevMap)
			}
		} else {
			tree.revs = make(RevMap)
		}
	}
}

// Must be called (directly or transitively) by any method that modifies the tree or a RevInfo,
// or that returns the tree or a *RevInfo to the caller, since the caller might change them.
// (Clearing jsonForm prevents MarshalJSON from writing stale JSON after a modification.)
// Note: Revs(), Get() and GetInfo() all call this, so callers of those don't have to.
func (tree *RevTree) initMutable() {
	tree.init()
	tree.jsonForm = nil
}

func (tree *RevTree) Validate() error {
	tree.init()
	return tree.parseErr
}

// The map from revIDs to RevInfos.
func (tree *RevTree) Revs() RevMap {
	tree.initMutable()
	return tree.revs
}

// The number of revisions in the tree.
func (tree *RevTree) RevCount() int {
	tree.init()
	return len(tree.revs)
}

// Returns true if this RevID exists in the tree (has a RevInfo).
func (tree *RevTree) Contains(revid string) bool {
	tree.init()
	_, exists := tree.revs[revid]
	return exists
}

// Returns the *RevInfo for a revision ID, or nil if it's not found
func (tree *RevTree) Get(revid string) *RevInfo {
	return tree.Revs()[revid]
}

// Returns the RevInfo for a revision ID, or an error if it's not found
func (tree *RevTree) GetInfo(revid string) (info *RevInfo, err error) {
	info = tree.Get(revid)
	if info == nil {
		err = errors.New("RevTree.GetInfo can't find rev: " + revid)
	}
	return
}

// Returns a RevID's parent RevID, or "" if it doesn't exist or has no parent.
func (tree *RevTree) ParentOf(revid string) string {
	tree.init()
	if info := tree.revs[revid]; info != nil {
		return info.ParentID()
	}
	return ""
}

func (tree *RevTree) ContainsCycles() bool {
	containsCycles := false
	for _, leafRevision := range tree.GetLeaves() {
		_, revHistoryErr := tree.GetHistory(leafRevision)
		if revHistoryErr != nil {
			containsCycles = true
		}
	}
	return containsCycles
}

// Repair rev trees that have cycles introduced by SG Issue #2847
func (tree *RevTree) RepairCycles() (err error) {

	// This function will be called back for every leaf node in tree
	leafProcessor := func(leaf *RevInfo) {

		// Walk up the tree until we find a root, and append each node
		node := leaf
		if node.IsRoot() {
			return
		}

		for {

			if node.ParentGenGTENodeGen() {
				base.InfofCtx(context.Background(), base.KeyCRUD, "Node %+v detected to have invalid parent rev (parent generation larger than node generation).  Repairing by designating as a root node.", base.UD(node))
				node.Parent = nil
				tree.jsonForm = nil // mutated
				break
			}

			node = node.Parent

			// Reached a root, we're done -- there's no need
			// to call appendNodeToResult() on the root, since
			// the child of the root will have already added a node
			// pointing to the root.
			if node.IsRoot() {
				break
			}

		}
	}

	// Iterate over leaves
	tree.forEachLeafRO(leafProcessor)

	return nil
}

// Detect situations like:
//
//	node: &{ID:10-684759c169c75629d02b90fe10b56925 Parent:184-a6b3f72a2bc1f988bfb720fec8db3a1d Deleted:fa...
//
// where the parent generation is *higher* than the node generation, which is never a valid scenario.
// Likewise, detect situations where the parent generation is equal to the node generation, which is also invalid.
func (node RevInfo) ParentGenGTENodeGen() bool {
	return GenOfRevID(node.ParentID()) >= GenOfRevID(node.ID)
}

// Returns the parent ID of a revid, or "" if the revid is a root or is not found.
func (tree *RevTree) GetParent(revid string) string {
	tree.init()
	if info := tree.revs[revid]; info == nil {
		return ""
	} else {
		return info.ParentID()
	}
}

// Returns the leaf revision IDs (those that have no children.)
func (tree *RevTree) GetLeaves() []string {
	return tree.GetLeavesFiltered(func(revId string) bool { return true })
}

func (tree *RevTree) GetLeavesFiltered(filter func(revId string) bool) []string {
	tree.init()
	isParent := map[string]bool{}
	for _, info := range tree.revs {
		isParent[info.ParentID()] = true
	}
	leaves := make([]string, 0, len(tree.revs)-len(isParent)+1)
	for revid := range tree.revs {
		if !isParent[revid] {
			if filter(revid) {
				leaves = append(leaves, revid)
			}
		}
	}
	return leaves

}

// Same as ForEachLeaf, but callback must not modify the RevInfo.
func (tree *RevTree) forEachLeafRO(callback func(*RevInfo)) {
	tree.init()
	isParent := map[string]bool{}
	for _, info := range tree.revs {
		isParent[info.ParentID()] = true
	}
	for revid, info := range tree.revs {
		if !isParent[revid] {
			callback(info)
		}
	}
}

func (tree *RevTree) ForEachLeaf(callback func(*RevInfo)) {
	tree.initMutable() // callback might mutate the info
	tree.forEachLeafRO(callback)
}

func (tree *RevTree) IsLeaf(revid string) bool {
	tree.init()
	rev := tree.revs[revid]
	if rev == nil {
		return false
	}
	for _, info := range tree.revs {
		if info.Parent == rev {
			return false
		}
	}
	return true
}

// Finds the "winning" revision, the one that should be treated as the default.
// This is the leaf revision whose (!deleted, generation, hash) tuple compares the highest.
func (tree *RevTree) WinningRevision() (winner string, branched bool, inConflict bool) {
	winnerExists := false
	leafCount := 0
	activeLeafCount := 0
	tree.forEachLeafRO(func(info *RevInfo) {
		exists := !info.Deleted
		leafCount++
		if exists {
			activeLeafCount++
		}
		if (exists && !winnerExists) ||
			((exists == winnerExists) && CompareRevIDs(info.ID, winner) > 0) {
			winner = info.ID
			winnerExists = exists
		}
	})
	branched = (leafCount > 1)
	inConflict = (activeLeafCount > 1)
	return
}

// Given a revision and a set of possible ancestors, finds the one that is the most recent
// ancestor of the revision; if none are ancestors, returns "".
func (tree *RevTree) FindAncestorFromSet(revid string, ancestors []string) string {
	// OPT: This is slow...
	tree.init()
	for revid != "" {
		for _, a := range ancestors {
			if a == revid {
				return a
			}
		}
		info := tree.revs[revid]
		if info == nil {
			break
		}
		revid = info.ParentID()
	}
	return ""
}

// Records a revision in a RevTree.
func (tree *RevTree) AddRevision(docid string, parentID string, info RevInfo) (err error) {
	tree.initMutable()
	revid := info.ID
	if revid == "" {
		err = errors.New(fmt.Sprintf("doc: %v, RevTree AddRevision, empty revid is illegal", docid))
		return
	}
	if tree.Contains(revid) {
		err = errors.New(fmt.Sprintf("doc: %v, RevTree AddRevision, already contains rev %q", docid, revid))
		return
	}
	if info.Parent != nil {
		return fmt.Errorf("RevTree.AddRevision: New RevInfo's Parent must be nil")
	}
	if parentID != "" {
		info.Parent = tree.revs[parentID]
		if info.Parent == nil {
			log.Printf("TREE = %+v", tree.revs) //TEMP
			err = errors.New(fmt.Sprintf("doc: %v, RevTree AddRevision, parent id %q is missing", docid, parentID))
			return
		}
	}
	tree.revs[revid] = &info
	return nil
}

func (tree *RevTree) GetRevisionBody(revid string, loader RevLoaderFunc) ([]byte, bool) {
	tree.init()
	if revid == "" {
		// TODO: CBG-1948
		panic("Illegal empty revision ID")
	}
	info := tree.revs[revid]
	if info == nil {
		return nil, false
	}

	// If we don't have a Body in memory and there's a BodyKey present, attempt to retrieve using the loader
	if info.Body == nil && info.BodyKey != "" {
		if info.BodyKey != "" {
			var err error
			info.Body, err = loader(info.BodyKey)
			if err != nil {
				return nil, false
			}
		}
	}

	// Handling for data affected by https://github.com/couchbase/sync_gateway/issues/3692
	// In that scenario, the revision history includes an entry that should have been moved to a transient
	// backup and removed from the rev tree, and the revtree version was modified to include a leading non-JSON byte.
	// We ignore these rev history entries, meaning that callers will see the same view of the data
	// as if the transient backup had expired. We are not attempting to repair the rev tree, as reclaiming storage
	// is a much lower priority than avoiding write errors, and want to avoid introducing additional conflict scenarios.
	// The invalid rev tree bodies will eventually be pruned through normal revision tree pruning.
	if len(info.Body) > 0 && info.Body[0] == NonJSONPrefix {
		return nil, false
	}
	return info.Body, true
}

func (tree *RevTree) SetRevisionBody(revid string, body []byte, bodyKey string, hasAttachments bool) {
	if revid == "" {
		// TODO: CBG-1948
		panic("Illegal empty revision ID")
	}
	info := tree.Get(revid)
	if info == nil {
		// TODO: CBG-1948
		panic(fmt.Sprintf("rev id %q not found", revid))
	}

	info.BodyKey = bodyKey
	info.Body = body
	info.HasAttachments = hasAttachments
}

func (tree *RevTree) RemoveRevisionBody(revid string) (deletedBodyKey string) {
	tree.init()
	info := tree.revs[revid]
	if info == nil {
		base.ErrorfCtx(context.Background(), "RemoveRevisionBody called for revid not in tree: %v", revid)
		return ""
	}
	deletedBodyKey = info.BodyKey
	info.BodyKey = ""
	info.Body = nil
	info.HasAttachments = false
	tree.jsonForm = nil
	return deletedBodyKey
}

// Deep-copies a RevTree.
func (tree *RevTree) copy() RevTree {
	result := *tree
	for rev, info := range result.revs {
		// Copy each RevInfo struct in the new tree:
		var copiedInfo RevInfo = *info
		result.revs[rev] = &copiedInfo
	}
	for _, info := range result.revs {
		// Point Parent fields to the copied RevInfos in the new tree:
		if info.Parent != nil {
			info.Parent = result.revs[info.ParentID()]
		}
	}
	return result
}

// Redacts (hashes) channel names in this RevTree.
func (tree *RevTree) redact(salt string) {
	for _, info := range tree.Revs() {
		if info.Channels != nil {
			redactedChannels := base.Set{}
			for existingChanKey := range info.Channels {
				redactedChannels.Add(base.Sha1HashString(existingChanKey, salt))
			}
			info.Channels = redactedChannels
		}
	}
}

// Prune all branches so that they have a maximum depth of maxdepth.
// There is one exception to that, which is tombstoned (deleted) branches that have been deemed "too old"
// to keep around.  The criteria for "too old" is as follows:
//
//   - Find the generation of the shortest non-tombstoned branch (eg, 100)
//   - Calculate the tombstone generation threshold based on this formula:
//     tombstoneGenerationThreshold = genShortestNonTSBranch - maxDepth
//     Ex: if maxDepth is 20, and tombstoneGenerationThreshold is 100, then tombstoneGenerationThreshold will be 80
//   - Check each tombstoned branch, and if the leaf node on that branch has a generation older (less) than
//     tombstoneGenerationThreshold, then remove all nodes on that branch up to the root of the branch.
//
// Returns:
//
//	pruned: number of revisions pruned
//	prunedTombstoneBodyKeys: set of tombstones with external body storage that were pruned, as map[revid]bodyKey
func (tree *RevTree) PruneRevisions(maxDepth uint32, keepRev string) (pruned int, prunedTombstoneBodyKeys map[string]string) {
	if tree.RevCount() <= int(maxDepth) {
		return
	}

	markedRevs := map[string]bool{} // RevIDs marked for removal

	computedMaxDepth, leaves := tree.computeDepthsAndFindLeaves()
	if computedMaxDepth > maxDepth {
		// Delete nodes whose depth is greater than maxDepth:
		for revid, node := range tree.revs {
			if node.depth > maxDepth {
				markedRevs[revid] = true
			}
		}
	}

	// Calculate tombstoneGenerationThreshold
	genShortestNonTSBranch, foundShortestNonTSBranch := tree.FindShortestNonTombstonedBranch()
	tombstoneGenerationThreshold := -1
	if foundShortestNonTSBranch {
		// Only set the tombstoneGenerationThreshold if a genShortestNonTSBranch was found.  (fixes #2695)
		tombstoneGenerationThreshold = genShortestNonTSBranch - int(maxDepth)
	}

	// If we have a valid tombstoneGenerationThreshold, delete any tombstoned branches that are too old
	if tombstoneGenerationThreshold != -1 {
		for _, leafRevId := range leaves {
			leaf := tree.revs[leafRevId]
			if !leaf.Deleted { // Ignore non-tombstoned leaves
				continue
			}
			leafGeneration, _ := ParseRevID(leaf.ID)
			if leafGeneration < tombstoneGenerationThreshold {
				// Delete the branch, down to the root:
				for node := leaf; node != nil; node = node.Parent {
					markedRevs[node.ID] = true
				}
				if leaf.BodyKey != "" {
					if prunedTombstoneBodyKeys == nil {
						prunedTombstoneBodyKeys = make(map[string]string)
					}
					prunedTombstoneBodyKeys[leafRevId] = leaf.BodyKey
				}
			}
		}
	}

	if len(markedRevs) > 0 {
		// Remove the pruned revs:
		for prunedID, _ := range markedRevs {
			delete(tree.revs, prunedID)
		}
		// Snip dangling Parent links:
		for _, node := range tree.revs {
			if markedRevs[node.ParentID()] {
				node.Parent = nil
			}
		}
		tree.jsonForm = nil // since I've been modified
	}

	return len(markedRevs), prunedTombstoneBodyKeys

}

func (tree *RevTree) computeDepthsAndFindLeaves() (maxDepth uint32, leaves []string) {
	// Performance is somewhere between O(n) and O(n^2), depending on the branchiness of the tree.
	tree.init()
	for _, info := range tree.revs {
		info.depth = math.MaxUint32
	}

	// Walk from each leaf to its root, assigning ancestors consecutive depths,
	// but stopping if we'd increase an already-visited ancestor's depth:
	leaves = tree.GetLeaves()
	for _, revid := range leaves {

		var depth uint32 = 1
		for node := tree.revs[revid]; node != nil; node = node.Parent {
			if node.depth <= depth {
				break // This hierarchy already has a shorter path to another leaf
			}
			node.depth = depth
			if depth > maxDepth {
				maxDepth = depth
			}
			depth++
		}
	}
	return maxDepth, leaves
}

// Find the minimum generation that has a non-deleted leaf.  For example in this rev tree:
//
//	http://cbmobile-bucket.s3.amazonaws.com/diagrams/example-sync-gateway-revtrees/three_branches.png
//
// The minimim generation that has a non-deleted leaf is "7-non-winning unresolved"
func (tree *RevTree) FindShortestNonTombstonedBranch() (generation int, found bool) {
	return tree.FindShortestNonTombstonedBranchFromLeaves(tree.GetLeaves())
}

func (tree *RevTree) FindShortestNonTombstonedBranchFromLeaves(leaves []string) (generation int, found bool) {
	tree.init()
	found = false
	genShortestNonTSBranch := math.MaxInt32

	for _, revid := range leaves {
		revInfo := tree.revs[revid]
		if revInfo.Deleted {
			// This is a tombstoned branch, skip it
			continue
		}
		gen := GenOfRevID(revid)
		if gen > 0 && gen < genShortestNonTSBranch {
			genShortestNonTSBranch = gen
			found = true
		}
	}
	return genShortestNonTSBranch, found
}

// Find the generation of the longest deleted branch.  For example in this rev tree:
//
//	http://cbmobile-bucket.s3.amazonaws.com/diagrams/example-sync-gateway-revtrees/four_branches_two_tombstoned.png
//
// The longest deleted branch has a generation of 10
func (tree *RevTree) FindLongestTombstonedBranch() (generation int) {
	return tree.FindLongestTombstonedBranchFromLeaves(tree.GetLeaves())
}

func (tree *RevTree) FindLongestTombstonedBranchFromLeaves(leaves []string) (generation int) {
	tree.init()
	genLongestTSBranch := 0
	for _, revid := range leaves {
		if tree.revs[revid].Deleted {
			gen := GenOfRevID(revid)
			if gen > genLongestTSBranch {
				genLongestTSBranch = gen
			}
		}
	}
	return genLongestTSBranch
}

// Render the RevTree in Graphviz Dot format, which can then be used to generate a PNG diagram
// like http://cbmobile-bucket.s3.amazonaws.com/diagrams/example-sync-gateway-revtrees/three_branches.png
// using the command: dot -Tpng revtree.dot > revtree.png or an online tool such as webgraphviz.com
func (tree *RevTree) RenderGraphvizDot() string {

	resultBuffer := bytes.Buffer{}

	// Helper func to get the graphviz dot representation of a node
	dotRepresentation := func(node *RevInfo) string {
		switch node.Deleted {
		case false:
			return fmt.Sprintf("%q -> %q; ", node.ParentID(), node.ID)
		default:
			multilineResult := bytes.Buffer{}
			multilineResult.WriteString(
				fmt.Sprintf(`%q [fontcolor=red];`, node.ID),
			)
			multilineResult.WriteString(
				fmt.Sprintf(`%q -> %q [label="Tombstone", fontcolor=red];`, node.ParentID(), node.ID),
			)
			return multilineResult.String()
		}
	}

	// Helper func to append node to result: parent -> child;
	dupes := base.Set{}
	appendNodeToResult := func(node *RevInfo) {
		nodeAsDotText := dotRepresentation(node)
		if dupes.Contains(nodeAsDotText) {
			return
		} else {
			dupes[nodeAsDotText] = struct{}{}
		}
		resultBuffer.WriteString(nodeAsDotText)
	}

	// Start graphviz dot file
	resultBuffer.WriteString("digraph graphname{")

	// This function will be called back for every leaf node in tree
	leafProcessor := func(leaf *RevInfo) {

		// Append the leaf to the output
		appendNodeToResult(leaf)

		// Walk up the tree until we find a root, and append each node
		node := leaf
		if node.IsRoot() {
			return
		}

		for {
			node = node.Parent

			// Reached a root, we're done -- there's no need
			// to call appendNodeToResult() on the root, since
			// the child of the root will have already added a node
			// pointing to the root.
			if node.IsRoot() {
				break
			}

			appendNodeToResult(node)
		}
	}

	// Iterate over leaves
	tree.forEachLeafRO(leafProcessor)

	// Finish graphviz dot file
	resultBuffer.WriteString("}")

	return resultBuffer.String()

}

// Returns the history of a revid as an array of revids in reverse chronological order.
// Returns error if detects cycle(s) in rev tree
func (tree *RevTree) GetHistory(revid string) ([]string, error) {
	maxHistory := tree.RevCount()

	history := make([]string, 0, 5)
	for info := tree.revs[revid]; info != nil; info = info.Parent {
		history = append(history, info.ID)
		if len(history) > maxHistory {
			return history, fmt.Errorf("getHistory found cycle in revision tree, history calculated as: %v", history)
		}
	}
	return history, nil
}

//////// FOR TESTS:

// simplified RevInfo whose Parent is a revID not a pointer; used for tests
type RevSpec struct {
	ID       string
	Parent   string
	Body     []byte // Used when revision body stored inline (stores bodies)
	Channels base.Set
	Deleted  bool
}
type RevSpecMap map[string]RevSpec

// only for tests
func MakeRevTree(spec RevSpecMap) RevTree {
	tree := RevTree{revs: RevMap{}}
	for id, item := range spec {
		tree.revs[id] = &RevInfo{ID: id, Body: item.Body, Deleted: item.Deleted, Channels: item.Channels}
	}
	for id, item := range spec {
		if item.Parent != "" {
			tree.revs[id].Parent = tree.revs[item.Parent]
		}
	}
	return tree
}

// only for tests
func (tree *RevTree) insert(item RevSpec) {
	tree.initMutable()
	var parent *RevInfo
	if item.Parent != "" {
		parent = tree.revs[item.Parent]
		if parent == nil {
			panic("RevTree.insert: unknown Parent")
		}
	}
	tree.revs[item.ID] = &RevInfo{ID: item.ID, Parent: parent, Body: item.Body, Deleted: item.Deleted, Channels: item.Channels}
}
