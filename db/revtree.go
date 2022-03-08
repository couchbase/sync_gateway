//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/couchbase/sync_gateway/base"
)

type RevKey string

// Information about a single revision.
type RevInfo struct {
	ID             string
	Parent         string
	BodyKey        string // Used when revision body stored externally (doc key used for external storage)
	Deleted        bool
	depth          uint32
	Body           []byte // Used when revision body stored inline (stores bodies)
	Channels       base.Set
	HasAttachments bool
}

func (rev RevInfo) IsRoot() bool {
	return rev.Parent == ""
}

//  A revision tree maps each revision ID to its RevInfo.
type RevTree map[string]*RevInfo

// The form in which a RevTree is stored in JSON. For space-efficiency it's stored as an array of
// rev IDs, with a parallel array of parent indexes. Ordering in the arrays doesn't matter.
// So the parent of Revs[i] is Revs[Parents[i]] (unless Parents[i] == -1, which denotes a root.)
type revTreeList struct {
	Revs           []string          `json:"revs"`                 // The revision IDs
	Parents        []int             `json:"parents"`              // Index of parent of each revision (-1 if root)
	Deleted        []int             `json:"deleted,omitempty"`    // Indexes of revisions that are deletions
	Bodies_Old     []string          `json:"bodies,omitempty"`     // JSON of each revision (legacy)
	BodyMap        map[string]string `json:"bodymap,omitempty"`    // JSON of each revision
	BodyKeyMap     map[string]string `json:"bodyKeyMap,omitempty"` // Keys of revision bodies stored in external documents
	Channels       []base.Set        `json:"channels"`
	HasAttachments []int             `json:"hasAttachments,omitempty"` // Indexes of revisions that has attachments
}

func (tree RevTree) MarshalJSON() ([]byte, error) {
	n := len(tree)
	rep := revTreeList{
		Revs:     make([]string, n),
		Parents:  make([]int, n),
		Channels: make([]base.Set, n),
	}
	revIndexes := map[string]int{"": -1}

	i := 0
	for _, info := range tree {
		revIndexes[info.ID] = i
		rep.Revs[i] = info.ID
		if info.Body != nil || info.BodyKey != "" {
			// Marshal either the BodyKey or the Body, depending on whether a BodyKey is specified
			if info.BodyKey == "" {
				if rep.BodyMap == nil {
					rep.BodyMap = make(map[string]string, 1)
				}
				rep.BodyMap[strconv.FormatInt(int64(i), 10)] = string(info.Body)
			} else {
				if rep.BodyKeyMap == nil {
					rep.BodyKeyMap = make(map[string]string)
				}
				rep.BodyKeyMap[strconv.FormatInt(int64(i), 10)] = info.BodyKey
			}
		}
		rep.Channels[i] = info.Channels
		if info.Deleted {
			if rep.Deleted == nil {
				rep.Deleted = make([]int, 0, 1)
			}
			rep.Deleted = append(rep.Deleted, i)
		}
		if info.HasAttachments {
			if rep.HasAttachments == nil {
				rep.HasAttachments = make([]int, 0, 1)
			}
			rep.HasAttachments = append(rep.HasAttachments, i)
		}
		i++
	}

	for i, revid := range rep.Revs {
		parentRevId := tree[revid].Parent
		parentRevIndex, ok := revIndexes[parentRevId]
		if ok {
			rep.Parents[i] = parentRevIndex
		} else {
			// If the parent revision does not exist in the revtree due to being a dangling parent, then
			// consider this a root node and set the parent index to -1
			// See SG Issue #2847 for more details.
			rep.Parents[i] = -1
		}

	}

	return base.JSONMarshal(rep)
}

func (tree *RevTree) UnmarshalJSON(inputjson []byte) (err error) {

	if tree == nil {
		// base.Warnf(base.KeyAll, "No RevTree for input %q", inputjson)
		return nil
	}
	var rep revTreeList
	err = base.JSONUnmarshal(inputjson, &rep)
	if err != nil {
		return
	}

	// validate revTreeList revs, parents and channels lists are of equal length
	if !(len(rep.Revs) == len(rep.Parents) && len(rep.Revs) == len(rep.Channels)) {
		return errors.New("revtreelist data is invalid, revs/parents/channels counts are inconsistent")
	}

	*tree = make(RevTree, len(rep.Revs))

	for i, revid := range rep.Revs {
		info := RevInfo{ID: revid}
		stringIndex := strconv.FormatInt(int64(i), 10)
		if rep.BodyMap != nil {
			if body := rep.BodyMap[stringIndex]; body != "" {
				info.Body = []byte(body)
			}
		} else if rep.Bodies_Old != nil && len(rep.Bodies_Old[i]) > 0 {
			info.Body = []byte(rep.Bodies_Old[i])
		}
		if rep.BodyKeyMap != nil {
			bodyKey, ok := rep.BodyKeyMap[stringIndex]
			if ok {
				info.BodyKey = bodyKey
			}
		}
		if rep.Channels != nil {
			info.Channels = rep.Channels[i]
		}
		parentIndex := rep.Parents[i]
		if parentIndex >= 0 {
			info.Parent = rep.Revs[parentIndex]
		}
		(*tree)[revid] = &info
	}
	if rep.Deleted != nil {
		for _, i := range rep.Deleted {
			info := (*tree)[rep.Revs[i]]
			info.Deleted = true // because tree[rep.Revs[i]].Deleted=true is a compile error
			(*tree)[rep.Revs[i]] = info
		}
	}
	if rep.HasAttachments != nil {
		for _, i := range rep.HasAttachments {
			info := (*tree)[rep.Revs[i]]
			info.HasAttachments = true
			(*tree)[rep.Revs[i]] = info
		}
	}
	return
}

func (tree RevTree) ContainsCycles() bool {
	containsCycles := false
	for _, leafRevision := range tree.GetLeaves() {
		_, revHistoryErr := tree.getHistory(leafRevision)
		if revHistoryErr != nil {
			containsCycles = true
		}
	}
	return containsCycles
}

// Repair rev trees that have cycles introduced by SG Issue #2847
func (tree RevTree) RepairCycles() (err error) {

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
				node.Parent = ""
				break
			}

			node = tree[node.Parent]

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
	tree.forEachLeaf(leafProcessor)

	return nil
}

// Detect situations like:
//     node: &{ID:10-684759c169c75629d02b90fe10b56925 Parent:184-a6b3f72a2bc1f988bfb720fec8db3a1d Deleted:fa...
// where the parent generation is *higher* than the node generation, which is never a valid scenario.
// Likewise, detect situations where the parent generation is equal to the node generation, which is also invalid.
func (node RevInfo) ParentGenGTENodeGen() bool {
	return genOfRevID(node.Parent) >= genOfRevID(node.ID)
}

// Returns true if the RevTree has an entry for this revid.
func (tree RevTree) contains(revid string) bool {
	_, exists := tree[revid]
	return exists
}

// Returns the RevInfo for a revision ID, or panics if it's not found
func (tree RevTree) getInfo(revid string) (*RevInfo, error) {
	info, exists := tree[revid]
	if !exists {
		return nil, errors.New("getInfo can't find rev: " + revid)
	}
	return info, nil
}

// Returns the parent ID of a revid. The parent is "" if the revid is a root.
// Panics if the revid is not in the map at all.
func (tree RevTree) getParent(revid string) string {
	info, err := tree.getInfo(revid)
	if err != nil {
		return ""
	}
	return info.Parent
}

// Returns the leaf revision IDs (those that have no children.)
func (tree RevTree) GetLeaves() []string {
	acceptAllLeavesFilter := func(revId string) bool {
		return true
	}
	return tree.GetLeavesFiltered(acceptAllLeavesFilter)
}

func (tree RevTree) GetLeavesFiltered(filter func(revId string) bool) []string {

	isParent := map[string]bool{}
	for _, info := range tree {
		isParent[info.Parent] = true
	}
	leaves := make([]string, 0, len(tree)-len(isParent)+1)
	for revid := range tree {
		if !isParent[revid] {
			if filter(revid) {
				leaves = append(leaves, revid)
			}
		}
	}
	return leaves

}

func (tree RevTree) forEachLeaf(callback func(*RevInfo)) {
	isParent := map[string]bool{}
	for _, info := range tree {
		isParent[info.Parent] = true
	}
	for revid, info := range tree {
		if !isParent[revid] {
			callback(info)

		}
	}
}

func (tree RevTree) isLeaf(revid string) bool {
	if !tree.contains(revid) {
		return false
	}
	for _, info := range tree {
		if info.Parent == revid {
			return false
		}
	}
	return true
}

// Finds the "winning" revision, the one that should be treated as the default.
// This is the leaf revision whose (!deleted, generation, hash) tuple compares the highest.
func (tree RevTree) winningRevision() (winner string, branched bool, inConflict bool) {
	winnerExists := false
	leafCount := 0
	activeLeafCount := 0
	tree.forEachLeaf(func(info *RevInfo) {
		exists := !info.Deleted
		leafCount++
		if exists {
			activeLeafCount++
		}
		if (exists && !winnerExists) ||
			((exists == winnerExists) && compareRevIDs(info.ID, winner) > 0) {
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
func (tree RevTree) findAncestorFromSet(revid string, ancestors []string) string {
	// OPT: This is slow...
	for revid != "" {
		for _, a := range ancestors {
			if a == revid {
				return a
			}
		}
		info, err := tree.getInfo(revid)
		if err != nil {
			break
		}
		revid = info.Parent
	}
	return ""
}

// Records a revision in a RevTree.
func (tree RevTree) addRevision(docid string, info RevInfo) (err error) {
	revid := info.ID
	if revid == "" {
		err = errors.New(fmt.Sprintf("doc: %v, RevTree addRevision, empty revid is illegal", docid))
		return
	}
	if tree.contains(revid) {
		err = errors.New(fmt.Sprintf("doc: %v, RevTree addRevision, already contains rev %q", docid, revid))
		return
	}
	parent := info.Parent
	if parent != "" && !tree.contains(parent) {
		err = errors.New(fmt.Sprintf("doc: %v, RevTree addRevision, parent id %q is missing", docid, parent))
		return
	}
	tree[revid] = &info
	return nil
}

func (tree RevTree) getRevisionBody(revid string, loader RevLoaderFunc) ([]byte, bool) {
	if revid == "" {
		// TODO: CBG-1948
		panic("Illegal empty revision ID")
	}
	info, found := tree[revid]
	if !found {
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
	if len(info.Body) > 0 && info.Body[0] == nonJSONPrefix {
		return nil, false
	}
	return info.Body, true
}

func (tree RevTree) setRevisionBody(revid string, body []byte, bodyKey string, hasAttachments bool) {
	if revid == "" {
		// TODO: CBG-1948
		panic("Illegal empty revision ID")
	}
	info, found := tree[revid]
	if !found {
		// TODO: CBG-1948
		panic(fmt.Sprintf("rev id %q not found", revid))
	}

	info.BodyKey = bodyKey
	info.Body = body
	info.HasAttachments = hasAttachments
}

func (tree RevTree) removeRevisionBody(revid string) (deletedBodyKey string) {
	info, found := tree[revid]
	if !found {
		base.ErrorfCtx(context.Background(), "RemoveRevisionBody called for revid not in tree: %v", revid)
		return ""
	}
	deletedBodyKey = info.BodyKey
	info.BodyKey = ""
	info.Body = nil
	info.HasAttachments = false
	return deletedBodyKey
}

// Deep-copies a RevTree.
func (tree RevTree) copy() RevTree {
	result := RevTree{}
	for rev, info := range tree {
		copiedInfo := *info
		result[rev] = &copiedInfo
	}
	return result
}

// Prune all branches so that they have a maximum depth of maxdepth.
// There is one exception to that, which is tombstoned (deleted) branches that have been deemed "too old"
// to keep around.  The criteria for "too old" is as follows:
//
// - Find the generation of the shortest non-tombstoned branch (eg, 100)
// - Calculate the tombstone generation threshold based on this formula:
//      tombstoneGenerationThreshold = genShortestNonTSBranch - maxDepth
//      Ex: if maxDepth is 20, and tombstoneGenerationThreshold is 100, then tombstoneGenerationThreshold will be 80
// - Check each tombstoned branch, and if the leaf node on that branch has a generation older (less) than
//   tombstoneGenerationThreshold, then remove all nodes on that branch up to the root of the branch.
// Returns:
//  pruned: number of revisions pruned
//  prunedTombstoneBodyKeys: set of tombstones with external body storage that were pruned, as map[revid]bodyKey
func (tree RevTree) pruneRevisions(maxDepth uint32, keepRev string) (pruned int, prunedTombstoneBodyKeys map[string]string) {

	if len(tree) <= int(maxDepth) {
		return
	}

	computedMaxDepth, leaves := tree.computeDepthsAndFindLeaves()
	if computedMaxDepth > maxDepth {
		// Delete nodes whose depth is greater than maxDepth:
		for revid, node := range tree {
			if node.depth > maxDepth {
				delete(tree, revid)
				pruned++
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
			leaf := tree[leafRevId]
			if !leaf.Deleted { // Ignore non-tombstoned leaves
				continue
			}
			leafGeneration, _ := ParseRevID(leaf.ID)
			if leafGeneration < tombstoneGenerationThreshold {
				pruned += tree.DeleteBranch(leaf)
				if leaf.BodyKey != "" {
					if prunedTombstoneBodyKeys == nil {
						prunedTombstoneBodyKeys = make(map[string]string)
					}
					prunedTombstoneBodyKeys[leafRevId] = leaf.BodyKey
				}
			}
		}
	}

	// Snip dangling Parent links:
	if pruned > 0 {
		for _, node := range tree {
			if node.Parent != "" {
				if _, found := tree[node.Parent]; !found {
					node.Parent = ""
				}
			}
		}
	}

	return pruned, prunedTombstoneBodyKeys

}

func (tree RevTree) DeleteBranch(node *RevInfo) (pruned int) {

	revId := node.ID

	for node := tree[revId]; node != nil; node = tree[node.Parent] {
		delete(tree, node.ID)
		pruned++
	}

	return pruned

}

func (tree RevTree) computeDepthsAndFindLeaves() (maxDepth uint32, leaves []string) {

	// Performance is somewhere between O(n) and O(n^2), depending on the branchiness of the tree.
	for _, info := range tree {
		info.depth = math.MaxUint32
	}

	// Walk from each leaf to its root, assigning ancestors consecutive depths,
	// but stopping if we'd increase an already-visited ancestor's depth:
	leaves = tree.GetLeaves()
	for _, revid := range leaves {

		var depth uint32 = 1
		for node := tree[revid]; node != nil; node = tree[node.Parent] {
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
//   http://cbmobile-bucket.s3.amazonaws.com/diagrams/example-sync-gateway-revtrees/three_branches.png
// The minimim generation that has a non-deleted leaf is "7-non-winning unresolved"
func (tree RevTree) FindShortestNonTombstonedBranch() (generation int, found bool) {
	return tree.FindShortestNonTombstonedBranchFromLeaves(tree.GetLeaves())
}

func (tree RevTree) FindShortestNonTombstonedBranchFromLeaves(leaves []string) (generation int, found bool) {

	found = false
	genShortestNonTSBranch := math.MaxInt32

	for _, revid := range leaves {

		revInfo := tree[revid]
		if revInfo.Deleted {
			// This is a tombstoned branch, skip it
			continue
		}
		gen := genOfRevID(revid)
		if gen > 0 && gen < genShortestNonTSBranch {
			genShortestNonTSBranch = gen
			found = true
		}
	}
	return genShortestNonTSBranch, found
}

// Find the generation of the longest deleted branch.  For example in this rev tree:
//   http://cbmobile-bucket.s3.amazonaws.com/diagrams/example-sync-gateway-revtrees/four_branches_two_tombstoned.png
// The longest deleted branch has a generation of 10
func (tree RevTree) FindLongestTombstonedBranch() (generation int) {
	return tree.FindLongestTombstonedBranchFromLeaves(tree.GetLeaves())
}

func (tree RevTree) FindLongestTombstonedBranchFromLeaves(leaves []string) (generation int) {
	genLongestTSBranch := 0
	for _, revid := range leaves {
		gen := genOfRevID(revid)
		if tree[revid].Deleted {
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
func (tree RevTree) RenderGraphvizDot() string {

	resultBuffer := bytes.Buffer{}

	// Helper func to surround graph node w/ double quotes
	surroundWithDoubleQuotes := func(orig string) string {
		return fmt.Sprintf(`"%s"`, orig)
	}

	// Helper func to get the graphviz dot representation of a node
	dotRepresentation := func(node *RevInfo) string {
		switch node.Deleted {
		case false:
			return fmt.Sprintf(
				"%s -> %s; ",
				surroundWithDoubleQuotes(node.Parent),
				surroundWithDoubleQuotes(node.ID),
			)
		default:
			multilineResult := bytes.Buffer{}
			multilineResult.WriteString(
				fmt.Sprintf(
					`%s [fontcolor=red];`,
					surroundWithDoubleQuotes(node.ID),
				),
			)
			multilineResult.WriteString(
				fmt.Sprintf(
					`%s -> %s [label="Tombstone", fontcolor=red];`,
					surroundWithDoubleQuotes(node.Parent),
					surroundWithDoubleQuotes(node.ID),
				),
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
			// Mark nodes with dangling parent references
			if tree[node.Parent] == nil {
				missingParentNode := &RevInfo{
					ID:      node.ID,
					Parent:  fmt.Sprintf("%s - MISSING", node.Parent),
					Deleted: node.Deleted,
				}
				appendNodeToResult(missingParentNode)
				break
			}

			node = tree[node.Parent]

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
	tree.forEachLeaf(leafProcessor)

	// Finish graphviz dot file
	resultBuffer.WriteString("}")

	return resultBuffer.String()

}

// Returns the history of a revid as an array of revids in reverse chronological order.
// Returns error if detects cycle(s) in rev tree
func (tree RevTree) getHistory(revid string) ([]string, error) {
	maxHistory := len(tree)

	history := make([]string, 0, 5)
	for revid != "" {
		info, err := tree.getInfo(revid)
		if err != nil {
			break
		}
		history = append(history, revid)
		if len(history) > maxHistory {
			return history, fmt.Errorf("getHistory found cycle in revision tree, history calculated as: %v", history)
		}
		revid = info.Parent
	}
	return history, nil
}

// ////// ENCODED REVISION LISTS (_revisions):

// Parses a CouchDB _rev or _revisions property into a list of revision IDs
func ParseRevisions(body Body) []string {
	// http://wiki.apache.org/couchdb/HTTP_Document_API#GET

	revisionsProperty, ok := body[BodyRevisions]
	if !ok {
		revid, ok := body[BodyRev].(string)
		if !ok {
			return nil
		}
		if genOfRevID(revid) < 1 {
			return nil
		}
		oneRev := make([]string, 0, 1)
		oneRev = append(oneRev, revid)
		return oneRev
	}

	// Revisions may be stored in a Body as Revisions or map[string]interface{}, depending on the source of the Body
	var revisions Revisions
	switch revs := revisionsProperty.(type) {
	case Revisions:
		revisions = revs
	case map[string]interface{}:
		revisions = Revisions(revs)
	default:
		return nil
	}

	return revisions.ParseRevisions()
}

// Splits out the "start" and "ids" properties from encoded revision list
func splitRevisionList(revisions Revisions) (int, []string) {
	start, ok := base.ToInt64(revisions[RevisionsStart])
	digests, _ := GetStringArrayProperty(revisions, RevisionsIds)
	if ok && len(digests) > 0 && int(start) >= len(digests) {
		return int(start), digests
	} else {
		return 0, nil
	}
}

// Standard CouchDB encoding of a revision list: digests without numeric generation prefixes go in
// the "ids" property, and the first (largest) generation number in the "start" property.
// The docID parameter is informational only - and used when logging edge cases.
func encodeRevisions(docID string, revs []string) Revisions {
	ids := make([]string, len(revs))
	var start int
	for i, revid := range revs {
		gen, id := ParseRevID(revid)
		ids[i] = id
		if i == 0 {
			start = gen
		} else if gen != start-i {
			base.WarnfCtx(context.TODO(), "Found gap in revision list for doc %q. Expecting gen %v but got %v in %v", base.UD(docID), start-i, gen, revs)
		}
	}
	return Revisions{RevisionsStart: start, RevisionsIds: ids}
}

// Given a revision history encoded by encodeRevisions() and a list of possible ancestor revIDs,
// trim the history to stop at the first ancestor revID. If no ancestors are found, trim to
// length maxUnmatchedLen.
// TODO: Document/rename what the boolean result return value represents
func trimEncodedRevisionsToAncestor(revs Revisions, ancestors []string, maxUnmatchedLen int) (result bool, trimmedRevs Revisions) {

	trimmedRevs = revs

	start, digests := splitRevisionList(revs)
	if digests == nil {
		return false, trimmedRevs
	}
	matchIndex := len(digests)
	for _, revID := range ancestors {
		gen, digest := ParseRevID(revID)
		if index := start - gen; index >= 0 && index < matchIndex && digest == digests[index] {
			matchIndex = index
			maxUnmatchedLen = matchIndex + 1
		}
	}
	if maxUnmatchedLen < len(digests) {
		// Make a shallow copy here in order to avoid data races where multiple goroutines are
		// modifying the same underlying map returned from the revision cache.
		// See https://github.com/couchbase/sync_gateway/issues/2427
		trimmedRevs = revs.ShallowCopy()
		trimmedRevs[RevisionsIds] = digests[0:maxUnmatchedLen]
	}
	return true, trimmedRevs

}
