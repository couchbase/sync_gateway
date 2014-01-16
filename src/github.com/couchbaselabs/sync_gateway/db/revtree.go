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
	"math"

	"github.com/couchbaselabs/sync_gateway/base"
)

type RevKey string

// Information about a single revision.
type RevInfo struct {
	ID       string
	Parent   string
	Deleted  bool
	Body     []byte
	Channels base.Set
	depth    uint32
}

//  A revision tree maps each revision ID to its RevInfo.
type RevTree map[string]*RevInfo

// The form in which a RevTree is stored in JSON. For space-efficiency it's stored as an array of
// rev IDs, with a parallel array of parent indexes. Ordering in the arrays doesn't matter.
// So the parent of Revs[i] is Revs[Parents[i]] (unless Parents[i] == -1, which denotes a root.)
type revTreeList struct {
	Revs     []string   `json:"revs"`              // The revision IDs
	Parents  []int      `json:"parents"`           // Index of parent of each revision (-1 if root)
	Deleted  []int      `json:"deleted,omitempty"` // Indexes of revisions that are deletions
	Bodies   []string   `json:"bodies,omitempty"`  // JSON of each revision
	Channels []base.Set `json:"channels"`
}

func (tree RevTree) MarshalJSON() ([]byte, error) {
	n := len(tree)
	rep := revTreeList{
		Revs:     make([]string, n),
		Parents:  make([]int, n),
		Bodies:   make([]string, n),
		Channels: make([]base.Set, n),
	}
	revIndexes := map[string]int{"": -1}

	i := 0
	for _, info := range tree {
		revIndexes[info.ID] = i
		rep.Revs[i] = info.ID
		rep.Bodies[i] = string(info.Body)
		rep.Channels[i] = info.Channels
		if info.Deleted {
			if rep.Deleted == nil {
				rep.Deleted = make([]int, 0, 1)
			}
			rep.Deleted = append(rep.Deleted, i)
		}
		i++
	}

	for i, revid := range rep.Revs {
		rep.Parents[i] = revIndexes[tree[revid].Parent]
	}

	return json.Marshal(rep)
}

func (tree RevTree) UnmarshalJSON(inputjson []byte) (err error) {
	if tree == nil {
		base.Warn("No RevTree for input %q", inputjson)
		return nil
	}
	var rep revTreeList
	err = json.Unmarshal(inputjson, &rep)
	if err != nil {
		return
	}

	for i, revid := range rep.Revs {
		info := RevInfo{ID: revid}
		if rep.Bodies != nil && len(rep.Bodies[i]) > 0 {
			info.Body = []byte(rep.Bodies[i])
		}
		if rep.Channels != nil {
			info.Channels = rep.Channels[i]
		}
		parentIndex := rep.Parents[i]
		if parentIndex >= 0 {
			info.Parent = rep.Revs[parentIndex]
		}
		tree[revid] = &info
	}
	if rep.Deleted != nil {
		for _, i := range rep.Deleted {
			info := tree[rep.Revs[i]]
			info.Deleted = true //because tree[rep.Revs[i]].Deleted=true is a compile error
			tree[rep.Revs[i]] = info
		}
	}
	return
}

// Returns true if the RevTree has an entry for this revid.
func (tree RevTree) contains(revid string) bool {
	_, exists := tree[revid]
	return exists
}

// Returns the RevInfo for a revision ID, or panics if it's not found
func (tree RevTree) getInfo(revid string) *RevInfo {
	info, exists := tree[revid]
	if !exists {
		panic("can't find rev")
	}
	return info
}

// Returns the parent ID of a revid. The parent is "" if the revid is a root.
// Panics if the revid is not in the map at all.
func (tree RevTree) getParent(revid string) string {
	return tree.getInfo(revid).Parent
}

// Returns the history of a revid as an array of revids in reverse chronological order.
func (tree RevTree) getHistory(revid string) []string {
	history := make([]string, 0, 5)
	for revid != "" {
		history = append(history, revid)
		revid = tree.getInfo(revid).Parent
	}
	return history
}

// Returns the leaf revision IDs (those that have no children.)
func (tree RevTree) getLeaves() []string {
	isParent := map[string]bool{}
	for _, info := range tree {
		isParent[info.Parent] = true
	}
	leaves := make([]string, 0, len(tree)-len(isParent)+1)
	for revid, _ := range tree {
		if !isParent[revid] {
			leaves = append(leaves, revid)
		}
	}
	return leaves
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
func (tree RevTree) winningRevision() string {
	winner := ""
	winnerExists := false
	for _, revid := range tree.getLeaves() {
		info := tree[revid]
		exists := !info.Deleted
		if (exists && !winnerExists) ||
			((exists == winnerExists) && compareRevIDs(revid, winner) > 0) {
			winner = revid
			winnerExists = exists
		}
	}
	return winner
}

// Given a revision and a set of possible ancestors, finds the one that is the most recent
// ancestor of the revision; if none are ancestors, returns "".
func (tree RevTree) findAncestorFromSet(revid string, ancestors []string) string {
	//OPT: This is slow...
	for revid != "" {
		for _, a := range ancestors {
			if a == revid {
				return a
			}
		}
		revid = tree.getInfo(revid).Parent
	}
	return ""
}

// Records a revision in a RevTree.
func (tree RevTree) addRevision(info RevInfo) {
	revid := info.ID
	if revid == "" {
		panic("empty revid is illegal")
	}
	if tree.contains(revid) {
		panic(fmt.Sprintf("already contains rev %q", revid))
	}
	parent := info.Parent
	if parent != "" && !tree.contains(parent) {
		panic(fmt.Sprintf("parent id %q is missing", parent))
	}
	tree[revid] = &info
}

func (tree RevTree) getRevisionBody(revid string) ([]byte, bool) {
	if revid == "" {
		panic("Illegal empty revision ID")
	}
	info, found := tree[revid]
	if !found {
		return nil, false
	}
	return info.Body, true
}

func (tree RevTree) setRevisionBody(revid string, body []byte) {
	if revid == "" {
		panic("Illegal empty revision ID")
	}
	info, found := tree[revid]
	if !found {
		panic(fmt.Sprintf("rev id %q not found", revid))
	}
	info.Body = body
}

func (tree RevTree) getParsedRevisionBody(revid string) Body {
	bodyJSON, found := tree.getRevisionBody(revid)
	if !found || len(bodyJSON) == 0 {
		return nil
	}
	var body Body
	if err := json.Unmarshal(bodyJSON, &body); err != nil {
		panic(fmt.Sprintf("Unexpected error parsing body of rev %q", revid))
	}
	return body
}

// Copies a RevTree.
func (tree RevTree) copy() RevTree {
	result := RevTree{}
	for rev, info := range tree {
		result[rev] = info
	}
	return result
}

//////// PRUNING THE TREE (REVS_LIMIT)

// Initializes the depth field of every tree node, computed as the distance to the closest leaf.
func (tree RevTree) computeDepths() (maxDepth uint32) {
	// TODO: Should deleted leaves be penalized since they're not very useful?
	// Performance is somewhere between O(n) and O(n^2), depending on the branchiness of the tree.
	for _, info := range tree {
		info.depth = math.MaxUint32
	}
	// Walk from each leaf to its root, assigning ancestors consecutive depths,
	// but stopping if we'd increase an already-visited ancestor's depth:
	for _, revid := range tree.getLeaves() {
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
	return
}

// Removes older ancestor nodes from the tree so that no node's depth is greater than maxDepth.
// Returns the number of nodes pruned.
func (tree RevTree) pruneRevisions(maxDepth uint32) (pruned int) {
	if len(tree) <= int(maxDepth) || tree.computeDepths() <= maxDepth {
		return
	}

	// Delete nodes whose depth is greater than maxDepth:
	for revid, node := range tree {
		if node.depth > maxDepth {
			delete(tree, revid)
			pruned++
		}
	}

	// Finally, snip dangling Parent links:
	if pruned > 0 {
		for _, node := range tree {
			if node.Parent != "" {
				if _, found := tree[node.Parent]; !found {
					node.Parent = ""
				}
			}
		}
	}
	return
}

//////// HELPERS:

// Parses a CouchDB _revisions property into a list of revision IDs
func ParseRevisions(body Body) []string {
	// http://wiki.apache.org/couchdb/HTTP_Document_API#GET
	revisions, ok := body["_revisions"].(map[string]interface{})
	if !ok {
		return nil
	}
	var start int
	if startf, ok := revisions["start"].(float64); ok {
		start = int(startf)
	}
	ids, ok := revisions["ids"].([]interface{})
	if !ok || len(ids) == 0 || start < len(ids) {
		return nil
	}
	result := make([]string, 0, len(ids))
	for _, id := range ids {
		result = append(result, fmt.Sprintf("%d-%s", start, id))
		start--
	}
	return result
}

func encodeRevisions(revs []string) Body {
	ids := make([]string, len(revs))
	var start int
	for i, revid := range revs {
		gen, id := parseRevID(revid)
		ids[i] = id
		if i == 0 {
			start = gen
		} else if gen != start-i {
			base.Warn("encodeRevisions found weird history %v", revs)
		}
	}
	return Body{"start": start, "ids": ids}
}
